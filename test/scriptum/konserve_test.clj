(ns scriptum.konserve-test
  "Pins the konserve-backed storage model.

  Each test names a property the manifest design claims, and several of them
  are regressions for mistakes made while arriving at it — a flat cache that let
  one branch continue another's index, and a cached manifest that left a
  long-lived reader blind to new commits.

  The Lucene `Directory` CONTRACT is not tested here. Lucene ships its own
  conformance suite for that and scriptum runs it — see `scriptum.tck-runner`,
  which is where exception types, concurrent `listAll`, slices and clones, and
  use-after-close are covered far better than by hand. What stays here is what
  that suite cannot know about: the storage model, and the cost of using it."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [konserve.filestore :refer [connect-fs-store]]
            [clojure.set :as set]
            [konserve.core :as k]
            [konserve.gc-guard :as guard]
            [konserve.utils :as ku]
            [scriptum.konserve :as sk]
            [scriptum.core :as sc])
  (:import [org.replikativ.scriptum ContentHash]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.document Document TextField Field$Store]
           [org.apache.lucene.index IndexWriter IndexWriterConfig DirectoryReader]
           [org.apache.lucene.search IndexSearcher]
           [org.apache.lucene.store IOContext LockObtainFailedException]
           [java.nio.file Files LinkOption Paths FileAlreadyExistsException]))

(def ^:dynamic *root* nil)

(defn- rm-rf [^java.io.File f]
  (when (.isDirectory f) (run! rm-rf (.listFiles f)))
  (.delete f))

(use-fixtures :each
  (fn [t]
    (let [root (str "/tmp/scriptum-konserve-test/" (random-uuid))]
      (try (binding [*root* root] (t))
           (finally (rm-rf (io/file root)))))))

(defn- store [] (connect-fs-store (str *root* "/store") :opts {:sync? true}))
(defn- cache [] (str *root* "/cache"))

(defn- add-doc! [dir text]
  (with-open [iw (IndexWriter. dir (IndexWriterConfig. (StandardAnalyzer.)))]
    (let [d (Document.)]
      (.add d (TextField. "body" text Field$Store/YES))
      (.addDocument iw d)
      (.commit iw))))

(defn- bodies
  "Every stored :body in the branch, as a set."
  [dir]
  (with-open [r (DirectoryReader/open dir)]
    (let [sf (.storedFields (IndexSearcher. r))]
      (into #{} (map #(.get (.document sf %) "body")) (range (.numDocs r))))))

(defn- let-the-millisecond-turn-over!
  "Let real time pass the stamps of the writes just made.

  Collection is EVENTUAL by design: konserve stamps writes from a clock pinned
  to wall time at millisecond granularity and the sweep spares ties, so a blob
  written in the same millisecond as the cutoff survives to the next cycle.
  Passing a later cutoff cannot substitute — `sweep!` clamps to
  `min(cutoff, safe-point)` and the safe point never runs ahead of now, which
  is exactly the property that stops a collection reaching into the future.

  So this waits on elapsed time that the semantics under test depend on; it is
  not synchronising concurrent work. konserve's own gc test does the same for
  the same reason."
  []
  (Thread/sleep 5))

(defn- inode [path]
  (Files/getAttribute (Paths/get ^String path (make-array String 0))
                      "unix:ino" (make-array LinkOption 0)))

;; =============================================================================

(deftest index-survives-losing-the-entire-local-cache
  (testing "konserve is the source of truth: wipe the cache and the index is
            still fully readable, rematerialized from blobs alone"
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "durable in konserve"))
      (rm-rf (io/file (cache)))
      (is (not (.exists (io/file (cache)))))
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (is (= #{"durable in konserve"} (bodies d)))))))

(deftest fork-shares-blobs-and-then-diverges
  (testing "forking copies a manifest; the two branches keep the shared document
            and each sees only its own additions"
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "shared base"))
      (sk/fork! s "main" "feature")
      (with-open [m (sk/konserve-directory s (cache) "main")
                  f (sk/konserve-directory s (cache) "feature")]
        (add-doc! m "only main")
        (add-doc! f "only feature")
        (is (= #{"shared base" "only main"} (bodies m)))
        (is (= #{"shared base" "only feature"} (bodies f))))
      (is (= #{"main" "feature"} (sk/branches s))))))

(deftest shared-segments-are-one-blob-and-one-inode
  (testing "content addressing makes a shared segment a single blob, and the
            per-branch views hard-link to it — so it costs disk and page cache
            once, not once per branch"
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "shared base"))
      (sk/fork! s "main" "feature")
      (with-open [_ (sk/konserve-directory s (cache) "feature")]
        (let [mm (sk/read-manifest s "main")
              fm (sk/read-manifest s "feature")
              shared (filter (fn [[n a]] (= a (get fm n))) mm)]
          (is (seq shared) "a fork must share its parent's segments")
          (doseq [[n _] shared]
            (is (= (inode (str (cache) "/main/" n))
                   (inode (str (cache) "/feature/" n)))
                (str n " must be one inode across branches"))))))))

(deftest merging-one-branch-does-not-disturb-another
  (testing "why no BranchAwareMergePolicy is needed: a merge writes NEW
            content-addressed blobs and leaves the old ones for whoever still
            references them"
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "shared base"))
      (sk/fork! s "main" "feature")
      (with-open [m (sk/konserve-directory s (cache) "main")]
        (add-doc! m "only main")
        (with-open [iw (IndexWriter. m (IndexWriterConfig. (StandardAnalyzer.)))]
          (.forceMerge iw 1)
          (.commit iw)))
      (with-open [f (sk/konserve-directory s (cache) "feature")]
        (is (= #{"shared base"} (bodies f))
            "feature must still read the pre-merge segments")))))

(deftest a-branch-never-continues-another-branchs-index
  (testing "REGRESSION: with a flat cache keyed by filename, Lucene saw the
            other branch's files and continued ITS index — the second branch's
            durable manifest ended up containing the first branch's segments"
    (let [s (store)]
      ;; two unrelated branches, never forked from one another
      (with-open [a (sk/konserve-directory s (cache) "alpha")]
        (add-doc! a "alpha only"))
      (with-open [b (sk/konserve-directory s (cache) "beta")]
        (add-doc! b "beta only")
        (is (= #{"beta only"} (bodies b))
            "beta must not inherit alpha's documents"))
      (let [am (sk/read-manifest s "alpha")
            bm (sk/read-manifest s "beta")]
        (is (empty? (clojure.set/intersection (set (vals am)) (set (vals bm))))
            "unrelated branches must not share segment addresses")))))

(deftest a-live-reader-sees-later-commits
  (testing "REGRESSION: caching the manifest at construction made
            openIfChanged blind forever, which is exactly what a remote reader
            polling a shared store depends on"
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "first"))
      (with-open [rd (sk/konserve-directory s (cache) "main")]
        (let [r0 (DirectoryReader/open rd)]
          (try
            (with-open [wd (sk/konserve-directory s (cache) "main")]
              (add-doc! wd "second"))
            (let [r1 (DirectoryReader/openIfChanged r0)]
              (try
                (is (some? r1) "the reader must observe the new commit")
                (is (= 2 (.numDocs r1)))
                (finally (some-> r1 .close))))
            (finally (.close r0))))))))

(deftest one-writer-per-branch-many-branches-in-parallel
  (testing "scriptum's concurrency contract, from the per-branch view directory:
            a second writer on a branch fails LOUDLY, while another branch's
            writer proceeds"
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "seed"))
      (sk/fork! s "main" "feature")
      (with-open [m1 (sk/konserve-directory s (cache) "main")
                  m2 (sk/konserve-directory s (cache) "main")
                  f (sk/konserve-directory s (cache) "feature")]
        (with-open [_w1 (IndexWriter. m1 (IndexWriterConfig. (StandardAnalyzer.)))]
          (is (thrown? LockObtainFailedException
                       (IndexWriter. m2 (IndexWriterConfig. (StandardAnalyzer.))))
              "a second writer on the SAME branch must fail loudly")
          (with-open [_wf (IndexWriter. f (IndexWriterConfig. (StandardAnalyzer.)))]
            (is true "a writer on a DIFFERENT branch proceeds in parallel")))))))

(deftest gc-collects-only-what-no-branch-references
  (testing "reachability from the live manifests is the root set — which is why
            no ref-counting deletion policy is needed"
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "keep me"))
      (sk/fork! s "main" "doomed")
      (with-open [d (sk/konserve-directory s (cache) "doomed" sid)]
        (add-doc! d "only in doomed"))
      (let [doomed-only (remove (set (vals (sk/read-manifest s "main")))
                                (vals (sk/read-manifest s "doomed")))]
        (is (seq doomed-only))
        (sk/delete-branch! s "doomed")
        (let-the-millisecond-turn-over!)
        (sk/gc! s sid)
        (is (= #{"main"} (sk/branches s)))
        (doseq [a doomed-only]
          ;; k/exists?, not k/get: reading a binary value as EDN yields a
          ;; misread byte rather than nil, so k/get cannot answer presence.
          (is (not (k/exists? s (sk/blob-key a) {:sync? true}))
              "a blob no branch references must be collected"))
        (with-open [d (sk/konserve-directory s (cache) "main")]
          (is (= #{"keep me"} (bodies d))
              "and main must be entirely intact"))))))

(deftest a-segment-costs-a-buffer-not-its-own-size
  (testing "REGRESSION: sync hashed and stored a segment by slurping it into a
            byte-array, so a commit cost the size of the segment in heap — and
            a merged segment above Integer/MAX_VALUE could not be allocated at
            all. Both directions stream now.

            The property under test is the DIGEST agreeing across chunk
            boundaries, which is what a streaming hash can plausibly get wrong;
            the memory bound itself is structural. The buffer is 64 KiB and the
            chunks here are deliberately co-prime with it."
    (let [f (io/file *root* "chunky.bin")]
      (io/make-parents f)
      (with-open [o (java.io.FileOutputStream. f)]
        (let [r (java.util.Random. 42)
              chunk (byte-array 65537)]
          (dotimes [_ 24] (.nextBytes r chunk) (.write o chunk))))
      (is (= (ContentHash/hashBytes (Files/readAllBytes (.toPath f)))
             (ContentHash/hashFile (.toPath f)))
          "the streamed digest must equal the whole-file digest"))))

(deftest a-multi-segment-index-round-trips-through-the-store
  (testing "end-to-end over the streaming paths: enough documents to make
            several segments, then a cache wipe forcing every one of them back
            out of konserve and into a fresh view"
    (let [s (store)
          texts (into #{} (map #(str "document number " %)) (range 500))]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (doseq [batch (partition-all 100 texts)]
          (with-open [iw (IndexWriter. d (IndexWriterConfig. (StandardAnalyzer.)))]
            (doseq [t batch]
              (let [doc (Document.)]
                (.add doc (TextField. "body" ^String t Field$Store/YES))
                (.addDocument iw doc)))
            (.commit iw))))
      (rm-rf (io/file (cache)))
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (is (= texts (bodies d))
            "every document must survive the round trip through konserve")))))

(deftest rename-keeps-listAll-truthful
  (testing "REGRESSION: `rename` updated the manifest but never the session, so
            an unsynced file renamed before its first sync left `listAll` naming
            the OLD name — which no longer existed — and hiding the NEW one,
            which did. `deleteFile` then threw NoSuchFile for a file that was
            right there, and `createOutput` on the new name saw no conflict and
            deleted the renamed content.

            Invisible until now because Lucene's commit path only ever renames a
            file already in the manifest, and neither this suite nor Lucene's own
            `testRename` asserts `listAll` afterwards."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (with-open [o (.createOutput d "before.tmp" IOContext/DEFAULT)]
          (.writeInt o 42))
        (.rename d "before.tmp" "after.tmp")
        (is (= ["after.tmp"] (vec (.listAll d)))
            "listAll must name the file that exists, not the one that doesn't")
        (is (= 4 (.fileLength d "after.tmp"))
            "and the renamed file must be readable under its new name")
        (is (thrown? FileAlreadyExistsException
                     (.createOutput d "after.tmp" IOContext/DEFAULT))
            "creating over the renamed file must be refused, not silently obeyed")
        (.deleteFile d "after.tmp")
        (is (= [] (vec (.listAll d))))))))

(deftest debris-from-a-dead-session-is-reclaimed
  (testing "REGRESSION: once `deleteFile` correctly refused files the manifest
            does not name, nothing reclaimed a segment left by a session that
            died mid-write — `listAll` never names it, so Lucene never asks for
            it. It pinned an inode forever. Reconciling the view against the
            manifest on open is what collects it."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "real content"))
      (let [debris (io/file (cache) "main" "_9999.cfs")]
        (spit debris "half-written garbage")
        (is (.exists debris))
        (with-open [d (sk/konserve-directory s (cache) "main")]
          (is (not (.exists debris))
              "a cached file no manifest names must be reclaimed on open")
          (is (= #{"real content"} (bodies d))
              "and the real index must be untouched"))))))

(deftest gc-spares-blobs-whose-manifest-has-not-landed
  (testing "the values-then-pointer race: a sweep running while a sync is in
            flight must not collect the blobs that sync is about to reference"
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "seed"))
      ;; Stand in for a sync in progress: blobs written, manifest not yet updated.
      (let [orphan (random-uuid)]
        (guard/with-unreferenced-writes sid
          (k/bassoc s (sk/blob-key orphan) (byte-array [1 2 3]) {:sync? true})
          (let-the-millisecond-turn-over!)
          (sk/gc! s sid)
          (is (k/exists? s (sk/blob-key orphan) {:sync? true})
              "an unreferenced-but-in-flight blob must survive the sweep"))
        ;; Sequence closed and it never became reachable, so now it is collectable.
        (let-the-millisecond-turn-over!)
        (sk/gc! s sid)
        (is (not (k/exists? s (sk/blob-key orphan) {:sync? true}))
            "once the sequence closes, a genuinely unreachable blob is collected")))))

;; =============================================================================
;; The store-backed writer, through scriptum.core's ordinary API
;; =============================================================================

(deftest a-store-backed-index-behaves-like-a-directory-backed-one
  (testing "everything a writer does to DOCUMENTS is pure Lucene, so the core
            API works unchanged over a konserve store — only branch topology
            differs, because a branch is a manifest rather than a directory"
    (let [s (store)
          w (sc/open-store-index s (cache) "main" {:store-id (random-uuid)})]
      (try
        (is (sc/store-backed? w))
        (sc/add-doc w {:title {:type :text :value "the quick brown fox"}
                       :id {:type :string :value "a"}})
        (sc/add-doc w {:title {:type :text :value "a slow green turtle"}
                       :id {:type :string :value "b"}})
        (sc/commit! w "two documents")
        (is (= 2 (sc/num-docs w)))
        (is (= 1 (count (sc/search w {:term [:id "a"]}))))
        (is (= 1 (count (sc/search w (sc/text-query :title "quick")))))
        (finally (sc/close! w)))
      ;; durable in the store, not in the cache: wipe the cache and reopen
      (rm-rf (io/file (cache)))
      (let [w2 (sc/open-store-index s (cache) "main")]
        (try
          (is (= 2 (sc/num-docs w2))
              "the store is the source of truth; the cache is derived")
          (finally (sc/close! w2)))))))

(deftest forking-a-store-backed-index-copies-a-manifest
  (testing "fork goes through the manifests rather than the filesystem, and the
            branches then diverge independently"
    (let [s (store)
          main (sc/open-store-index s (cache) "main")]
      (try
        (sc/add-doc main {:title {:type :text :value "shared base"}})
        (sc/commit! main "seed")
        (let [feature (sc/fork main "feature")]
          (try
            (is (= #{"main" "feature"} (sc/branches main)))
            (sc/add-doc feature {:title {:type :text :value "only feature"}})
            (sc/commit! feature "feature work")
            (sc/add-doc main {:title {:type :text :value "only main"}})
            (sc/commit! main "main work")
            (is (= 2 (sc/num-docs feature)))
            (is (= 2 (sc/num-docs main)))
            (is (= 1 (count (sc/search feature (sc/text-query :title "feature")))))
            (is (zero? (count (sc/search main (sc/text-query :title "feature"))))
                "the branches must not see each other's work")
            (finally (sc/close! feature))))
        (finally (sc/close! main))))))

(deftest gc-is-refused-on-a-store-backed-index
  (testing "collection for a store-backed index is reachability from the live
            manifests with a guard-derived cutoff, which core's path-based gc!
            cannot express — it says so rather than dereferencing a null path"
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (try
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"scriptum.konserve/gc!"
                              (sc/gc! w (java.time.Instant/now))))
        (finally (sc/close! w))))))
