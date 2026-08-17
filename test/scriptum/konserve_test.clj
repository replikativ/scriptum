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
            [scriptum.metadata :as m]
            [scriptum.core :as sc])
  (:import [org.replikativ.scriptum ContentHash]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.document Document TextField StringField Field$Store]
           [org.apache.lucene.index IndexWriter IndexWriterConfig DirectoryReader Term]
           [org.apache.lucene.search IndexSearcher]
           [org.apache.lucene.store AlreadyClosedException IOContext LockObtainFailedException]
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

(defn- bodies-at
  "Every stored :body in the index state at `address`, as a set."
  [s cache address]
  (with-open [d (sk/snapshot-directory s cache address)
              r (DirectoryReader/open d)]
    (let [sf (.storedFields (IndexSearcher. r))]
      (into #{} (map #(.get (.document sf %) "body")) (range (.numDocs r))))))

(defn- latest-segments
  "The newest `segments_N` a manifest names.

  A manifest can name SEVERAL. Lucene deletes a superseded commit point after
  the commit that supersedes it, and that delete rides along with the next flip
  rather than the current one — so the durable snapshot legitimately carries the
  previous generation for a while. Picking `first` off an unordered map made
  this a coin toss.

  Generations are base-36 and unpadded, so longer wins and equal lengths compare
  lexicographically."
  [m]
  (->> (keys m)
       (filter #(clojure.string/starts-with? % "segments_"))
       (sort-by (juxt count identity))
       last))

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
      (with-open [d (sk/konserve-directory s (cache) "feature")]
        ;; Touch every file first: materialization is LAZY, so a branch's view
        ;; is populated on first read rather than at open. The inode sharing is
        ;; a property of how a file arrives in the view, not of when.
        (doseq [^String n (vec (.listAll d))]
          (.close (.openInput d n org.apache.lucene.store.IOContext/DEFAULT)))
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

(deftest deletes-do-not-rewrite-the-segment
  (testing "content addressing rests on Lucene files being WRITE-ONCE, and
            deletes are the case where that could plausibly fail: a live-docs
            bitmap changes as documents are removed.

            It holds, because Lucene puts the generation in the NAME —
            `_0_5.liv`, not `_0.liv` — so a delete publishes a new small file
            instead of rewriting anything. The large `.cfs` keeps its address
            however many deletes land on it, which is what keeps a delete-heavy
            workload from re-uploading segments to a remote store."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (with-open [iw (IndexWriter. d (IndexWriterConfig. (StandardAnalyzer.)))]
          (dotimes [i 200]
            (let [doc (Document.)]
              (.add doc (StringField. "id" (str i) Field$Store/YES))
              (.add doc (TextField. "body" (str "document number " i) Field$Store/YES))
              (.addDocument iw doc)))
          (.commit iw))
        (let [before (sk/read-manifest s "main")
              cfs (first (filter #(clojure.string/ends-with? % ".cfs") (keys before)))]
          (is (some? cfs) "the compound file is what we care about not churning")
          (dotimes [n 5]
            (with-open [iw (IndexWriter. d (IndexWriterConfig. (StandardAnalyzer.)))]
              (.deleteDocuments iw (into-array Term [(Term. "id" (str n))]))
              (.commit iw)))
          (let [after (sk/read-manifest s "main")
                changed (set (for [[f a] after :when (not= a (get before f))] f))]
            (is (= (get before cfs) (get after cfs))
                "the segment itself must keep its address across deletes")
            (is (not (contains? changed cfs)))
            (is (some #(clojure.string/ends-with? % ".liv") (keys after))
                "deletes publish a live-docs file")
            (is (every? #(or (clojure.string/ends-with? % ".liv")
                             (clojure.string/starts-with? % "segments_"))
                        changed)
                "and nothing else may change address")))))))

(deftest a-stale-view-entry-is-repaired-not-served
  (testing "a view entry is a hard link into a content-addressed pool, so a
            name that links to the WRONG blob is detectable by inode alone —
            two stats, no re-hash.

            It arises when the manifest advances without this cache following:
            another process wrote the branch, or a session died. Lucene catches
            some of these itself, because `segments_N` embeds its generation in
            a header suffix, but it reports them as INDEX CORRUPTION — alarming
            and wrong, since the store is intact and only the derived cache is
            stale. The cache is disposable by design, so the fix is to repair
            it rather than fail."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "first"))
      (let [seg1 (latest-segments (sk/read-manifest s "main"))
            ;; keep the OLD blob reachable by a hard link, so copying it later
            ;; cannot write through to the pool
            stash (io/file *root* "stash")]
        (Files/createLink (Paths/get (.getPath stash) (make-array String 0))
                          (Paths/get (str (cache) "/main/" seg1) (make-array String 0)))
        (with-open [d (sk/konserve-directory s (cache) "main")]
          (add-doc! d "second"))
        (let [m (sk/read-manifest s "main")
              seg2 (latest-segments m)
              pooled (str (cache) "/pool/" (get m seg2))]
          ;; the view names the current file but links to the previous blob
          (.delete (io/file (cache) "main" seg2))
          (Files/createLink (Paths/get (str (cache) "/main/" seg2) (make-array String 0))
                            (Paths/get (.getPath stash) (make-array String 0)))
          (is (not= (inode (str (cache) "/main/" seg2)) (inode pooled))
              "precondition: the view entry is the wrong blob")
          (with-open [d (sk/konserve-directory s (cache) "main")]
            (is (= #{"first" "second"} (bodies d))
                "the index must read correctly despite the stale view")
            (is (= (inode (str (cache) "/main/" seg2)) (inode pooled))
                "and the view entry must now be the blob the manifest names")))))))

;; =============================================================================
;; The branch registry
;; =============================================================================

(deftest the-registry-survives-collection
  (testing "REGRESSION, caught on the first run: `sweep!` is allow-list, so a
            key nothing names is deleted — and the registry was not in the
            whitelist. One collection emptied it, after which the mark had no
            branches to walk from and the next would have taken the whole index.

            The registry is a GC root, alongside the manifests it names."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "keep me"))
      (sk/fork! s "main" "other")
      (let-the-millisecond-turn-over!)
      (sk/gc! s sid)
      (is (= #{"main" "other"} (sk/branches s))
          "collection must not sweep the registry")
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (is (= #{"keep me"} (bodies d)))))))

(deftest a-branch-is-registered-before-its-manifest
  (testing "the registry is a GC ROOT, so it must never under-report: a branch
            whose manifest exists but which the registry has forgotten has that
            manifest and every blob it names swept. Registering first means a
            crash leaves a registered branch with no manifest, which is
            harmless — `read-manifest` returns `{}`."
    (let [s (store)]
      (with-open [_ (sk/konserve-directory s (cache) "main")]
        ;; registered at open, before anything could have written a manifest
        (is (contains? (sk/branches s) "main"))
        (is (empty? (sk/read-manifest s "main"))
            "and with no manifest yet, which must be harmless"))
      (is (= #{"main"} (sk/branches s)))
      ;; a fork registers before copying too
      (sk/fork! s "main" "feature")
      (is (= #{"main" "feature"} (sk/branches s)))
      (sk/delete-branch! s "feature")
      (is (= #{"main"} (sk/branches s))))))

(deftest the-registry-can-be-rebuilt
  (testing "nothing consults the keyspace on the read path, so drift cannot
            repair itself. `repair-branches!` is the way back — the expensive
            scan `branches` used to do on every call, now explicit and rare."
    (let [s (store)]
      (with-open [_ (sk/konserve-directory s (cache) "main")]
        (add-doc! (sk/konserve-directory s (cache) "main") "x"))
      (sk/fork! s "main" "lost")
      ;; simulate drift: the registry forgets a branch that still has a manifest
      (k/assoc s sk/branches-key #{"main"} {:sync? true})
      (is (= #{"main"} (sk/branches s)) "precondition: drifted")
      (is (= #{"main" "lost"} (sk/repair-branches! s))
          "the scan finds the manifest the registry forgot")
      (is (= #{"main" "lost"} (sk/branches s))))))

(deftest a-store-records-its-manifest-layout
  (testing "an unversioned store IS version 1, because no other version has ever
            existed — this namespace is unreleased, so stamping on first open is
            the whole migration. It only works while that stays true, which is
            why the stamp goes in now rather than when it is first needed."
    (let [s (store)]
      (is (nil? (k/get s sk/format-key nil {:sync? true})) "precondition: unstamped")
      (with-open [_ (sk/konserve-directory s (cache) "main")]
        (is (= {:version sk/format-version} (k/get s sk/format-key nil {:sync? true}))
            "opening a Directory stamps the store")))))

(deftest a-store-from-a-newer-scriptum-is-refused
  (testing "THE REFUSAL IS THE POINT of versioning at all. Reading a later
            layout as though it were this one fails as corruption somewhere far
            from the cause — which already happened in miniature when the blob
            address function changed from `hasch/uuid` to `ContentHash` and
            nothing could tell the two apart, both being plausible UUIDs."
    (let [s (store)]
      (k/assoc s sk/format-key {:version (inc sk/format-version)} {:sync? true})
      (let [e (try (sk/konserve-directory s (cache) "main") nil
                   (catch clojure.lang.ExceptionInfo e e))]
        (is (some? e) "opening must refuse, not proceed")
        (is (re-find #"reads only" (ex-message e)))
        (is (= {:store-version (inc sk/format-version) :supported sk/format-version}
               (ex-data e))
            "and says which layouts are involved, not merely that it failed"))
      (is (= {:version (inc sk/format-version)} (k/get s sk/format-key nil {:sync? true}))
          "refusing must not overwrite the stamp it refused"))))

(deftest collection-cannot-erase-the-format-stamp
  (testing "`sweep!` is allow-list, so an unnamed key is deleted — the bug that
            already emptied the registry once. The stamp fails more quietly and
            much later: sweep it and the store looks unversioned, gets restamped
            with whatever version is current, and a store written by a later
            scriptum is then read as this one. The collector would reintroduce
            precisely the misreading the stamp exists to prevent."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "keep me"))
      (let-the-millisecond-turn-over!)
      (sk/gc! s sid)
      (is (= {:version sk/format-version} (k/get s sk/format-key nil {:sync? true}))
          "the format stamp is a GC root"))))

(deftest collection-is-refused-on-a-newer-layout
  (testing "`gc!` reaches a store without opening a Directory, so it cannot rely
            on the check there — and it is the one operation where misreading a
            manifest DESTROYS data instead of failing. `reachable-addresses`
            takes `vals` of every manifest and assumes each is a whole-blob
            address; under a layout whose entries are not addresses, every val
            misses every blob key and allow-list `sweep!` takes the whole store."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "keep me"))
      (let [blob-key? (fn [e] (= [:scriptum :blob] (subvec (:key e) 0 2)))
            blobs #(count (filter blob-key? (k/keys s {:sync? true})))
            before (blobs)]
        (is (pos? before) "precondition: there are blobs to lose")
        (k/assoc s sk/format-key {:version (inc sk/format-version)} {:sync? true})
        (let-the-millisecond-turn-over!)
        (is (thrown? clojure.lang.ExceptionInfo (sk/gc! s sid)))
        (is (= before (blobs)) "and refusing must delete nothing")))))

;; =============================================================================
;; The manifest as a value
;; =============================================================================

(deftest a-branch-points-at-an-immutable-snapshot
  (testing "the branch key holds an ADDRESS, not the tree. That is what makes an
            index state something a caller can hold: a snapshot address cannot
            change under them, where a branch name can. It is also why the
            address is `ContentHash/hashMap` — the same hash family as the blob
            addresses it names, so it is a merkle root and not a checksum."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "first"))
      (let [a1 (sk/branch-snapshot s "main")]
        (is (uuid? a1) "the branch cell holds an address")
        (is (= (sk/read-snapshot s a1) (sk/read-manifest s "main")))
        (is (= a1 (sk/snapshot-address (sk/read-snapshot s a1)))
            "and the address is the content of what it names")
        (with-open [d (sk/konserve-directory s (cache) "main")]
          (add-doc! d "second"))
        (let [a2 (sk/branch-snapshot s "main")]
          (is (not= a1 a2) "committing moves the pointer")
          (is (= #{"first"} (bodies-at s (cache) a1))
              "and the OLD snapshot still resolves to the old index state"))))))

(deftest a-commit-that-never-finished-does-not-move-the-branch
  (testing "THE LOAD-BEARING ASSUMPTION of putting the flip in `syncMetaData`.

            Lucene calls it as the commit's durability barrier —
            `SegmentInfos.finishCommit` renames `pending_segments_N`, calls it,
            and deletes the renamed file if it throws. So blobs written by
            `sync` must NOT be reachable until it runs, or a half-finished
            commit becomes visible.

            Dropping the Directory without a clean close is the only way to
            check that; a normal close would be indistinguishable."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "committed"))
      (let [before (sk/branch-snapshot s "main")
            d (sk/konserve-directory s (cache) "main")
            iw (IndexWriter. d (IndexWriterConfig. (StandardAnalyzer.)))]
        (let [doc (Document.)]
          (.add doc (TextField. "body" "never committed" Field$Store/YES))
          (.addDocument iw doc))
        ;; Force the segment out: blobs land in the store, but no commit follows.
        (.flush iw)
        (is (= before (sk/branch-snapshot s "main"))
            "flushing writes blobs; it must not move the branch")
        ;; Abandon both without committing or closing cleanly.
        (.rollback iw)
        (.close d)
        (is (= before (sk/branch-snapshot s "main"))
            "and abandoning the writer must leave the branch where it was")
        (with-open [d2 (sk/konserve-directory s (cache) "main")]
          (is (= #{"committed"} (bodies d2))
              "reopening from the store alone sees only the commit that finished"))))))

(deftest a-fork-copies-a-pointer-not-a-tree
  (testing "both branches name the SAME snapshot until either commits, so a fork
            writes one small value rather than duplicating the file map. The
            shared tree is what makes the two histories structurally shared at
            the manifest level, not only at the blob level."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "shared"))
      (sk/fork! s "main" "feature")
      (is (= (sk/branch-snapshot s "main") (sk/branch-snapshot s "feature"))
          "a fresh fork shares its parent's snapshot outright")
      (with-open [d (sk/konserve-directory s (cache) "feature")]
        (add-doc! d "only-feature"))
      (is (not= (sk/branch-snapshot s "main") (sk/branch-snapshot s "feature"))
          "and diverges on the first commit")
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (is (= #{"shared"} (bodies d)) "without disturbing the parent")))))

(deftest an-externally-held-snapshot-survives-collection
  (testing "a snapshot address is safe to hand out, which is the whole point of
            making it a value — but a holder is invisible to a mark that walks
            only branch pointers. `extra-snapshots` is how they say so, and it
            is what datahike's `mark-from-key-map` needs in order to stop
            returning `#{}` once scriptum's blobs live in datahike's store."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "held"))
      (let [held (sk/branch-snapshot s "main")]
        ;; the branch moves on; nothing points at `held` any more
        (with-open [d (sk/konserve-directory s (cache) "main" sid)]
          (add-doc! d "newer"))
        (let-the-millisecond-turn-over!)
        (sk/gc! s sid (ku/now) #{held})
        (is (seq (sk/read-snapshot s held)) "the held snapshot survives")
        (is (= #{"held"} (bodies-at s (cache) held))
            "and still resolves to the index state it named")))))

(deftest a-superseded-snapshot-is-collected
  (testing "the flip side: a snapshot no branch names and no holder claims is
            garbage, and collecting it is what keeps a long history from
            accumulating one tree per commit."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "first"))
      (let [superseded (sk/branch-snapshot s "main")]
        (with-open [d (sk/konserve-directory s (cache) "main" sid)]
          (add-doc! d "second"))
        (let-the-millisecond-turn-over!)
        (sk/gc! s sid)
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"is missing"
                              (sk/read-snapshot s superseded))
            "the superseded snapshot is gone, and reading it says so rather
             than returning an empty map that looks like an empty branch")
        (with-open [d (sk/konserve-directory s (cache) "main" sid)]
          (is (= #{"first" "second"} (bodies d))
              "and the live branch is untouched"))))))

(deftest an-older-layout-is-refused-not-migrated
  (testing "there is NO migration, on purpose. Layout 1 was never released — no
            released scriptum contains this namespace, and the version stamp
            itself postdates every build — so the only v1 stores are development
            ones, cheaper to discard than to convert.

            The converter that did exist was worse than nothing: it read the
            branch REGISTRY to decide what to convert, and an incomplete
            registry is exactly what this repository's earlier missing-GC-root
            bug produced. Branches it missed kept their v1 maps, the stamp
            recorded the store as converted so it never retried, those branches
            read as empty, and the next `gc!` swept their blobs."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "written-as-v2"))
      (let [files (sk/read-manifest s "main")]
        ;; rewrite the store into the v1 shape
        (k/assoc s (sk/manifest-key "main") files {:sync? true})
        (k/assoc s sk/format-key {:version 1} {:sync? true})
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"never released"
                              (sk/konserve-directory s (cache) "main"))
            "opening refuses rather than guessing")
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"v1 file map"
                              (sk/branch-snapshot s "main"))
            "and a v1 cell read directly says so, rather than being treated as
             an address that resolves to nothing")))))

(deftest a-store-written-before-the-stamp-is-refused
  (testing "an unstamped store is fresh ONLY if nothing has registered a branch
            in it — `register-branch!` runs immediately after `ensure-format!`
            on the first open, so a registry means the store predates this
            layout. Without that check an unstamped v1 store would be silently
            stamped v2 and then read as empty."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "pre-stamp"))
      ;; strip the stamp: the state any store written before today would be in
      (k/dissoc s sk/format-key {:sync? true})
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"predates"
                            (sk/konserve-directory s (cache) "main"))))))

(deftest a-fresh-store-is-stamped-not-refused
  (testing "the other side of that check: an empty store has no registry, so it
            is stamped and opened normally. Getting this wrong would refuse
            every new index."
    (let [s (store)]
      (is (nil? (k/get s sk/format-key nil {:sync? true})) "precondition: unstamped")
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "fresh"))
      (is (= {:version sk/format-version} (k/get s sk/format-key nil {:sync? true})))
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (is (= #{"fresh"} (bodies d)))))))

;; =============================================================================
;; Root sets and cache reclamation
;; =============================================================================

(deftest the-root-set-is-exported-not-inlined
  (testing "an embedder — datahike, via `sec/mark-from-key-map` — builds ONE
            whitelist from every index and sweeps everything else. With the root
            set inline in `gc!` it had to re-derive this by hand, and the two
            roots easy to miss are the two already missed once here: the branch
            registry and the format stamp. A swept registry makes the next mark
            find no branches and take the whole index."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "one"))
      (sk/fork! s "main" "feature")
      (let [m (sk/mark s)
            all (into #{} (map :key) (k/keys s {:sync? true}))]
        (is (contains? m sk/branches-key) "the registry is a root")
        (is (contains? m sk/format-key) "so is the format stamp")
        (is (contains? m (sk/manifest-key "main")))
        (is (contains? m (sk/manifest-key "feature")))
        (is (empty? (set/difference all m))
            "and on a store scriptum owns outright, the mark covers every key —
             so an embedder unioning this cannot lose anything of ours")))))

(deftest cache-collection-reclaims-snapshot-views
  (testing "REGRESSION: snapshot views were exempted from collection wholesale,
            so they grew once per address ever opened — and because their
            entries are hard links into the pool, a pool blob deleted here kept
            its inode alive underneath. `gc-cache!` reported bytes it had not
            freed, which on the 512 MB Lambda /tmp it exists for is the exact
            failure it claims to prevent."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "first"))
      (let [old (sk/branch-snapshot s "main")]
        (is (= #{"first"} (bodies-at s (cache) old)) "materializes a snapshot view")
        (is (.isDirectory (io/file (cache) "snapshots" (str old))))
        (with-open [d (sk/konserve-directory s (cache) "main" sid)]
          (add-doc! d "second"))
        ;; nobody holds `old` any more
        (let [r (sk/gc-cache! s (cache))]
          (is (pos? (:snapshot-views r)) "the superseded view is reclaimed")
          (is (not (.isDirectory (io/file (cache) "snapshots" (str old))))))))))

(deftest cache-collection-keeps-a-held-snapshot-view
  (testing "the other side: a snapshot passed as held must not be thrashed, or
            every call re-downloads its blobs."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "first"))
      (let [held (sk/branch-snapshot s "main")]
        (is (= #{"first"} (bodies-at s (cache) held)))
        (with-open [d (sk/konserve-directory s (cache) "main" sid)]
          (add-doc! d "second"))
        (sk/gc-cache! s (cache) #{held})
        (is (.isDirectory (io/file (cache) "snapshots" (str held)))
            "a held snapshot keeps its view")))))

(deftest cache-collection-spares-a-live-writers-lock
  (testing "REGRESSION: view deletion unlinked the whole directory including
            Lucene's `write.lock`, so a writer whose branch had drifted out of
            the registry failed its next commit with NoSuchFileException on the
            lock itself. The lock is Lucene's, not ours — the open-time reconcile
            already exempts it and this must too."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "one")
        (with-open [iw (IndexWriter. d (IndexWriterConfig. (StandardAnalyzer.)))]
          ;; drift: the registry forgets a branch that is open and being written
          (k/assoc s sk/branches-key #{} {:sync? true})
          (sk/gc-cache! s (cache))
          (is (.exists (io/file (cache) "main" "write.lock"))
              "the lock survives collection")
          (let [doc (Document.)]
            (.add doc (TextField. "body" "two" Field$Store/YES))
            (.addDocument iw doc))
          (.commit iw))))))

(deftest reserved-branch-names-are-refused
  (testing "a branch view is `cache/<branch>`, and `pool`/`snapshots` are
            siblings of it. `pool` is the dangerous one: opening that branch runs
            the reconcile, which deletes everything its manifest does not name —
            the entire content pool, for every branch."
    (let [s (store)]
      (doseq [n sk/reserved-branch-names]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"reserved"
                              (sk/konserve-directory s (cache) n))
            (str n " must be refused at open")))
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "x"))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"reserved"
                            (sk/fork! s "main" "pool"))
          "and forking to one must be refused too"))))

(deftest a-failed-fork-does-not-deregister-a-branch-it-found
  (testing "REGRESSION in a fix: the rollback added to stop `fork!` leaving a
            dangling pointer deregistered `to` unconditionally. A branch can
            already be registered and merely uncommitted — `konserve-directory`
            registers at open — and the `k/exists?` guard cannot see that,
            because such a branch has no manifest key. Dropping it from the
            registry hands the next `gc!` a branch nothing roots, and it sweeps
            the manifest and every blob: the exact failure `register-branch!`
            warns about, caused by a rollback meant to prevent a smaller one."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "src" sid)]
        (add-doc! d "source"))
      ;; `victim` exists in the registry but has not committed yet
      (sk/register-branch! s "victim")
      (is (contains? (sk/branches s) "victim") "precondition: registered")
      ;; force the fork to fail every attempt by removing the source's snapshot
      (let [a (sk/branch-snapshot s "src")]
        (k/dissoc s (sk/snapshot-key a) {:sync? true})
        (is (thrown? clojure.lang.ExceptionInfo (sk/fork! s "src" "victim"))))
      (is (contains? (sk/branches s) "victim")
          "a failed fork must not deregister a branch it did not register"))))

(deftest a-pre-registry-store-is-refused-not-wiped
  (testing "REGRESSION in a fix: freshness was probed via the branch registry,
            but the registry was introduced AFTER the v1 manifest layout. A v1
            store older than the registry has manifests and no registry, so it
            read as fresh, got stamped v2, and the next `gc!` swept it to
            nothing — and self-sealingly, since the stamp makes it undetectable
            afterwards. Freshness means NO MANIFESTS."
    (let [s (store)]
      ;; a v1 store from before the registry existed: a manifest, no registry
      (k/assoc s (sk/manifest-key "main") {"segments_1" (random-uuid)} {:sync? true})
      (is (nil? (k/get s sk/branches-key nil {:sync? true})) "precondition: no registry")
      (is (nil? (k/get s sk/format-key nil {:sync? true})) "precondition: no stamp")
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"predates"
                            (sk/konserve-directory s (cache) "main")))
      (is (nil? (k/get s sk/format-key nil {:sync? true}))
          "and refusing must not stamp it, or the evidence is destroyed"))))

(deftest marking-refuses-an-empty-registry-over-a-live-keyspace
  (testing "`sweep!` is allow-list, so marking from a registry that has lost its
            entries whitelists nothing and deletes every branch — which is how
            the registry became a GC root in the first place. An empty registry
            over a non-empty keyspace is drift, not an empty index."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "precious"))
      (k/assoc s sk/branches-key #{} {:sync? true})
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"registry is empty"
                            (sk/gc! s sid))
          "collection refuses rather than wiping")
      (sk/repair-branches! s)
      (sk/gc! s sid)
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (is (= #{"precious"} (bodies d)) "and repair restores it")))))

(deftest a-stale-external-snapshot-is-ignored-not-fatal
  (testing "`extra-snapshots` are HINTS from a holder that is not scriptum, and
            datahike's key-map is exactly where superseded addresses collect.
            One stale entry must not stop collection for the whole store — that
            would punish the caller `mark` was exported for. A dangling BRANCH
            pointer is the opposite case: scriptum owns it, and it is corruption."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "live"))
      (let [gone (random-uuid)]
        (is (set? (sk/mark s #{gone})) "a vanished hint is skipped")
        (let-the-millisecond-turn-over!)
        (is (sk/gc! s sid (ku/now) #{gone}) "and collection still runs"))
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (is (= #{"live"} (bodies d)))))))

(deftest a-dangling-branch-pointer-names-its-branch
  (testing "the operator has to know WHICH branch to delete. `read-snapshot`
            alone reports only the address, which on a store with many branches
            is not actionable."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "healthy"))
      (sk/fork! s "main" "broken")
      (with-open [d (sk/konserve-directory s (cache) "broken" sid)]
        (add-doc! d "doomed"))
      (k/dissoc s (sk/snapshot-key (sk/branch-snapshot s "broken")) {:sync? true})
      (let [e (try (sk/mark s) nil (catch clojure.lang.ExceptionInfo e e))]
        (is (some? e))
        (is (= "broken" (:branch (ex-data e))) "the failure names the branch")
        (is (re-find #"Delete the branch" (ex-message e)) "and says what to do"))
      ;; and that is the recovery
      (sk/delete-branch! s "broken")
      (is (set? (sk/mark s)) "collection works again once it is gone"))))

(deftest a-failed-open-does-not-corrupt-the-live-writer
  (testing "REGRESSION: the open-time reconcile deletes every cached file the
            manifest does not name — which is precisely a live writer's unsynced
            segments — and it ran before the caller could take Lucene's lock. So
            a second open that went on to FAIL with LockObtainFailedException had
            already destroyed the first writer's work: its next commit threw
            NoSuchFileException and its reader threw CorruptIndexException.

            A failed open must not damage a successful one. Taking the lock
            around the reconcile makes it skip when anything else owns the view."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "committed")
        (with-open [iw (IndexWriter. d (IndexWriterConfig. (StandardAnalyzer.)))]
          (let [doc (Document.)]
            (.add doc (TextField. "body" "in flight" Field$Store/YES))
            (.addDocument iw doc))
          (.flush iw)                       ; blobs exist, manifest does not name them
          ;; a second open on the same branch and cache must fail — and be inert
          (is (thrown? Exception
                       (with-open [_ (sk/konserve-directory s (cache) "main")]
                         (IndexWriter. _ (IndexWriterConfig. (StandardAnalyzer.))))))
          (.commit iw)
          (is (= #{"committed" "in flight"} (bodies d))
              "the first writer must be unharmed"))))))

(deftest concurrent-readers-may-materialize-the-same-file
  (testing "REGRESSION: `link-into-view!` checked `.exists` then linked, so two
            readers materializing one file both saw it absent and both linked —
            and the loser got FileAlreadyExistsException thrown straight out of
            `openInput`. That is the createOutput signal; Directory permits only
            NoSuchFile/FileNotFound or a plain IOException there.

            Measured at 38 failures over 24 threads opening a cold forked
            branch. Losing the race is SUCCESS: the file is there either way.

            The TCK cannot see this — it creates every file through
            `createOutput`, so names live in `session` and never take the
            materialization path at all."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "shared"))
      (sk/fork! s "main" "b")
      (let [errs (atom [])
            cold (str *root* "/cold")]
        (run! deref
              (doall (for [_ (range 16)]
                       (future
                         (try
                           (with-open [d (sk/konserve-directory s cold "b")]
                             (doseq [^String n (vec (.listAll d))]
                               (.close (.openInput d n IOContext/DEFAULT))))
                           (catch Throwable t (swap! errs conj t)))))))
        (is (empty? @errs)
            (str "concurrent materialization must not fail: "
                 (pr-str (map #(.getSimpleName (class %)) @errs))))))))

(deftest a-closed-directory-cannot-rewrite-the-manifest
  (testing "REGRESSION: `listAll`, `deleteFile`, `syncMetaData` and `fileLength`
            all reach the STORE before touching the live directory, so
            FilterDirectory's delegated `ensureOpen` never fired for them. A
            `deleteFile` after `close` therefore wrote a NEW manifest with the
            file's reference removed, leaving `segments_N` naming a blob no
            manifest reaches — the branch unopenable and the blob collectable.

            `testDetectClose` probes only `createOutput`, which does reach the
            live directory, which is why the suite passed."
    (let [s (store)
          d (sk/konserve-directory s (cache) "main")]
      (add-doc! d "content")
      (let [before (sk/read-manifest s "main")
            cfs (first (filter #(clojure.string/ends-with? % ".cfs") (keys before)))]
        (.close d)
        (doseq [[label f] [["listAll" #(.listAll d)]
                           ["deleteFile" #(.deleteFile d cfs)]
                           ["syncMetaData" #(.syncMetaData d)]
                           ["fileLength" #(.fileLength d cfs)]]]
          (is (thrown? AlreadyClosedException (f))
              (str label " must refuse once the directory is closed")))
        (is (= before (sk/read-manifest s "main"))
            "and nothing may have reached the durable manifest")))))

(deftest cache-collection-frees-the-pool
  (testing "the store collector does not touch the cache, and the cache is where
            the bytes sit on a machine. Measured on a merge-heavy workload,
            `gc!` reclaimed 82% of the store and 0% of the pool — 73 blobs
            against 14 live addresses. Unbounded on a long-running container;
            on Lambda's 512 MB /tmp it is a hard failure with a store a fraction
            of the size.

            Safe in a way the store collector is not: the pool is DERIVED, so
            the worst case is a re-download, never a dangling reference."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (dotimes [c 6] (add-doc! d (str "doc " c)))
        (with-open [iw (IndexWriter. d (IndexWriterConfig. (StandardAnalyzer.)))]
          (.forceMerge iw 1)
          (.commit iw)))
      (let-the-millisecond-turn-over!)
      (sk/gc! s sid)
      (let [pool (io/file (cache) "pool")
            before (count (.listFiles pool))
            live (count (sk/reachable-addresses s))
            {:keys [blobs]} (sk/gc-cache! s (cache))]
        (is (pos? blobs) "superseded blobs must be reclaimed")
        (is (= live (count (.listFiles pool)))
            "and exactly the live addresses must remain")
        (is (< (count (.listFiles pool)) before)))
      ;; the index still reads, from the pool that is left
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (is (= 6 (count (bodies d)))
            "collection must not disturb the index it collected around")))))

(deftest cache-collection-drops-views-of-deleted-branches
  (testing "a branch's view directory outlives the branch otherwise"
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "x"))
      (sk/fork! s "main" "doomed")
      (with-open [_ (sk/konserve-directory s (cache) "doomed")] nil)
      (is (.isDirectory (io/file (cache) "doomed")))
      (sk/delete-branch! s "doomed")
      (is (pos? (:views (sk/gc-cache! s (cache)))))
      (is (not (.exists (io/file (cache) "doomed")))))))

(deftest cache-collection-does-not-disturb-an-open-reader
  (testing "unlinking a mapped file is safe on POSIX — the inode outlives the
            directory entry for as long as anything maps it, and a live view
            holds a hard link to the same inode regardless"
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "before")
        (with-open [r (DirectoryReader/open d)]
          ;; collect the whole pool out from under a live reader
          (run! #(.delete ^java.io.File %) (.listFiles (io/file (cache) "pool")))
          (is (= 1 (.numDocs r))
              "a reader with the file mapped must keep working")
          (let [sf (.storedFields (IndexSearcher. r))]
            (is (= "before" (.get (.document sf 0) "body")))))))))

(deftest a-shared-store-needs-both-marks
  (testing "`sweep!` is allow-list, so silence is deletion. The metadata index
            keeps its roots under bare keywords and every PSS node under a raw
            UUID — no `[:scriptum \u2026]` prefix — so `scriptum.konserve/mark`
            cannot infer them. Sweeping from it alone deleted the roots and every
            node, leaving the in-memory atom as the only copy until restart.

            No in-tree caller shares a store today, but `open-store-index` takes
            a `:metadata-index` over any store, so the wiring is one line away."
    (let [mi (m/create-metadata-index *root*)]
      (m/index! mi "main" {"tx" "42"} 1)
      (m/flush-index! mi)
      (let [kv (:kv-store mi)
            marked (m/mark kv)
            all (into #{} (map :key) (k/keys kv {:sync? true}))]
        (is (contains? marked :metadata/roots))
        (is (contains? marked :metadata/freed))
        (is (empty? (set/difference all marked))
            "every key the metadata index needs is named, PSS nodes included")
        (is (every? #(not (and (vector? %) (= :scriptum (first %)))) marked)
            "and none of them carry a prefix scriptum.konserve could match on")))))

(deftest cache-collection-tolerates-a-stale-external-snapshot
  (testing "REGRESSION in a fix: `mark` was relaxed to treat `extra-snapshots`
            as hints, but `gc-cache!` reached the pointers through
            `reachable-addresses` instead, which still resolved a vanished extra
            and threw. The DOCUMENTED usage was the failing one — an embedder
            whose held list lags by one entry got a `gc!` that worked and a
            `gc-cache!` that threw every time, so the local pool was never
            reclaimed. That is the unbounded-cache failure `gc-cache!` exists to
            prevent, reported as a branch-pointer error."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "live"))
      (let [gone (random-uuid)]
        (is (map? (sk/gc-cache! s (cache) #{gone}))
            "a vanished hint must not stop cache collection")
        (is (set? (sk/mark s #{gone})) "as it does not stop marking")
        (is (= (sk/reachable-snapshots s #{gone})
               (sk/reachable-snapshots s))
            "and the two paths agree on what is reachable"))
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (is (= #{"live"} (bodies d)))))))

(deftest the-guard-is-engaged-by-default
  (testing "THE GUARD DEFAULTED TO OFF AND BRICKED BRANCHES ON THE ORDINARY PATH.
            `konserve.protocols/store-id` answers nil for a `connect-fs-store`
            store — which is what callers actually use — and a nil id makes the
            guard a no-op and `gc!` take `ts` instead of `guard/cutoff`. A
            collection landing between the blob writes and the pointer flip then
            swept blobs the branch was about to name; the writer saw no error and
            the branch could not be opened again. Reproduced without
            instrumentation, bricking by commit 2.

            Every writer and collector on the same bytes must pass the SAME id,
            so the fallback is the store's own base path: stable across opens and
            processes, one id per store."
    (let [s (store)]
      (is (nil? ((requiring-resolve 'konserve.protocols/store-id) s))
          "precondition: konserve has no id for this store")
      (is (some? (sk/store-id-for s)) "but one is derivable")
      (is (= (sk/store-id-for s) (sk/store-id-for (store)))
          "and it is the same for a second connection to the same bytes")
      ;; the default Directory must be guarded
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "seeded"))
      (is (seq (sk/branches s))))))

(deftest collecting-without-a-guard-is-refused
  (testing "collecting is the destructive half, so it is the half that must not
            be opt-out. Unguarded, a commit in flight loses its blobs and the
            branch head names something that is not there."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "precious"))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"without a store id"
                            (sk/gc! s nil)))
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (is (= #{"precious"} (bodies d)) "and nothing was collected")))))
