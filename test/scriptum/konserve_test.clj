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
            [konserve.store :as kstore]
            [clojure.set :as set]
            [konserve.core :as k]
            [konserve.gc-guard :as guard]
            [konserve.utils :as ku]
            [scriptum.konserve :as sk]
            [scriptum.metadata :as m]
            [scriptum.core :as sc]
            [scriptum.yggdrasil :as y]
            [yggdrasil.protocols :as p])
  (:import [org.replikativ.scriptum ContentHash]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.document Document TextField StringField Field$Store]
           [org.apache.lucene.index IndexWriter IndexWriterConfig DirectoryReader Term]
           [org.apache.lucene.search IndexSearcher]
           [org.apache.lucene.store AlreadyClosedException IOContext LockObtainFailedException]
           [java.nio.file Files LinkOption Paths FileAlreadyExistsException]
           [java.time Instant]))

(def ^:dynamic *root* nil)

(defn- rm-rf [^java.io.File f]
  (when (.isDirectory f) (run! rm-rf (.listFiles f)))
  (.delete f))

(use-fixtures :each
  (fn [t]
    (let [root (str "/tmp/scriptum-konserve-test/" (random-uuid))]
      (try (binding [*root* root] (t))
           (finally (rm-rf (io/file root)))))))

(def ^:private store-ids
  "One CONSTANT RANDOM UUID per store path, minted on first use.

  Modelling konserve's contract rather than working around it: `:id` is a global
  address, so it is chosen once at random and reused, never derived from the
  location. Deriving it from the path gets both directions wrong — move the
  store and its identity changes, while two unrelated stores under the same
  mount path collide.

  Held in an atom rather than regenerated per call because the guard's whole
  requirement is that every connection to one store agrees. An earlier version
  of this suite minted `(random-uuid)` per OPEN, which is the two-ids-on-one-store
  case `konserve.gc-guard` calls out as deleting live data — the tests were
  modelling the bug."
  (atom {}))

(defn- store-at
  "A konserve store with an identity, via `connect-store`.

  Never `connect-fs-store`: only `connect-store` requires a UUID `:id` and
  attaches it, and a store answering nil to `konserve.protocols/store-id` is
  what silently disabled scriptum's GC guard."
  [path]
  (let [canonical (.getCanonicalPath (io/file path))
        id (get (swap! store-ids update canonical #(or % (random-uuid))) canonical)
        cfg {:backend :file :path path :id id}]
    (io/make-parents (io/file path "x"))
    (if (konserve.filestore/store-exists? nil path)
      (kstore/connect-store cfg {:sync? true})
      (kstore/create-store cfg {:sync? true}))))

(defn- store [] (store-at (str *root* "/store")))
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

(deftest abort-discards-buffered-store-backed-changes
  (testing "abort is a real Lucene rollback, unlike close's deliberate
            commit-on-close, and therefore never advances the branch manifest"
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (sc/add-doc w {:id {:type :string :value "committed"}})
      (sc/commit! w "durable base")
      (let [before (sc/snapshot-address w)]
        (sc/add-doc w {:id {:type :string :value "buffered"}})
        (sc/abort! w)
        (is (= before (sk/branch-snapshot s "main"))
            "rollback must not publish a new snapshot")
        (let [reopened (sc/open-store-index s (cache) "main")]
          (try
            (is (= ["committed"]
                   (mapv #(get % "id") (sc/search reopened :all {:limit 10}))))
            (finally (sc/close! reopened))))))))

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
        (is (= (sk/snapshot-files s a1) (sk/read-manifest s "main")))
        (is (empty? (sk/snapshot-parents s a1)) "the first commit descends from nothing")
        (is (= a1 (sk/snapshot-address (sk/snapshot-files s a1)
                                       (sk/snapshot-parents s a1)))
            "and the address is the content of what it names, parent included")
        (with-open [d (sk/konserve-directory s (cache) "main")]
          (add-doc! d "second"))
        (let [a2 (sk/branch-snapshot s "main")]
          (is (not= a1 a2) "committing moves the pointer")
          (is (= [a1] (sk/snapshot-parents s a2))
              "and the new commit records the one it descends from")
          (is (= #{"first"} (bodies-at s (cache) a1))
              "and the OLD snapshot still resolves to the old index state"))))))

(deftest a-store-snapshot-search-needs-no-live-branch-writer
  (testing "an address-pinned search handle remains on its generation while the
            branch advances, and owns only read-only resources"
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (try
        (doseq [i (range 5)]
          (sc/add-doc w {:id {:type :string :value (str i)}
                         :body {:type :text :value "common text"}}))
        (sc/commit! w "first generation")
        (let [address (sc/snapshot-address w)]
          (with-open [snapshot (sc/open-store-snapshot s (cache) address)]
            (is (= address (:snapshot-address snapshot)))
            (is (= 5 (count (sc/search-store-snapshot snapshot
                                                      (sc/text-query :body "common")
                                                      {:limit 20
                                                       :fields [:id]}))))
            (is (= 5 (sc/count-store-snapshot
                      snapshot (sc/text-query :body "common"))))
            (is (= 0 (sc/count-store-snapshot
                      snapshot (sc/text-query :body "absent"))))
            (sc/add-doc w {:id {:type :string :value "later"}
                           :body {:type :text :value "common text"}})
            (sc/commit! w "branch advances")
            (is (= 5 (count (sc/search-store-snapshot snapshot
                                                      (sc/text-query :body "common")
                                                      {:limit 20})))
                "the held snapshot cannot move with the branch")
            (is (= 5 (sc/count-store-snapshot
                      snapshot (sc/text-query :body "common")))
                "the count is pinned to the same immutable generation")))
        (finally (sc/close! w))))))

(deftest immutable-snapshot-leases-have-independent-lifetimes
  (testing "overlapping immutable DB values share the read-only Lucene resource,
            but closing either logical lease cannot invalidate the other"
    (let [s (store)
          c (cache)
          w (sc/open-store-index s c "main")]
      (try
        (sc/add-doc w {:body {:type :text :value "shared snapshot"}})
        (sc/commit! w)
        (let [address (sc/snapshot-address w)
              first (sc/open-store-snapshot s c address)
              second (sc/open-store-snapshot s c address)
              retained (sc/retain-store-snapshot first)]
          (try
            (is (identical? (:reader first) (:reader second)))
            (is (identical? (:directory first) (:directory retained)))
            (.close ^java.io.Closeable first)
            (.close ^java.io.Closeable first)
            (is (= 1 (count (sc/search-store-snapshot
                             second (sc/text-query :body "shared") {}))))
            (.close ^java.io.Closeable second)
            (is (= 1 (count (sc/search-store-snapshot
                             retained (sc/text-query :body "snapshot") {})))
                "the last retained lease still owns the physical reader")
            (finally
              (.close ^java.io.Closeable first)
              (.close ^java.io.Closeable second)
              (.close ^java.io.Closeable retained)))
          (with-open [reopened (sc/open-store-snapshot s c address)]
            (is (= 1 (count (sc/search-store-snapshot reopened :all {})))
                "the final close released enough state for a clean reopen")))
        (finally (sc/close! w))))))

(deftest candidates-page-through-an-entire-pinned-snapshot
  (testing "searchAfter exposes every candidate without a fixed top-N and the
            continuation cannot be reused on a different snapshot"
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (try
        (doseq [i (range 23)]
          (sc/add-doc w {:id {:type :string :value (format "%02d" i)}
                         :body {:type :text :value "same score"}}))
        (sc/commit! w "candidate generation")
        (let [address (sc/snapshot-address w)]
          (with-open [snapshot (sc/open-store-snapshot s (cache) address)]
            (loop [after nil
                   ids []
                   page-count 0]
              (let [{:keys [candidates continuation exhausted? ordering]
                     :as page}
                    (sc/candidate-page snapshot (sc/text-query :body "same")
                                       {:page-size 7
                                        :after after
                                        :fields [:id]
                                        :query-id :same-query})
                    ids' (into ids (map #(get % "id")) candidates)]
                (is (= address (:snapshot-address page)))
                (is (= :score-desc-doc-id-asc ordering))
                (is (every? #(and (contains? % :doc-id)
                                  (contains? % :score)
                                  (= #{"id" :doc-id :score} (set (keys %))))
                            candidates))
                (is (apply <= (map :doc-id candidates))
                    "equal-score hits use Lucene's stable doc-id tie break")
                (when (and after (seq candidates))
                  (is (< (:doc-id after) (:doc-id (first candidates)))
                      "the tie break remains monotonic across page boundaries"))
                (if exhausted?
                  (do
                    (is (nil? continuation))
                    (is (= 4 (inc page-count)))
                    (is (= (set (map #(format "%02d" %) (range 23)))
                           (set ids')))
                    (is (= 23 (count ids'))))
                  (do
                    (is (= address (:snapshot-address continuation)))
                    (recur continuation ids' (inc page-count))))))

            (let [first-page (sc/candidate-page snapshot :all
                                                {:page-size 3 :query-id :match-all})
                  old-cursor (:continuation first-page)]
              (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                    #"continuation does not belong"
                                    (sc/candidate-page snapshot :all
                                                       {:after old-cursor
                                                        :query-id :different-query})))
              (sc/add-doc w {:id {:type :string :value "new"}
                             :body {:type :text :value "same score"}})
              (sc/commit! w "new snapshot")
              (with-open [new-snapshot (sc/open-store-snapshot
                                        s (cache) (sc/snapshot-address w))]
                (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                      #"continuation does not belong"
                                      (sc/candidate-page new-snapshot :all
                                                         {:after old-cursor})))))))
        (finally (sc/close! w))))))

(deftest analyzer-free-term-builders-query-multi-valued-string-fields
  (testing "exact and prefix builders operate on complete StringField values,
            not analyzer output, and a multi-valued document remains one hit"
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (try
        (doseq [[id lexemes]
                [["one" ["cat" "cater" "running"]]
                 ["two" ["catalog" "dog"]]
                 ["three" ["Cat" "run"]]]]
          (sc/add-doc w {:id {:type :string :value id}
                         :lexeme {:type :string :value lexemes :store? false}}))
        (sc/commit! w "exact lexeme generation")
        (with-open [snapshot (sc/open-store-snapshot
                              s (cache) (sc/snapshot-address w))]
          (letfn [(ids [query]
                    (set (map #(get % "id")
                              (sc/search-store-snapshot snapshot query
                                                        {:limit 20
                                                         :fields [:id]}))))]
            (is (= #{"one"} (ids (sc/term-query :lexeme "cat")))
                "exact means one complete indexed term")
            (is (empty? (ids (sc/term-query :lexeme "ca")))
                "the exact builder does not analyze or expand a prefix")
            (is (= #{"one" "two"} (ids (sc/prefix-query :lexeme "cat")))
                "two matching values in document one still produce one hit")
            (is (= #{"three"} (ids (sc/prefix-query :lexeme "Cat")))
                "no analyzer lower-cases an already-normalized term")))
        (finally (sc/close! w))))))

(deftest analyzer-free-prefix-candidates-page-through-one-immutable-snapshot
  (testing "candidate continuation stays on its snapshot while the branch
            advances; constant Lucene scores are cursor data, not ranking"
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (try
        (doseq [[id lexemes]
                [["one" ["cat" "cater"]]
                 ["two" ["catalog"]]
                 ["miss" ["dog"]]]]
          (sc/add-doc w {:id {:type :string :value id}
                         :lexeme {:type :string :value lexemes :store? false}}))
        (sc/commit! w "pinned prefix generation")
        (let [old-address (sc/snapshot-address w)
              query (sc/prefix-query :lexeme "cat")]
          (with-open [snapshot (sc/open-store-snapshot s (cache) old-address)]
            (let [first-page (sc/candidate-page snapshot query
                                                {:page-size 1
                                                 :fields [:id]
                                                 :query-id [:prefix :lexeme "cat"]})]
              (is (false? (:exhausted? first-page)))
              (is (= 1 (count (:candidates first-page))))
              (sc/add-doc w {:id {:type :string :value "later"}
                             :lexeme {:type :string
                                      :value ["catfish" "category"]
                                      :store? false}})
              (sc/commit! w "branch advances past held candidate snapshot")
              (loop [after (:continuation first-page)
                     candidates (:candidates first-page)]
                (let [page (sc/candidate-page snapshot query
                                              {:page-size 1
                                               :after after
                                               :fields [:id]
                                               :query-id [:prefix :lexeme "cat"]})
                      candidates' (into candidates (:candidates page))]
                  (if (:exhausted? page)
                    (do
                      (is (= #{"one" "two"}
                             (set (map #(get % "id") candidates')))
                          "the old snapshot neither sees the later document nor
                           repeats the multi-valued document")
                      (is (= 1 (count (set (map :score candidates'))))
                          "constant score is stable paging state, not relevance"))
                    (recur (:continuation page) candidates'))))))
          (with-open [new-snapshot (sc/open-store-snapshot
                                    s (cache) (sc/snapshot-address w))]
            (is (= #{"one" "two" "later"}
                   (set (map #(get % "id")
                             (sc/search-store-snapshot new-snapshot query
                                                       {:limit 20
                                                        :fields [:id]}))))
                "a new snapshot sees the branch's later matching document")))
        (finally (sc/close! w))))))

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
        (is (seq (sk/snapshot-files s held)) "the held snapshot survives")
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
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"no migration"
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
            Lucene's `write.lock`, so a writer on a branch that is no longer in
            the registry failed its next commit with NoSuchFileException on the
            lock itself. The lock is the liveness test — held means skip the
            directory untouched — and every deletion happens under it, because
            doing the last pass after release let a writer acquire it in the gap
            and lose its whole view.

            Drift is not the way to reach this any more (the shared walk refuses
            an empty registry outright), so this uses the honest route: a branch
            deleted while a writer still holds it open."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "main doc"))
      (with-open [d (sk/konserve-directory s (cache) "doomed" sid)]
        (add-doc! d "doomed doc")
        (with-open [iw (IndexWriter. d (IndexWriterConfig. (StandardAnalyzer.)))]
          ;; the branch goes away underneath a live writer
          (sk/delete-branch! s "doomed")
          (sk/gc-cache! s (cache))
          (is (.exists (io/file (cache) "doomed" "write.lock"))
              "a held lock means the view is left alone")
          (let [doc (Document.)]
            (.add doc (TextField. "body" "still writable" Field$Store/YES))
            (.addDocument iw doc))
          (.commit iw))))))

(deftest cache-collection-reclaims-a-released-view
  (testing "the other side of the liveness test: once nothing holds the lock,
            the view goes entirely, lock file included."
    (let [s (store)
          sid (random-uuid)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "main doc"))
      (with-open [d (sk/konserve-directory s (cache) "doomed" sid)]
        (add-doc! d "doomed doc"))
      (sk/delete-branch! s "doomed")
      (is (pos? (:views (sk/gc-cache! s (cache)))))
      (is (not (.exists (io/file (cache) "doomed")))
          "including the lock file, which is only ours to remove while we hold it"))))

(deftest branch-names-are-whitelisted
  "REGRESSION: only `pool` and `snapshots` were rejected, and everything else
   went straight into `(io/file cache branch)` — while the open-time reconcile
   is a DELETE LOOP over that path. An empty name made the branch view the cache
   ROOT, so the reconcile deleted from there; `..` and `../escape` opened outside
   the cache entirely; and `a/b` registered a branch whose view `gc-cache!` could
   never match by name, so it reclaimed a LIVE branch's view."
  (testing "names that would escape or collide with the cache layout are refused"
    (let [s (store)]
      (doseq [n ["" ".." "." "a/b" "../escape" "/abs" ".hidden" "a b" "x\u0000y"
                 "pool" "snapshots"]]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not a usable branch name"
                              (sk/konserve-directory s (cache) n))
            (str (pr-str n) " must be refused at open")))
      (is (thrown? clojure.lang.ExceptionInfo (sk/konserve-directory s (cache) nil))
          "and a non-string too")))
  (testing "ordinary names still work, including the ones people actually use"
    (let [s (store)]
      (doseq [n ["main" "feature-1" "release_2.0" "v1.2.3" "a"]]
        (with-open [d (sk/konserve-directory s (cache) n)]
          (is (some? d) (str (pr-str n) " must be allowed"))))))
  (testing "and the guard covers every entry point that names a branch"
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "x"))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not a usable branch name"
                            (sk/fork! s "main" "../escape")))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not a usable branch name"
                            (sk/point-branch-at! s "" (sk/branch-snapshot s "main")))))))

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

(deftest marking-protects-a-branch-the-registry-has-forgotten
  (testing "`sweep!` is allow-list, so a branch the registry does not name is
            treated as garbage and deleted. It used to REFUSE only when the
            registry was entirely empty, which missed the case that actually
            happens: `register-branch!` runs once at Directory open while
            `flip!` writes the manifest on EVERY commit, so `delete-branch!`
            under a live writer removes the registry entry and the next commit
            puts the manifest back. The branch then read back fine — through a
            cold cache, even — and the next routine `gc!` destroyed it with no
            error anywhere.

            Protecting beats refusing: an orphan manifest is always drift, since
            the registry is written before any manifest can be, so keeping it is
            never wrong — and refusing would wedge collection until someone ran
            `repair-branches!`."
    (let [s (store)
          sid (sk/store-id-for s)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "precious"))
      ;; the registry forgets a branch whose manifest is still there
      (k/assoc s sk/branches-key #{} {:sync? true})
      (is (contains? (sk/mark s) (sk/manifest-key "main"))
          "the orphan manifest is still a root")
      (let-the-millisecond-turn-over!)
      (sk/gc! s sid)
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (is (= #{"precious"} (bodies d))
            "and collection leaves it intact rather than wiping it")))))

(deftest a-delete-under-a-live-writer-does-not-lose-its-commits
  (testing "REPRODUCED data loss. `delete-branch!` drops the registry entry and
            the manifest, but a live writer's next `flip!` writes the manifest
            back — so the branch is resurrected as a manifest nothing roots. Its
            commit succeeded and read back through a cold directory, and a
            routine `gc!` then deleted it silently and bricked the writer."
    (let [s (store)
          sid (sk/store-id-for s)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "m1"))
      (sk/fork! s "main" "feature")
      (with-open [d (sk/konserve-directory s (cache) "feature" sid)]
        (add-doc! d "f1")
        ;; the branch is deleted out from under the open Directory
        (sk/delete-branch! s "feature")
        (add-doc! d "f2")
        (is (= #{"m1" "f1" "f2"} (bodies d)) "the commit succeeds"))
      (let-the-millisecond-turn-over!)
      (sk/gc! s sid)
      (with-open [d (sk/konserve-directory s (cache) "feature" sid)]
        (is (= #{"m1" "f1" "f2"} (bodies d))
            "and a routine collection must not destroy what it committed"))
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (is (= #{"m1"} (bodies d)) "main is unaffected")))))

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
      (is (some? (sk/store-id-for s)) "the store carries konserve's id")
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

(deftest the-guard-id-comes-from-konserve
  (testing "identity is konserve's, deliberately, and it is a CONSTANT RANDOM
            UUID rather than anything derived. Deriving one from the store path
            was tried and is worse than refusing on two counts: it is a different
            KIND of name, so a component reaching a store through `connect-store`
            and another through `connect-fs-store` hold a UUID and a path for one
            store — two ids, the direction gc-guard calls out as deleting live
            data — and it is not global, since moving the store changes it while
            two unrelated stores under one mount path share it.

            A store carrying no id is refused rather than silently unguarded."
    (let [s1 (store)
          s2 (store)]
      (is (uuid? (sk/store-id-for s1)))
      (is (= (sk/store-id-for s1) (sk/store-id-for s2))
          "every connection to one store agrees")
      (let [bare (konserve.filestore/connect-fs-store
                  (str *root* "/bare") :opts {:sync? true})]
        (is (nil? (sk/store-id-for bare)))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"carries no konserve id"
                              (sk/konserve-directory bare (cache) "main")))))))

(deftest a-fork-inherits-the-parents-guard-id
  (testing "REGRESSION: `fork` dropped an explicit `:store-id`, so a caller using
            the documented option got a parent guarded under their id and a fork
            guarded under the derived one — two ids on one store, which is the
            direction that deletes live data."
    (let [s (store)
          sid (str "explicit-" (random-uuid))]
      (let [w (sc/open-store-index s (cache) "main" {:store-id sid})]
        (try
          (sc/add-doc w {:body {:type :text :value "parent"}})
          (sc/commit! w "seed")
          (let [f (sc/fork w "child")]
            (try
              (is (= sid (:store-id (:backing f)))
                  "the fork must carry the parent's id, not re-derive one")
              (finally (sc/close! f))))
          (finally (sc/close! w)))))))

;; =============================================================================
;; What createOver sets up
;; =============================================================================

(deftest a-reopened-store-branch-records-its-parent
  (testing "REGRESSION: `createOver` never called `initLastCommitId`, which
            `create` and `open` both do. So every store-backed reopen started
            with a null last-commit-id and the next commit recorded NO parent —
            a spurious root per open, and a fork whose first commit had no parent
            at all, leaving the fork point nowhere in the graph. That breaks
            `ancestors`, `common-ancestor` and `commit-graph`."
    (let [s (store)]
      (let [w (sc/open-store-index s (cache) "main")]
        (try (sc/add-doc w {:body {:type :text :value "first"}})
             (sc/commit! w "one")
             (finally (sc/close! w))))
      ;; a fresh writer over the same branch must adopt the existing head
      (let [w (sc/open-store-index s (cache) "main")]
        (try
          (is (some? (.getLastCommitId (sc/->writer w)))
              "the reopened writer must adopt the branch's head")
          (sc/add-doc w {:body {:type :text :value "second"}})
          (sc/commit! w "two")
          (let [snaps (sc/list-snapshots w)
                newest (last (sort-by :generation snaps))]
            (is (seq (:parent-ids newest))
                "and its commit must record a parent, not read as a new root"))
          (finally (sc/close! w)))))))

(deftest a-store-backed-fork-is-not-the-main-branch
  (testing "REGRESSION: `createOver` hardcoded `isMainBranch` true, so every
            store-backed branch — forks included — answered true."
    (let [s (store)]
      (let [w (sc/open-store-index s (cache) "main")]
        (try
          (sc/add-doc w {:body {:type :text :value "x"}})
          (sc/commit! w "seed")
          (is (sc/main-branch? w) "main is main")
          (let [f (sc/fork w "feature")]
            (try (is (not (sc/main-branch? f)) "a fork is not")
                 (finally (sc/close! f))))
          (finally (sc/close! w)))))))

(deftest path-only-operations-refuse-a-store-backed-writer
  (testing "REGRESSION: these dereferenced a null `basePath` and threw
            NullPointerException naming an implementation field, despite the
            docstring claiming they throw and `hasBasePath()` existing for
            exactly this and never being called."
    (let [s (store)]
      (let [w (sc/open-store-index s (cache) "main")]
        (try
          (sc/add-doc w {:body {:type :text :value "x"}})
          (sc/commit! w "seed")
          (let [bw (sc/->writer w)]
            (is (thrown-with-msg? java.io.IOException #"directory-backed"
                                  (.gc bw (java.time.Instant/now))))
            (is (thrown-with-msg? java.io.IOException #"directory-backed"
                                  (.fork bw "nope"))))
          (finally (sc/close! w)))))))

(deftest a-fork-inherits-the-parents-tuning
  (testing "REGRESSION: `fork` passed neither the analyzer nor the size knobs, and
            a fork builds a fresh IndexWriterConfig — so a parent capped at 256 MB
            produced a fork at Lucene's 5120 MB default, the cap the remote-store
            guidance exists to keep clear of S3's single-PUT limit."
    (let [s (store)]
      (let [w (sc/open-store-index s (cache) "main"
                                   {:max-merged-segment-mb 256 :ram-buffer-mb 32})]
        (try
          (sc/add-doc w {:body {:type :text :value "x"}})
          (sc/commit! w "seed")
          (let [f (sc/fork w "feature")]
            (try
              (is (= 256.0 (.getMaxMergedSegmentMB (sc/->writer f)))
                  "the fork must inherit the merged-segment cap")
              (finally (sc/close! f))))
          (finally (sc/close! w)))))))

;; =============================================================================
;; Round-tripping a snapshot address
;; =============================================================================

(deftest a-held-address-restores-a-writable-branch
  (testing "the operation that makes a snapshot address worth holding.
            `snapshot-directory` could already READ one, but nothing turned one
            back into a writable branch — so a holder could not restore to it,
            and opening the branch silently gave whatever it had moved on to.
            Primary and secondary then disagree with nothing detecting it, which
            is the failure the immutable key-map was meant to make
            unrepresentable."
    (let [s (store)
          sid (sk/store-id-for s)]
      (let [w (sc/open-store-index s (cache) "main")]
        (try (sc/add-doc w {:body {:type :text :value "first"}})
             (sc/commit! w "one")
             (finally (sc/close! w))))
      (let [held (let [w (sc/open-store-index s (cache) "main")]
                   (try (sc/snapshot-address w) (finally (sc/close! w))))]
        (is (uuid? held) "a branch reports its address")
        ;; the branch moves on
        (let [w (sc/open-store-index s (cache) "main")]
          (try (sc/add-doc w {:body {:type :text :value "second"}})
               (sc/commit! w "two")
               (finally (sc/close! w))))
        (let [w (sc/open-store-index s (cache) "main")]
          (try (is (= 2 (count (sc/search w :all)))
                   "precondition: the branch has moved past the held state")
               (finally (sc/close! w))))
        ;; and restores
        (let [w (sc/open-store-index-at s (cache) "main" held)]
          (try
            (is (= 1 (count (sc/search w :all)))
                "restoring must give the held state, not the branch's latest")
            (is (= held (sc/snapshot-address w)))
            ;; and it is writable from there
            (sc/add-doc w {:body {:type :text :value "third"}})
            (sc/commit! w "three")
            (is (= 2 (count (sc/search w :all))))
            (is (not= held (sc/snapshot-address w)) "committing moves it on")
            (finally (sc/close! w))))
        (is (some? sid))))))

(deftest forking-from-an-address-branches-the-named-state
  (testing "`fork!` copies whatever the source names NOW; this names a specific
            state. datahike's `branch-from-key-map` hands the key-map of an OLD
            commit, and forking the head there would branch from the wrong place."
    (let [s (store)]
      (let [w (sc/open-store-index s (cache) "main")]
        (try (sc/add-doc w {:body {:type :text :value "first"}})
             (sc/commit! w "one")
             (finally (sc/close! w))))
      (let [old (let [w (sc/open-store-index s (cache) "main")]
                  (try (sc/snapshot-address w) (finally (sc/close! w))))]
        (let [w (sc/open-store-index s (cache) "main")]
          (try (sc/add-doc w {:body {:type :text :value "second"}})
               (sc/commit! w "two")
               (finally (sc/close! w))))
        (sk/fork-from-snapshot! s "from-old" old)
        (let [w (sc/open-store-index s (cache) "from-old")]
          (try (is (= 1 (count (sc/search w :all)))
                   "the fork must hold the named state, not the head")
               (finally (sc/close! w))))
        (let [w (sc/open-store-index s (cache) "main")]
          (try (is (= 2 (count (sc/search w :all))) "and main is untouched")
               (finally (sc/close! w))))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"already exists"
                              (sk/fork-from-snapshot! s "from-old" old)))))))

(deftest pointing-at-a-collected-snapshot-is-refused
  (testing "the address is old, so the gc-guard cannot protect it — a collection
            that marked before the pointer landed sweeps it regardless. The
            branch must be left as it was rather than dangling, since `mark`
            resolves every branch pointer and one dangling branch stops
            collection for the whole store."
    (let [s (store)]
      (let [w (sc/open-store-index s (cache) "main")]
        (try (sc/add-doc w {:body {:type :text :value "x"}})
             (sc/commit! w "one")
             (finally (sc/close! w))))
      (let [before (sk/branch-snapshot s "main")]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not in the store"
                              (sk/point-branch-at! s "main" (random-uuid))))
        (is (= before (sk/branch-snapshot s "main"))
            "a refused move must leave the branch where it was")
        (is (set? (sk/mark s)) "and collection still works")))))

;; =============================================================================
;; The yggdrasil protocols over a store
;; =============================================================================

(deftest yggdrasil-gc-sweep-does-not-delete-its-own-roots
  (testing "REGRESSION: `gc-sweep!` discarded `snapshot-ids` and collected from
            the current branch unconditionally, so asking it to delete NOTHING
            took history from four commits to one — including the id `gc-roots`
            had just named as live. It also threw outright on a non-main current
            branch, and returned the system where the protocol asks for a
            reclamation report."
    (let [s (store)
          sys (y/create-over-store s (cache))]
      (try
        (let [sys (reduce (fn [sys i]
                            (sc/add-doc (get (:writers sys) "main")
                                        {:body {:type :text :value (str "doc " i)}})
                            (sc/commit! (get (:writers sys) "main") (str "c" i))
                            sys)
                          sys (range 4))
              before (count (p/history sys))
              report (p/gc-sweep! sys #{})]
          (is (satisfies? p/Branchable report)
              "the system, which is what yggdrasil 0.2.14 asks for — an earlier
               version returned a report, the contract in yggdrasil's dev tree
               but not in the release scriptum pins, so chaining threw")
          (is (= :main (p/current-branch report)) "and it is chainable")
          (is (= before (count (p/history sys)))
              "sweeping with no candidates must not destroy history")
          (is (= 4 (count (sc/search (get (:writers sys) "main") :all)))
              "and the documents are all still there"))
        (finally (y/close! sys))))))

(deftest yggdrasil-delete-branch-removes-it-from-the-store
  (testing "REGRESSION: `delete-branch!` closed the writer and dropped the handle,
            so `branches` stopped listing it while the store still did — its
            manifest stayed a permanent GC root and its blobs were never
            collectable."
    (let [s (store)
          sys (y/create-over-store s (cache))]
      (try
        (sc/add-doc (get (:writers sys) "main") {:body {:type :text :value "x"}})
        (sc/commit! (get (:writers sys) "main") "seed")
        (let [sys (p/branch! sys :feature)]
          (is (contains? (sk/branches s) "feature") "the store knows the branch")
          (let [sys (p/delete-branch! sys :feature)]
            (is (not (contains? (p/branches sys) :feature)))
            (is (not (contains? (sk/branches s) "feature"))
                "and deleting it must remove it from the store too")))
        (finally (y/close! sys))))))

(deftest identical-content-in-two-lineages-does-not-collide
  (testing "WHY THE ADDRESS COVERS THE PARENT. Addressing the file map alone
            gives two commits with identical content the same address, so the
            second write replaces the first's parent — and once a mark walks
            parents, one lineage's history silently becomes the other's.

            Git draws this line the same way: a tree hash is content, a commit
            hash is content plus parent."
    (let [files {"_0.cfs" (random-uuid) "_0.si" (random-uuid)}
          p1 (random-uuid)
          p2 (random-uuid)]
      (is (= (sk/snapshot-address files [p1]) (sk/snapshot-address files [p1]))
          "deterministic")
      (is (not= (sk/snapshot-address files [p1]) (sk/snapshot-address files [p2]))
          "same content, different lineage, different address")
      (is (not= (sk/snapshot-address files [p1])
                (sk/snapshot-address (assoc files "_1.si" (random-uuid)) [p1]))
          "and content still moves it"))))

(deftest a-head-address-covers-every-ancestor
  (testing "the merkle claim, made real. The values are blob addresses, which are
            content hashes of segments, so a head address covers every segment in
            the index AND every ancestor: tamper anywhere in the history and the
            head changes. Addressing the file map alone covered only the newest
            commit."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "one"))
      (let [a1 (sk/branch-snapshot s "main")]
        (with-open [d (sk/konserve-directory s (cache) "main")]
          (add-doc! d "two"))
        (let [a2 (sk/branch-snapshot s "main")]
          ;; recomputing the head from its own parts must reproduce it
          (is (= a2 (sk/snapshot-address (sk/snapshot-files s a2)
                                         (sk/snapshot-parents s a2))))
          ;; and the chain is walkable, which is what retention will need
          (is (= [a1] (sk/snapshot-parents s a2)))
          (is (empty? (sk/snapshot-parents s a1)))
          ;; a different ancestor yields a different head for the same content
          (is (not= a2 (sk/snapshot-address (sk/snapshot-files s a2) [(random-uuid)]))
              "the head moves if its history does"))))))

(defn- ancestors-of
  "Every address reachable from `a` by walking parents, `a` included.

  Stops at a parent that is not in the store rather than raising — a chain whose
  tail has been collected is the normal state today, not corruption."
  [s a]
  (loop [queue [a] seen #{}]
    (if-let [x (first queue)]
      (if (contains? seen x)
        (recur (rest queue) seen)
        (recur (into (vec (rest queue))
                     (try (sk/snapshot-parents s x) (catch Exception _ nil)))
               (conj seen x)))
      seen)))

(deftest a-merge-keeps-both-lineages-reachable
  (testing "WHY :parents IS PLURAL. `merge-from!` brings another branch's
            history in, and a commit recording only the target's previous head
            leaves that lineage unreachable the moment anything walks parents —
            and makes the head address stop covering it, so the merkle claim
            would be false after any merge.

            The codebase already models this one layer up: Lucene commit
            user-data carries `scriptum.parent-ids` as a list and yggdrasil's
            `commit-info` returns a set. A scalar here would have been the
            outlier, and widening it later is exactly the migration this layout
            exists to avoid.

            Reachability, not direct parenthood, is the assertion: `.mergeFrom`
            commits internally, so the merged lineage is recorded on that commit
            and the caller's own commit makes it a grandparent."
    (let [s (store)
          main (sc/open-store-index s (cache) "main")]
      (try
        (sc/add-doc main {:body {:type :text :value "on main"}})
        (sc/commit! main "m1")
        (let [feature (sc/fork main "feature")]
          (try
            (sc/add-doc feature {:body {:type :text :value "on feature"}})
            (sc/commit! feature "f1")
            (let [_ (sc/merge-from! main feature)
                  ;; The source's head AFTER the merge is the state that was
                  ;; merged — `merge-from!` commits it before recording.
                  feature-head (sc/snapshot-address feature)]
              ;; NO further commit: the link must exist as soon as the merge
              ;; returns. Requiring one hid a version where the address was
              ;; recorded into a pending set nothing ever consumed.

              (let [head (sc/snapshot-address main)
                    reachable (ancestors-of s head)]
                (is (contains? reachable feature-head)
                    "the merged lineage must be an ancestor of the result")
                ;; and the commit that took it in names BOTH, so the address covers both
                (let [merge-commit (first (filter #(< 1 (count (sk/snapshot-parents s %)))
                                                  reachable))]
                  (is (some? merge-commit) "some commit records two parents")
                  (is (contains? (set (sk/snapshot-parents s merge-commit)) feature-head))
                  (is (= merge-commit
                         (sk/snapshot-address (sk/snapshot-files s merge-commit)
                                              (sk/snapshot-parents s merge-commit)))
                      "and its address is computed over both"))))
            (finally (sc/close! feature))))
        (finally (sc/close! main))))))

(deftest cache-collection-leaves-foreign-directories-alone
  (testing "REGRESSION: `gc-cache!` deleted EVERY directory under `cache` that
            was not `pool`, `snapshots` or a registered branch — so an unrelated
            directory a caller had put there was destroyed. Its docstring says
            'views of branches that are gone', and a view is named like a branch
            and holds Lucene files; anything else is somebody's."
    (let [s (store)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "x"))
      (let [precious (io/file (cache) "my-important-data")]
        (.mkdirs precious)
        (spit (io/file precious "notes.txt") "do not delete")
        ;; and a genuine dead view, to prove collection still works
        (with-open [d (sk/konserve-directory s (cache) "doomed")]
          (add-doc! d "y"))
        (sk/delete-branch! s "doomed")
        (let [r (sk/gc-cache! s (cache))]
          (is (pos? (:views r)) "the dead view is still reclaimed")
          (is (not (.exists (io/file (cache) "doomed"))))
          (is (.exists (io/file precious "notes.txt"))
              "but a foreign directory must survive"))))))

(deftest collection-can-be-given-extra-keys-to-keep
  (testing "`mark` is exported telling embedders to union
            `scriptum.metadata/mark`, and until `gc!` took a whitelist there was
            nowhere to put the answer — it swept from scriptum's own keys alone,
            which on a store carrying a metadata index deleted it outright."
    (let [s (store)
          sid (sk/store-id-for s)]
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (add-doc! d "x"))
      ;; a foreign key on the same store, as an embedder would have
      (k/assoc s :metadata/roots {:index (random-uuid)} {:sync? true})
      (let-the-millisecond-turn-over!)
      (sk/gc! s sid (ku/now) nil #{:metadata/roots})
      (is (some? (k/get s :metadata/roots nil {:sync? true}))
          "a key named in extra-keys survives the sweep")
      (with-open [d (sk/konserve-directory s (cache) "main")]
        (is (= #{"x"} (bodies d)) "and the index is untouched")))))

(deftest restoring-a-branch-cannot-land-inside-a-collection
  (testing "REPRODUCED, and the worst failure found in this branch.
            `point-branch-at!` verified AFTER writing the pointer that the
            snapshot was still there, and claimed that caught a racing
            collection. It cannot: `gc!` computes its whitelist BEFORE the new
            pointer exists and sweeps AFTER the check has passed, so the check
            sees a snapshot that is still present and about to be deleted. The
            window is mark->sweep, and `sweep!` walks the whole keyspace.

            `open-store-index-at` returned a working writer and reported
            success; afterwards the branch could not be opened by any reader or
            writer, and BOTH collectors threw store-wide, permanently, for every
            branch. The only documented repair discarded the branch's data.

            Serializing the two is the fix — the same in-process boundary the
            guard already works within."
    (let [s (store)
          sid (sk/store-id-for s)]
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (add-doc! d "x"))
      (let [a1 (sk/branch-snapshot s "main")]
        (with-open [d (sk/konserve-directory s (cache) "main" sid)]
          (add-doc! d "y"))
        (let-the-millisecond-turn-over!)
        ;; Run the restore in the window between mark and sweep — the
        ;; interleaving a slow sweep gives for free.
        (let [real-mark sk/mark
              restored (promise)]
          (with-redefs [sk/mark (fn [& args]
                                  (let [m (apply real-mark args)]
                                    (when-not (realized? restored)
                                      (deliver restored
                                               (future (sk/point-branch-at! s "main" a1))))
                                    m))]
            (sk/gc! s sid))
          ;; EITHER ORDER IS CORRECT once they are serialized: the restore wins
          ;; and the snapshot survives, or the collection wins and the restore
          ;; refuses with its precondition. What must not happen is the third
          ;; outcome — a restore that reports success over a snapshot the sweep
          ;; then deletes.
          (let [outcome (try {:ok @@restored}
                             (catch java.util.concurrent.ExecutionException e
                               {:refused (ex-message (.getCause e))}))]
            (when-let [msg (:refused outcome)]
              (is (re-find #"not in the store|was collected while" msg)
                  "a refused restore must say why, not corrupt — either the
                   precondition or the post-write rollback, depending on where
                   in the collection it landed"))))
        ;; whatever order they took, the branch must be readable and collection alive
        (with-open [d (sk/konserve-directory s (cache) "main" sid)]
          (is (seq (bodies d)) "the branch must still open"))
        (is (set? (sk/mark s)) "and collection must not be wedged store-wide")
        (is (map? (sk/gc-cache! s (cache))))))))

(deftest warming-fetches-a-cold-index-in-parallel
  (testing "Lucene's demand is SERIAL — `StandardDirectoryReader` opens segment
            readers one at a time — so a cold query pays one round trip per file
            in sequence. None of Lucene's own warming hooks help: `prefetch` is
            called after this Directory has already materialized the whole blob
            inside `openInput`, `setPreload` pages in files that are on disk, and
            `setMergedSegmentWarmer` warms this writer's own merges. The fetch is
            ours, and fetching ahead of Lucene is the only lever."
    (let [s (store)]
      (let [w (sc/open-store-index s (cache) "main" {:ram-buffer-mb 0.05})]
        (try
          (dotimes [i 60]
            (sc/add-doc w {:body {:type :text :value (str "doc " i)}}))
          (sc/commit! w "bulk")
          (finally (sc/close! w))))
      (let [n-files (count (sk/read-manifest s "main"))]
        (is (< 1 n-files) "precondition: several segments")
        ;; wipe the derived cache — this is the cold machine
        (rm-rf (io/file (cache)))
        (let [r (sk/warm! s (cache) "main")]
          (is (= n-files (:fetched r))
              "warming materializes every file the snapshot names")
          (is (false? (:budget-exhausted? r)))
          (is (number? (:ms r))))
        ;; and the index reads without touching the store again
        (let [w (sc/open-store-index s (cache) "main")]
          (try (is (= 60 (sc/num-docs w))
                   "the whole index reads without touching the store again")
               (finally (sc/close! w))))
        ;; idempotent, and selective
        (is (= n-files (:fetched (sk/warm! s (cache) "main")))
            "warming again is a no-op cost")
        (rm-rf (io/file (cache)))
        (let [r (sk/warm! s (cache) "main"
                          {:only #(clojure.string/starts-with? % "segments_")})]
          (is (< 0 (:fetched r) n-files) ":only warms a subset"))
        ;; the budget is a hard ceiling, in files — scriptum's own unit; budgets
        ;; do not translate across index families and are not meant to
        (rm-rf (io/file (cache)))
        (let [r (sk/warm! s (cache) "main" {:budget 2})]
          (is (= 2 (:fetched r)) "exactly the budget, not one more")
          (is (true? (:budget-exhausted? r)))
          (is (zero? (:budget-left r))))))))

;; =============================================================================
;; Retention
;; =============================================================================

(deftest retention-bounds-a-store-backed-index
  (testing "THE THING THAT MAKES A STORE-BACKED INDEX FINITE. Every commit point
            is kept, so the branch's file map is cumulative and all of it is
            legitimately reachable — which is why `gc!` correctly reclaimed
            nothing however long the index ran. Dropping commit points removes
            their files from the manifest, and the collector can then take the
            blobs no other branch names.

            No protected-file scan is needed here, unlike the directory model:
            a drop removes a NAME from this branch's manifest, and whether the
            blob dies is decided afterwards by reachability across every branch,
            which `mark` already computes."
    (let [s (store)
          sid (sk/store-id-for s)
          w (sc/open-store-index s (cache) "main")]
      (try
        (dotimes [i 20]
          (sc/add-doc w {:body {:type :text :value (str "doc " i)}})
          (sc/commit! w (str "c" i)))
        (let [before-files (count (sk/read-manifest s "main"))
              before-points (count (filter #(clojure.string/starts-with? % "segments_")
                                           (keys (sk/read-manifest s "main"))))]
          (is (<= 20 before-points) "precondition: every commit point is retained")
          (let [dropped (sc/retain! w {:before (Instant/now)})]
            (is (pos? dropped) "commit points are dropped")
            ;; PUBLISHED AT THE NEXT FLIP, not this one. Lucene deletes a dropped
            ;; commit point's files during the checkpoint AFTER finishCommit, by
            ;; which time the manifest for the retain commit is already written.
            ;; `retain!` used to force a second commit to publish immediately,
            ;; which cost a commit point per call and grew the index forever on
            ;; the coordinator path.
            (sc/add-doc w {:body {:type :text :value "after"}})
            (sc/commit! w "publish")
            (let [after-files (count (sk/read-manifest s "main"))]
              (is (< after-files before-files)
                  "and the manifest shrinks, which is what makes the blobs collectable")))
          (is (= 21 (sc/num-docs w)) "without losing a single live document"))
        (finally (sc/close! w)))
      ;; the blobs the manifest no longer names are now reclaimable
      (let-the-millisecond-turn-over!)
      (let [collected (count (sk/gc! s sid))]
        (is (pos? collected) "the collector finally has something to take"))
      (with-open [d (sk/konserve-directory s (cache) "main" sid)]
        (is (= 21 (count (bodies-at s (cache) (sk/branch-snapshot s "main"))))
            "and the index still reads every live document")))))

(deftest retention-can-drop-named-commits
  (testing "yggdrasil's coordinator computes reachability itself and hands each
            adapter its own candidates, so a time cutoff cannot express what it
            wants — an unreachable commit may be newer than a reachable one on
            another branch."
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (try
        (dotimes [i 5]
          (sc/add-doc w {:body {:type :text :value (str "doc " i)}})
          (sc/commit! w (str "c" i)))
        (let [snaps (sort-by :generation (sc/list-snapshots w))
              victims (map :snapshot-id (take 2 snaps))
              dropped (sc/retain! w {:commit-ids victims})]
          (is (= 2 dropped) "exactly the named commit points go")
          (let [left (set (map :snapshot-id (sc/list-snapshots w)))]
            (is (empty? (filter left victims)) "and they are gone from the history")
            (is (= 5 (sc/num-docs w)) "with every live document intact")))
        (finally (sc/close! w))))))

(deftest retention-is-refused-on-a-directory-backed-index
  (testing "there a commit point holds real files another branch may share, so
            dropping one needs the protected-file scan `gc!` does."
    (let [path (str *root* "/dir-index")
          w (sc/create-index path "main")]
      (try
        (sc/add-doc w {:title "x"})
        (sc/commit! w "one")
        (is (nil? (sc/retain! w {:before (Instant/now)}))
            "retain! is a no-op there, and gc! is the way")
        (finally (sc/close! w))))))

(deftest retention-never-drops-the-branch-head
  (testing "REGRESSION: a time cutoff dropped the commit `gc-roots` had just
            reported as live, because retain's own new commit became the newest
            and the real head no longer was. The directory-backed collector
            protects every branch's head regardless of age and says so at
            length; this had no equivalent. No data was lost — the retain commit
            carries the same segments — but yggdrasil's promise that a root and
            its ancestors survive was broken."
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (try
        (dotimes [i 3]
          (sc/add-doc w {:body {:type :text :value (str "d" i)}})
          (sc/commit! w (str "c" i)))
        (let [head (.getLastCommitId (sc/->writer w))]
          (sc/retain! w {:before (Instant/now)})
          (let [ids (set (map :snapshot-id (sc/list-snapshots w)))]
            (is (contains? ids head)
                "the head that gc-roots would report must survive its own collection")))
        (finally (sc/close! w))))))

(deftest retention-that-drops-nothing-costs-nothing
  (testing "REGRESSION, and the reason a store-backed index grew without bound
            under the yggdrasil coordinator: committing is not free here, since
            the commit itself becomes a commit point. Candidates come from a
            registry that never contains scriptum's own bookkeeping commits, so
            every cycle added points no later cycle could nominate — the
            collector made the index bigger, monotonically."
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (try
        (dotimes [i 3]
          (sc/add-doc w {:body {:type :text :value (str "d" i)}})
          (sc/commit! w (str "c" i)))
        (let [points #(count (sc/list-snapshots w))
              before (points)]
          (is (= 0 (sc/retain! w {:commit-ids ["no-such-commit"]}))
              "a sweep that matches nothing drops nothing")
          (is (= before (points)) "and must not add a commit point either")
          ;; repeated no-op sweeps, as a coordinator loop would issue
          (dotimes [_ 5] (sc/retain! w {:commit-ids ["no-such-commit"]}))
          (is (= before (points)) "however many times it runs"))
        (finally (sc/close! w))))))

(deftest retention-keeps-the-commit-graph-linear
  (testing "REGRESSION: `lastCommitId` was assigned before the commit that
            established it, so retain's commits both claimed the PRE-retain head
            as parent. That left an orphan tip — never an ancestor of the head,
            so yggdrasil's reachable set never saw it and its registry never
            nominated it, which is what made the growth permanent."
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (try
        (dotimes [i 4]
          (sc/add-doc w {:body {:type :text :value (str "d" i)}})
          (sc/commit! w (str "c" i)))
        (sc/retain! w {:before (Instant/now)})
        (sc/add-doc w {:body {:type :text :value "after"}})
        (sc/commit! w "after")
        (let [snaps (sort-by :generation (sc/list-snapshots w))
              ids (set (map :snapshot-id snaps))
              ;; every commit but the oldest must name a parent that is present
              orphans (remove (fn [{:keys [parent-ids]}]
                                (or (nil? parent-ids)
                                    (some ids (if (string? parent-ids)
                                                (clojure.string/split parent-ids #",")
                                                parent-ids))))
                              (rest snaps))]
          (is (empty? orphans)
              (str "no commit may dangle off a parent that is gone: " (pr-str orphans))))
        (finally (sc/close! w))))))

(deftest retention-survives-closing-the-writer
  (testing "REGRESSION: `retain!` returned what it dropped while the shrink was
            only in the Directory's memory. Lucene drops commit points during the
            checkpoint AFTER the commit's flip, and `IndexWriter.close` publishes
            nothing (it skips a commit when nothing Lucene-level changed) — so
            retain-then-close brought every dropped commit point BACK on reopen,
            while a coordinator reading the successful return as deletion had
            already written those ids off for good."
    (let [s (store)
          sid (sk/store-id-for s)]
      (let [w (sc/open-store-index s (cache) "main")]
        (try
          (dotimes [i 6] (sc/add-doc w {:body {:type :text :value (str i)}})
                   (sc/commit! w (str "c" i)))
          (is (pos? (sc/retain! w {:before (Instant/now)})))
          (finally (sc/close! w))))
      ;; reopen: the drop must have survived the close
      (let [w (sc/open-store-index s (cache) "main")]
        (try
          (is (< (count (sc/list-snapshots w)) 6)
              "the dropped commit points must not come back")
          (is (= 6 (sc/num-docs w)) "and no document is lost")
          (finally (sc/close! w))))
      ;; and the blobs are genuinely reclaimable now
      (let-the-millisecond-turn-over!)
      (is (pos? (count (sk/gc! s sid))) "collection has something to take"))))

(deftest closing-a-writer-does-not-reuse-the-previous-commit-identity
  (testing "REGRESSION: `commitOnClose` is true, so `close` commits what is
            buffered — but nothing set commit data on that path, so the new
            commit point inherited the PREVIOUS one's uuid, timestamp and
            parents. Two index states answered to one snapshot-id: `as-of`
            returned the older, `history` listed the id twice, and the graph
            collapsed two states into one node."
    (let [s (store)]
      (let [w (sc/open-store-index s (cache) "main")]
        (sc/add-doc w {:body {:type :text :value "committed"}})
        (sc/commit! w "one")
        ;; buffered, never explicitly committed
        (sc/add-doc w {:body {:type :text :value "buffered"}})
        (sc/close! w))
      (let [w (sc/open-store-index s (cache) "main")]
        (try
          (let [snaps (sc/list-snapshots w)
                ids (map :snapshot-id snaps)]
            (is (= 2 (count snaps)) "the close produced its own commit point")
            (is (= (count ids) (count (set ids)))
                "and it must carry its own identity, not the previous one's")
            (is (= 2 (sc/num-docs w)) "with the buffered document in it"))
          (finally (sc/close! w)))))))

(deftest a-refused-store-fork-does-not-commit-the-parent
  (testing "REGRESSION: store-backed `fork` committed the parent before
            discovering the target existed — the same defect the Java `fork` was
            rewritten to remove, still present on this path."
    (let [s (store)
          w (sc/open-store-index s (cache) "main")]
      (try
        (sc/add-doc w {:body {:type :text :value "x"}})
        (sc/commit! w "one")
        (let [f (sc/fork w "feature")]
          (sc/close! f))
        (let [before (count (sc/list-snapshots w))]
          (dotimes [_ 3]
            (is (thrown? clojure.lang.ExceptionInfo (sc/fork w "feature"))))
          (is (= before (count (sc/list-snapshots w)))
              "a refused fork must leave the parent untouched"))
        (finally (sc/close! w))))))

(deftest retention-publishes-under-the-guard
  (testing "CRITICAL REGRESSION: `flip!` writes a snapshot and then a branch
            pointer — a values-then-pointer sequence — and was covered by the
            gc-guard only by accident, because a flip could not happen without a
            preceding `sync` having opened it. `retain!` publishes by calling
            `syncMetaData` directly with no sync in front, so that flip ran
            UNGUARDED: a collection landing between the two writes swept the
            snapshot the pointer was about to name, bricking the branch and
            leaving `gc!` throwing for every branch in the store.

            Driven deterministically — a collection is run in the window between
            the snapshot write and the pointer write, which is the whole race. A
            timing loop did not reproduce it reliably enough to be a regression
            test."
    (let [s (store)
          sid (sk/store-id-for s)
          w (sc/open-store-index s (cache) "main")]
      (try
        (dotimes [i 4]
          (sc/add-doc w {:body {:type :text :value (str "d" i)}})
          (sc/commit! w (str "c" i)))
        (let [real k/assoc
              fired (atom 0)]
          ;; Collect in the gap: just before EVERY branch-pointer write, while
          ;; the snapshot it is about to name has already been written. Every
          ;; one, because `retain!` flips twice — once for its own commit, which
          ;; a preceding `sync` has guarded, and once to publish the deletions,
          ;; which is the unguarded one. Intercepting only the first tested the
          ;; safe flip and passed either way.
          (with-redefs [k/assoc (fn [store' key v & opts]
                                  (when (and (vector? key) (= :manifest (second key)))
                                    (swap! fired inc)
                                    (let-the-millisecond-turn-over!)
                                    (try (sk/gc! s sid) (catch Exception _ nil)))
                                  (apply real store' key v opts))]
            (sc/retain! w {:before (Instant/now)}))
          (is (<= 2 @fired) "precondition: a collection ran inside the publish window"))
        (is (map? (sk/read-manifest s "main"))
            "the branch must still resolve — its snapshot must not have been swept")
        (is (= 4 (sc/num-docs w)) "and no committed document is lost")
        (finally (sc/close! w)))
      (is (set? (sk/mark s)) "collection must not be wedged for the store")
      (let [w2 (sc/open-store-index s (cache) "main")]
        (try (is (= 4 (sc/num-docs w2)) "and the branch reopens cold")
             (finally (sc/close! w2)))))))

(deftest detached-generations-seal-without-moving-a-ref
  (testing "a private generation derives from an immutable address and only the
            embedding root can publish the result"
    (let [s (store)
          c (cache)
          source (sc/open-store-index s c "source")]
      (try
        (sc/add-doc source {:body "base"})
        (sc/commit! source "base")
        (let [base (sc/snapshot-address source)
              before-branches (sk/branches s)
              generation (sc/begin-generation s c base {:workspace-id "generation-one"})]
          (try
            (sc/add-doc generation {:body "private"})
            (let [sealed (sc/seal-generation! generation "private")]
              (is (not= base sealed))
              (is (= #{"base" "private"} (bodies-at s c sealed))
                  "the exact returned address opens independently of a branch")
              (is (= #{"base"} (bodies-at s c base))
                  "the immutable source generation is untouched")
              (is (= base (sk/branch-snapshot s "source"))
                  "the source ref is untouched")
              (is (= before-branches (sk/branches s))
                  "sealing does not register a native branch")
              (is (not (k/exists? s (sk/manifest-key "generation-one") {:sync? true}))
                  "and does not create a hidden mutable manifest")
              ;; Model the embedding root now naming the immutable address.
              (sc/release-generation! generation)
              (is (= #{"base" "private"} (bodies-at s c sealed))))
            (finally (sc/close! generation))))
        (finally (sc/close! source))))))

(deftest detached-generation-abort-has-no-ref-side-effect
  (let [s (store)
        c (cache)
        source (sc/open-store-index s c "source")]
    (try
      (sc/add-doc source {:body "base"})
      (sc/commit! source)
      (let [base (sc/snapshot-address source)
            branches (sk/branches s)
            generation (sc/begin-generation s c base {:workspace-id "aborted-generation"})]
        (sc/add-doc generation {:body "must-not-land"})
        (sc/abort-generation! generation)
        (is (= base (sk/branch-snapshot s "source")))
        (is (= branches (sk/branches s)))
        (is (not (k/exists? s (sk/manifest-key "aborted-generation") {:sync? true})))
        (is (= #{"base"} (bodies-at s c base))))
      (finally (sc/close! source)))))

(deftest detached-generations-from-one-base-are-isolated
  (let [s (store)
        c (cache)
        source (sc/open-store-index s c "source")]
    (try
      (sc/add-doc source {:body "base"})
      (sc/commit! source)
      (let [base (sc/snapshot-address source)
            a (sc/begin-generation s c base {:workspace-id "generation-a"})
            b (sc/begin-generation s c base {:workspace-id "generation-b"})]
        (try
          (sc/add-doc a {:body "only-a"})
          (sc/add-doc b {:body "only-b"})
          (let [aa (sc/seal-generation! a)
                ba (sc/seal-generation! b)]
            (is (= #{"base" "only-a"} (bodies-at s c aa)))
            (is (= #{"base" "only-b"} (bodies-at s c ba)))
            (is (= #{"base"} (bodies-at s c base)))
            (is (= #{"source"} (sk/branches s)))
            (sc/release-generation! a)
            (sc/release-generation! b))
          (finally
            (sc/close! a)
            (sc/close! b))))
      (finally (sc/close! source)))))

(defn- segment-commit-files [store address]
  (into #{}
        (filter #(.startsWith ^String % "segments_"))
        (keys (sk/snapshot-files store address))))

(deftest detached-generations-keep-only-the-latest-lucene-commit
  (testing "external snapshot roots own history; a derived generation does not
            copy every old Lucene commit point into its current file map"
    (let [s (store)
          c (cache)
          source (sc/open-store-index s c "source")]
      (try
        ;; A native Scriptum branch deliberately retains its commit points. This
        ;; makes the source a strong regression fixture rather than assuming a
        ;; single-commit base.
        (dotimes [n 3]
          (sc/add-doc source {:body (str "base-" n)})
          (sc/commit! source (str "base-" n)))
        (let [base (sc/snapshot-address source)]
          (is (= 3 (count (segment-commit-files s base)))
              "precondition: the native source carries three commit points")
          (loop [address base
                 n 0]
            (when (< n 3)
              (let [workspace (str "latest-only-" n)
                    generation (sc/begin-generation
                                s c address {:workspace-id workspace})
                    next-address
                    (try
                      (sc/add-doc generation {:body (str "derived-" n)})
                      (let [sealed (sc/seal-generation! generation)]
                        (sc/release-generation! generation)
                        sealed)
                      (finally
                        (sc/close! generation)))]
                (let [segments (segment-commit-files s next-address)]
                  (is (= 1 (count segments))
                      (str "the new immutable state contains only its current segments_N: "
                           (pr-str segments))))
                (is (= (+ 4 n) (count (bodies-at s c next-address))))
                (is (= (+ 3 n) (count (bodies-at s c address)))
                    "deriving does not rewrite the exact source snapshot")
                (recur next-address (inc n))))))
        (finally
          (sc/close! source))))))

(deftest detached-generation-workspaces-clean-up-idempotently
  (let [s (store)
        c (cache)
        sealed-workspace "sealed-workspace"
        sealed-path (io/file c sealed-workspace)
        sealed-generation
        (sc/begin-generation s c nil {:workspace-id sealed-workspace})]
    (is (.isDirectory sealed-path))
    (sc/add-doc sealed-generation {:body "durable"})
    (let [address (sc/seal-generation! sealed-generation)]
      (is (not (.exists sealed-path))
          "sealing closes and removes the private hard-link view")
      ;; Every lifecycle operation is safe to repeat and must not make the
      ;; immutable result unreadable.
      (dotimes [_ 2]
        (sc/release-generation! sealed-generation)
        (sc/abort-generation! sealed-generation)
        (sc/close! sealed-generation))
      (is (= #{"durable"} (bodies-at s c address)))
      (is (not (.exists sealed-path))))

    (let [aborted-workspace "aborted-workspace"
          aborted-path (io/file c aborted-workspace)
          aborted-generation
          (sc/begin-generation s c nil {:workspace-id aborted-workspace})]
      (is (.isDirectory aborted-path))
      (sc/add-doc aborted-generation {:body "discarded"})
      (dotimes [_ 2]
        (sc/abort-generation! aborted-generation)
        (sc/close! aborted-generation))
      (is (not (.exists aborted-path))
          "rollback closes and removes the private hard-link view")
      (is (empty? (sk/branches s)))
      (is (not (k/exists? s (sk/manifest-key aborted-workspace) {:sync? true}))))))

(deftest detached-generation-has-an-exact-gc-mark
  (let [s (store-at (str *root* "/detached-store"))
        c (str *root* "/detached-cache")
        generation (sc/begin-generation s c nil {:workspace-id "generation-mark"})]
    (try
      (sc/add-doc generation {:body "marked"})
      (let [address (sc/seal-generation! generation)
            files (sk/snapshot-files s address)
            expected (-> #{sk/format-key (sk/snapshot-key address)}
                         (into (map sk/blob-key) (vals files)))]
        (is (empty? (sk/branches s)))
        (is (= expected (sc/mark-generation s address))
            "the external address roots exactly its snapshot and blobs")
        (let-the-millisecond-turn-over!)
        (sk/gc! s (sk/store-id-for s))
        (is (= #{"marked"} (bodies-at s c address))
            "the generation guard protects the sealed-but-unpublished address")
        (sc/release-generation! generation)
        (let-the-millisecond-turn-over!)
        (sk/gc! s (sk/store-id-for s) (ku/now) [address])
        (is (= #{"marked"} (bodies-at s c address))
            "an embedding collector preserves the detached generation by address"))
      (finally (sc/close! generation)))))

(deftest immutable-generation-audit-verifies-snapshot-and-blobs
  (let [s (store)
        c (cache)
        generation (sc/begin-generation s c nil {:workspace-id "generation-audit"})]
    (try
      (sc/add-doc generation {:body "audited"})
      (let [address (sc/seal-generation! generation)
            report (sc/verify-generation s address)]
        (is (= :ok (:status report)))
        (is (= address (:root report)))
        (is (= address (:recomputed-root report)))
        (is (empty? (:errors report)))
        (is (pos? (get-in report [:objects :blobs])))
        (is (= (get-in report [:objects :blobs])
               (get-in report [:objects :verified-blobs])))
        (sc/release-generation! generation))
      (finally (sc/close! generation)))))

(deftest immutable-generation-audit-detects-snapshot-tampering
  (let [s (store)
        c (cache)
        generation (sc/begin-generation s c nil {:workspace-id "generation-snapshot-audit"})]
    (try
      (sc/add-doc generation {:body "audited"})
      (let [address (sc/seal-generation! generation)
            snapshot (sk/read-snapshot s address)]
        (sc/release-generation! generation)
        ;; Corrupt the immutable value in place while retaining its old key.
        (k/assoc s (sk/snapshot-key address)
                 (update snapshot :parents conj (random-uuid)) {:sync? true})
        (let [report (sc/verify-generation s address)]
          (is (= :mismatch (:status report)))
          (is (= address (:root report)))
          (is (not= address (:recomputed-root report)))
          (is (some #(= :audit/snapshot-mismatch (:type %)) (:errors report)))))
      (finally (sc/close! generation)))))

(deftest immutable-generation-audit-detects-corrupt-and-missing-blobs
  (let [s (store)
        c (cache)
        generation (sc/begin-generation s c nil {:workspace-id "generation-blob-audit"})]
    (try
      (sc/add-doc generation {:body "audited"})
      (let [address (sc/seal-generation! generation)
            blob-address (first (vals (sk/snapshot-files s address)))]
        (sc/release-generation! generation)
        (k/bassoc s (sk/blob-key blob-address) (byte-array [1 2 3 4]) {:sync? true})
        (let [report (sc/verify-generation s address)]
          (is (= :mismatch (:status report)))
          (is (= address (:recomputed-root report))
              "the snapshot map remains intact")
          (is (some #(= :audit/blob-mismatch (:type %)) (:errors report))))
        (k/dissoc s (sk/blob-key blob-address) {:sync? true})
        (let [report (sc/verify-generation s address)]
          (is (= :mismatch (:status report)))
          (is (some #(= :audit/missing-blob (:type %)) (:errors report)))))
      (finally (sc/close! generation)))))

(deftest immutable-generation-audit-reports-a-missing-snapshot
  (let [address (random-uuid)
        report (sc/verify-generation (store) address)]
    (is (= {:status :mismatch
            :root address
            :recomputed-root nil
            :objects {:snapshots 0 :blobs 0 :verified-blobs 0}
            :errors [{:type :audit/missing-snapshot
                      :address address
                      :expected address
                      :recomputed nil}]}
           report))))
