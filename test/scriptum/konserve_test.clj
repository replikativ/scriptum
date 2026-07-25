(ns scriptum.konserve-test
  "Pins the konserve-backed storage model.

  Each test names a property the manifest design claims, and several of them
  are regressions for mistakes made while arriving at it — a flat cache that let
  one branch continue another's index, and a cached manifest that left a
  long-lived reader blind to new commits."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [konserve.filestore :refer [connect-fs-store]]
            [clojure.set :as set]
            [konserve.core :as k]
            [konserve.gc-guard :as guard]
            [konserve.utils :as ku]
            [scriptum.konserve :as sk])
  (:import [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.document Document TextField Field$Store]
           [org.apache.lucene.index IndexWriter IndexWriterConfig DirectoryReader]
           [org.apache.lucene.search IndexSearcher]
           [org.apache.lucene.store LockObtainFailedException]
           [java.nio.file Files LinkOption Paths]))

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
