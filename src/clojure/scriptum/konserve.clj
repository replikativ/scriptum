(ns scriptum.konserve
  "Konserve-backed storage for scriptum indices.

  Konserve is the source of truth; a local directory is a derived cache that
  may be deleted at any time. This is proximum's dual-storage model
  (`proximum.vectors`) applied to Lucene, and Lucene fits it better than
  vectors do: segment files are WRITE-ONCE, so a cached file is valid forever
  and never needs invalidating.

  BRANCH IDENTITY LIVES IN A MANIFEST, NOT IN A DIRECTORY TREE. The
  path-based design encodes a branch as `branches/<name>/` and enumerates
  branches with `newDirectoryStream`, which makes the filesystem the branch
  registry — the role konserve is supposed to hold. Here a branch is

      [:scriptum :manifest <branch>]  ->  {lucene-filename -> content-address}

  and each referenced blob is

      [:scriptum :blob <address>]     ->  the bytes

  Three consequences, each verified by the tests in this namespace:

  1. Forking is copying a manifest. No bytes move.
  2. Segments shared between branches are ONE blob, because the address is the
     content hash. Locally they are one INODE too (the per-branch view is made
     of hard links into a content-addressed pool), so shared segments occupy
     memory once when mmap'd.
  3. Merging is branch-local. A merge writes new blobs under new addresses and
     leaves the old ones for whoever still references them.

  That third point removes the reason `BranchAwareMergePolicy` existed, the
  second removes `BranchedDirectory`'s base/overlay composition, and reachability
  from the set of live manifests removes `BranchDeletionPolicy`'s ref-counting.

  CONCURRENCY follows konserve's contract: one writer per branch in one runtime,
  readers unconstrained. Per-branch view directories give that for free — Lucene's
  own `write.lock` lives in the view, so a second writer on the same branch fails
  loudly with LockObtainFailedException while writers on different branches
  proceed in parallel."
  (:require [clojure.java.io :as io]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.gc :as kgc]
            [konserve.gc-guard :as guard]
            [konserve.utils :as ku]
            [hasch.core :as hasch])
  (:import [org.apache.lucene.store Directory MMapDirectory]
           [java.nio.file Paths Files LinkOption]))

;; =============================================================================
;; Keys
;; =============================================================================

(defn- ->path ^java.nio.file.Path [^String s]
  (Paths/get s (make-array String 0)))

(defn manifest-key [branch] [:scriptum :manifest branch])
(defn blob-key [address] [:scriptum :blob address])

(defn read-manifest
  "The branch's `{lucene-filename -> address}` map, or `{}` when it has none."
  [store branch]
  (or (k/get store (manifest-key branch) nil {:sync? true}) {}))

(defn branches
  "Every branch that has a manifest in `store`."
  [store]
  (into #{}
        (comp (map :key)
              (filter #(and (vector? %) (= [:scriptum :manifest] (subvec % 0 2))))
              (map #(nth % 2)))
        (k/keys store {:sync? true})))

;; =============================================================================
;; Local cache: a content-addressed pool + per-branch hard-link views
;; =============================================================================

(defn- pool-file ^java.io.File [cache address]
  (io/file cache "pool" (str address)))

(defn- view-file ^java.io.File [cache branch name]
  (io/file cache branch name))

(defn- slurp-bytes ^bytes [^java.io.File f]
  (let [bs (byte-array (.length f))]
    (with-open [in (io/input-stream f)] (.readNBytes in bs 0 (alength bs)))
    bs))

(defn- spit-bytes!
  "Write `bs` to `f` via a temp file + rename, so an interrupted materialization
  cannot leave a truncated file that a later run mistakes for a cache hit."
  [^java.io.File f ^bytes bs]
  (io/make-parents f)
  (let [tmp (io/file (.getParentFile f) (str "." (.getName f) ".tmp"))]
    (with-open [out (io/output-stream tmp)] (.write out bs))
    (.renameTo tmp f)))

(defn- ensure-pooled!
  "The bytes for `address`, present in the local pool."
  ^java.io.File [store cache address]
  (let [pf (pool-file cache address)]
    (when-not (.exists pf)
      (if-let [bs (k/bget store (blob-key address)
                          (fn [{is :input-stream}]
                            (when is
                              (let [bos (java.io.ByteArrayOutputStream.)]
                                (io/copy is bos)
                                (.toByteArray bos))))
                          {:sync? true})]
        (spit-bytes! pf bs)
        (throw (ex-info "scriptum: blob referenced by a manifest is missing from the store"
                        {:address address :cache cache}))))
    pf))

(defn- link-into-view!
  "Hard-link the pooled blob into `branch`'s view under its Lucene name.

  A hard link rather than a copy: branches that share a segment then share one
  inode, so the bytes sit on disk once and mmap'd pages are shared between
  branches instead of duplicated."
  [store cache branch name address]
  (let [pf (ensure-pooled! store cache address)
        vf (view-file cache branch name)]
    (when-not (.exists vf)
      (io/make-parents vf)
      (Files/createLink (->path (.getPath vf)) (->path (.getPath pf))))
    vf))

(defn- pool!
  "Fold a freshly written view file into the pool under `address`, so a later
  fork of this branch shares its inode instead of re-materializing."
  [cache branch name address]
  (let [pf (pool-file cache address)]
    (when-not (.exists pf)
      (io/make-parents pf)
      (Files/createLink (->path (.getPath pf))
                        (->path (.getPath (view-file cache branch name)))))))

;; =============================================================================
;; The Directory
;; =============================================================================

(defn konserve-directory
  "A Lucene `Directory` for `branch`, durable in `store`, read through an
  mmap'd local cache under `cache`.

  `store-id` identifies the PHYSICAL konserve store for `konserve.gc-guard`;
  every writer sharing that store must use the same value, or a collection will
  not see this index's in-flight writes. Omitting it disables the guard, which
  is only safe on a store that is never collected."
  (^Directory [store cache branch] (konserve-directory store cache branch nil))
  (^Directory [store ^String cache ^String branch store-id]
   (.mkdirs (io/file cache branch))
   (let [live (MMapDirectory/open (->path (str cache "/" branch)))
         manifest (atom (read-manifest store branch))
         ;; Files created through this Directory but not yet synced. Tracked
         ;; explicitly because the local cache is NOT authoritative — it can
         ;; hold files from another branch sharing the pool, and Lucene must
         ;; never see those. The manifest defines what the index contains.
         session (atom #{})]
     (doseq [[n address] @manifest] (link-into-view! store cache branch n address))
     (proxy [Directory] []
       (listAll []
         ;; Re-read rather than serving the cached manifest: this is what
         ;; DirectoryReader.openIfChanged consults, so a stale manifest leaves a
         ;; long-lived reader permanently blind to new commits. Cheap and right
         ;; for remote stores too — the manifest is a small mutable pointer, so
         ;; a reader polls the pointer and never re-reads immutable segment data.
         (let [m (read-manifest store branch)]
           (reset! manifest m)
           (into-array String (sort (into (set (keys m)) @session)))))

       (fileLength [name]
         (when-let [a (get @manifest name)] (link-into-view! store cache branch name a))
         (.fileLength live name))

       (createOutput [name context]
         ;; A name this branch writes must not resolve to a stale local file
         ;; left behind by another branch sharing the cache root.
         (when (.exists (view-file cache branch name)) (.deleteFile live name))
         (swap! session conj name)
         (.createOutput live name context))

       (createTempOutput [prefix suffix context]
         (let [out (.createTempOutput live prefix suffix context)]
           (swap! session conj (.getName out))
           out))

       (sync [names]
         ;; The durability hook: Lucene syncs before it commits, so this is where
         ;; write-once files become durable and shareable.
         ;;
         ;; Guarded, because it is precisely a values-then-pointer sequence — the
         ;; blobs go in first and only the manifest write makes them reachable.
         ;; A collection landing in between would sweep blobs the manifest is
         ;; about to reference. See konserve.gc-guard.
         (.sync live names)
         (let [write! (fn []
                        (let [m (reduce (fn [m ^String n]
                                          (if (contains? m n)
                                            m
                                            (let [bs (slurp-bytes (view-file cache branch n))
                                                  address (hasch/uuid bs)]
                                              (k/bassoc store (blob-key address) bs {:sync? true})
                                              (pool! cache branch n address)
                                              (assoc m n address))))
                                        @manifest names)]
                          (k/assoc store (manifest-key branch) m {:sync? true})
                          (reset! manifest m)))]
           (if store-id
             (guard/with-unreferenced-writes store-id (write!))
             (write!))))

       (syncMetaData [] nil)

       (rename [source dest]
         (.rename live source dest)
         (when-let [a (get @manifest source)]
           (let [m (-> @manifest (dissoc source) (assoc dest a))]
             (k/assoc store (manifest-key branch) m {:sync? true})
             (reset! manifest m))))

       (deleteFile [name]
         (when (.exists (view-file cache branch name)) (.deleteFile live name))
         (swap! session disj name)
         ;; Drop the reference only. The blob stays until a GC finds it
         ;; unreachable from EVERY manifest, so a branch or a reader still
         ;; holding an older manifest keeps working.
         (when (contains? @manifest name)
           (let [m (dissoc @manifest name)]
             (k/assoc store (manifest-key branch) m {:sync? true})
             (reset! manifest m))))

       (openInput [name context]
         (when-let [a (get @manifest name)] (link-into-view! store cache branch name a))
         (.openInput live name context))

       ;; Lucene's own lock, in the per-branch view: one writer per branch, and
       ;; writers on different branches do not see each other. Exactly scriptum's
       ;; contract, with no lock of our own.
       (obtainLock [name] (.obtainLock live name))
       (close [] (.close live))
       (getPendingDeletions [] (.getPendingDeletions live))))))

;; =============================================================================
;; Branch operations
;; =============================================================================

(defn fork!
  "Branch `from` as `to`: copy the manifest. O(1) — no segment bytes move, and
  the two branches share every blob they have in common."
  [store from to]
  (when (contains? (branches store) to)
    (throw (ex-info "scriptum: branch already exists" {:branch to})))
  (let [m (read-manifest store from)]
    (k/assoc store (manifest-key to) m {:sync? true})
    m))

(defn delete-branch!
  "Forget `branch`. Blobs it referenced survive until `gc!` finds them
  unreachable from every remaining manifest."
  [store branch]
  (k/dissoc store (manifest-key branch) {:sync? true})
  nil)

(defn reachable-addresses
  "Every blob address referenced by any branch — the GC root set."
  [store]
  (into #{} (mapcat #(vals (read-manifest store %))) (branches store)))

(defn gc!
  "Collect blobs no branch references any more.

  Reachability from the live manifests IS the root set, which is why no
  ref-counting deletion policy is needed. `store-id` must be the one writers
  pass to `konserve-directory`, so the sweep can see their in-flight writes.

  Collection is EVENTUAL, not immediate. Write stamps have millisecond
  granularity and the sweep spares ties, so a blob written in the same
  millisecond as the call survives to the next cycle.

  `cutoff` defaults to now and can only make a collection MORE conservative:
  the sweep takes `min(cutoff, safe-point)`, and the safe point never runs
  ahead of now, so passing a later instant cannot force an earlier collection.
  Pass one to hold back a collection (\"nothing newer than X\"), not to hurry it.

  Blocking, returning the set of collected keys — see below.

  NOTE for shared stores: this collects blobs that no CURRENT manifest names.
  A reader on another machine pinned to an older manifest can still be holding
  one. Readers on a shared store therefore need a root of their own before this
  is safe to run there.

  `konserve.gc/sweep!` is async-only, so the channel has to be taken from here:
  returning it unconsumed would let a caller observe the store before the sweep
  had run."
  ([store store-id] (gc! store store-id (ku/now)))
  ([store store-id cutoff]
   (let [keep (into #{} (map blob-key) (reachable-addresses store))
         manifests (into #{} (map manifest-key) (branches store))
         result (<!! (kgc/sweep! store (into keep manifests) cutoff 1000
                                 {:store-id store-id}))]
     (if (instance? Throwable result) (throw result) result))))
