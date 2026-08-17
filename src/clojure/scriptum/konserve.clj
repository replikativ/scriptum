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

  CONCURRENCY, WITHIN ONE RUNTIME, follows konserve's contract: one writer per
  branch, readers unconstrained. Per-branch view directories give that for
  free — Lucene's own `write.lock` lives in the view, so a second writer on the
  same branch fails loudly with LockObtainFailedException while writers on
  different branches proceed in parallel.

  ACROSS RUNTIMES that is only half true, and the halves differ:

  - DIFFERENT branches are safe by construction, and this is the point of the
    manifest. Two writers on two branches touch disjoint keys — one manifest
    each, and blobs whose keys are content hashes, so a segment both happen to
    write is the same key holding the same bytes. Nothing needs coordinating
    because nothing is shared.
  - The SAME branch is NOT yet protected. `write.lock` lives in the local
    cache, which is per-machine, so two processes both open a writer, both
    commit, and the manifest write is last-writer-wins: the loser's segments
    are silently orphaned. Exclusion has to move into the store — a lease on
    the branch, or a compare-and-set on the manifest — before this is safe on
    a shared store. Until then, one writer per branch is the caller's job.

  THE LUCENE CONTRACT is checked against Lucene's own conformance suite rather
  than asserted here — `BaseDirectoryTestCase`, via `scriptum.tck-runner`. It
  is worth running after any change to this namespace: the exception types, the
  create/delete edge cases, and the `listAll`-under-concurrent-writes race were
  all found by it and none of them by hand."
  (:require [clojure.java.io :as io]
            [konserve.core :as k]
            [konserve.gc :as kgc]
            [konserve.gc-guard :as guard]
            [konserve.utils :as ku])
  (:import [org.replikativ.scriptum ContentHash]
           [org.apache.lucene.index IndexWriter]
           [org.apache.lucene.store Directory FilterDirectory MMapDirectory]
           [java.nio.file Paths Files FileAlreadyExistsException NoSuchFileException]))

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

(defn- address-of
  "The content address of a local file: SHA-512 -> UUID5, read in bounded chunks.

  `ContentHash` rather than `hasch/uuid` because it is the SAME function
  scriptum's merkle commits already use, so a blob's address IS its merkle leaf
  hash — the manifest is the tree, and verifying a commit is re-hashing the
  blobs it names. hasch would give a second, incompatible address function for
  the same bytes, and would additionally require them all in memory at once."
  [^java.io.File f]
  (ContentHash/hashFile (->path (.getPath f))))

(defn- ensure-pooled!
  "The bytes for `address`, present in the local pool.

  STREAMED, via a temp file + rename: a segment is materialized through a 64 KB
  window rather than a heap buffer the size of the segment, and an interrupted
  materialization cannot leave a truncated file that a later run mistakes for a
  cache hit."
  ^java.io.File [store cache address]
  (let [pf (pool-file cache address)]
    (when-not (.exists pf)
      (io/make-parents pf)
      (let [tmp (io/file (.getParentFile pf) (str "." (.getName pf) ".tmp"))
            got (k/bget store (blob-key address)
                        (fn [{is :input-stream}]
                          (when is
                            ;; :buffer-size explicitly — io/copy defaults to 1 KiB,
                            ;; and the bound this function advertises should be the
                            ;; one it actually uses.
                            (with-open [out (io/output-stream tmp)]
                              (io/copy is out :buffer-size 65536))
                            true))
                        {:sync? true :streaming? true})]
        (if got
          (.renameTo tmp pf)
          (do (.delete tmp)
              (throw (ex-info "scriptum: blob referenced by a manifest is missing from the store"
                              {:address address :cache cache}))))))
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

(def remote-tuning
  "Lucene size knobs for an index whose segments are blobs in a remote store.

  Lucene's defaults assume a local disk, where a segment is a file and a 5 GB
  one costs nothing to leave lying there. Against an object store the same
  segment is a blob written and read WHOLE, which changes what the defaults
  buy you:

    :max-merged-segment-mb 256   (Lucene: 5120)
      The largest blob, and so the peak memory a commit costs — konserve's S3
      backing assembles a blob in the heap to PUT it, roughly twice its size.
      It also has to stay clear of S3's 5 GB single-PUT ceiling, since that
      backing does not do multipart. 256 MB bounds the heap at ~0.5 GB and
      leaves an order of magnitude of headroom.

    :ram-buffer-mb 32            (Lucene: 16)
      The other end of the distribution: this bounds segments created by a
      FLUSH, before any merge. Raised rather than lowered, because against a
      remote store the cost is per REQUEST — measured at ~5 objects per commit
      with a median blob of ~500 bytes — so fewer, larger flushes are cheaper
      than many small ones.

  Pass to `scriptum.core/create-index` or `open-branch`. These are defaults to
  start from, not tuned constants: the right cap depends on the heap you have
  and how large the index gets."
  {:max-merged-segment-mb 256
   :ram-buffer-mb 32})

(defn konserve-directory
  "A Lucene `Directory` for `branch`, durable in `store`, read through an
  mmap'd local cache under `cache`.

  `store-id` keys `konserve.gc-guard`'s safe point, and should be the store's
  own `:id` — konserve's LOGICAL store identity, which is deliberately the same
  across machines and backends holding the same store, and is NOT a name for
  the bytes on this disk.

  That distinction decides whether a collection is correct, and only one
  direction is dangerous:

  - Every writer on the SAME bytes passing the SAME id is the requirement. Get
    that wrong — two connections to one store made with different ids — and a
    sweep runs against a safe point that cannot see the other writer's in-flight
    blobs, and deletes what a manifest is about to reference.
  - Two SEPARATE stores sharing one id (a store and its replica) is merely
    conservative: each sweep is held back by the other's writers. Nothing is
    lost, though a continuously-written replica can hold a collection off.

  So the id may be coarser than the physical store, never finer. Omitting it
  disables the guard, which is only safe on a store that is never collected."
  (^Directory [store cache branch] (konserve-directory store cache branch nil))
  (^Directory [store ^String cache ^String branch store-id]
   (.mkdirs (io/file cache branch))
   (let [live (MMapDirectory/open (->path (str cache "/" branch)))
         manifest (atom (read-manifest store branch))
         ;; Files created through this Directory but not yet synced. Tracked
         ;; explicitly because the local cache is NOT authoritative — it can
         ;; hold debris from an interrupted earlier session, and Lucene must
         ;; never see that. Manifest + session is what this index contains.
         session (atom #{})
         ;; Logical existence, which is NOT the same as a file being present in
         ;; the cache: the manifest names what is durable, the session names
         ;; what this Directory has created since. Every contract check below
         ;; asks this rather than the filesystem.
         has? (fn [name] (or (contains? @manifest name) (contains? @session name)))
         ;; Every read-modify-write of the manifest holds this. The atom alone
         ;; is not enough: `sync`, `rename` and `deleteFile` each read the
         ;; manifest, derive a new one, put it in the store and only then
         ;; install it — three steps that interleave and lose one of two edits.
         lock (Object.)]
     (doseq [[n address] @manifest] (link-into-view! store cache branch n address))
     ;; Reconcile the view with the manifest before anything reads it. A cached
     ;; file the manifest does not name is debris from a session that died
     ;; mid-write, and nothing else will ever remove it: `listAll` never names
     ;; it, so Lucene's `IndexFileDeleter` never asks for it, and `deleteFile`
     ;; now refuses it. Left alone it pins an inode in the view forever.
     ;;
     ;; `write.lock` is exempt — it is Lucene's, not ours, and deleting one held
     ;; by a live writer in this JVM would break the exclusion it provides.
     (doseq [^String n (.list (io/file cache branch))]
       (when-not (or (contains? @manifest n) (= n IndexWriter/WRITE_LOCK_NAME))
         (.delete (view-file cache branch n))))
     ;; FilterDirectory over the live MMapDirectory, not a bare Directory: this
     ;; IS an mmap'd directory with a store-backed materialization layer in
     ;; front, and `FilterDirectory.unwrap` is how Lucene detects that. A bare
     ;; Directory denies being mmap-backed while handing out IndexInputs that
     ;; report otherwise — an inconsistency BaseDirectoryTestCase catches
     ;; (testIsLoaded), and which would also hide preload/madvise from Lucene.
     (proxy [FilterDirectory] [live]
       (listAll []
         ;; Re-read rather than serving the cached manifest: this is what
         ;; DirectoryReader.openIfChanged consults, so a stale manifest leaves a
         ;; long-lived reader permanently blind to new commits. Cheap and right
         ;; for remote stores too — the manifest is a small mutable pointer, so
         ;; a reader polls the pointer and never re-reads immutable segment data.
         ;;
         ;; `compare-and-set!`, never `reset!`: this runs on READER threads,
         ;; which are unconstrained, and a blind overwrite can install a
         ;; manifest OLDER than one `sync` just committed. `rename` then fails
         ;; to find `pending_segments_N`, never writes `segments_N`, and the
         ;; store is left naming a file no commit points at — reopening the
         ;; index throws IndexNotFoundException. If anything advanced the
         ;; manifest while we were reading the store, that value is the newer
         ;; one and has to win.
         (let [before @manifest
               m (read-manifest store branch)]
           (compare-and-set! manifest before m)
           (into-array String (sort (into (set (keys @manifest)) @session)))))

       (fileLength [name]
         (when-let [a (get @manifest name)] (link-into-view! store cache branch name a))
         (.fileLength live name))

       (createOutput [name context]
         ;; Lucene's contract: creating a file that already exists is an error,
         ;; never a silent truncation — that is how it detects a colliding
         ;; segment name instead of destroying a live segment.
         (when (has? name)
           (throw (FileAlreadyExistsException. name)))
         ;; A local file the manifest does not name is debris from an
         ;; interrupted session, not content. Drop it, so Lucene does not
         ;; append to a half-written file left by a previous run.
         (when (.exists (view-file cache branch name)) (.deleteFile live name))
         ;; Create FIRST, announce second. `session` is what `listAll` reports,
         ;; and a concurrent reader is entitled to open anything listAll named
         ;; — so announcing before the file exists hands it a NoSuchFileException
         ;; (BaseDirectoryTestCase.testThreadSafetyInListAll). The reverse gap is
         ;; harmless: a file on disk that listAll has not announced yet is simply
         ;; not visible, which is what an unannounced file should be.
         (let [out (.createOutput live name context)]
           (swap! session conj name)
           out))

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
                        (locking lock
                          (let [m (reduce (fn [m ^String n]
                                            (if (contains? m n)
                                              m
                                              (let [vf (view-file cache branch n)
                                                    address (address-of vf)]
                                              ;; An InputStream, not the bytes: konserve streams a
                                              ;; blob in through a fixed buffer, so a segment costs
                                              ;; that buffer and not its own size. (A java.io.File
                                              ;; would be the obvious argument and `bassoc` claims
                                              ;; to take one, but konserve's `blob->channel` File
                                              ;; branch mis-hints it as a String and throws.)
                                                (with-open [in (io/input-stream vf)]
                                                  (k/bassoc store (blob-key address) in {:sync? true}))
                                                (pool! cache branch n address)
                                                (assoc m n address))))
                                          @manifest names)]
                            (k/assoc store (manifest-key branch) m {:sync? true})
                            (reset! manifest m)
                          ;; Synced names are the manifest's now. Handing them
                          ;; over keeps a name from living in both sets, so
                          ;; `rename` and `deleteFile` have one place to edit.
                            (swap! session #(reduce disj % names)))))]
           (if store-id
             (guard/with-unreferenced-writes store-id (write!))
             (write!))))

       (syncMetaData [] nil)

       (rename [source dest]
         (.rename live source dest)
         (locking lock
           ;; The session has to follow the rename too. Without this, `listAll`
           ;; keeps naming `source` — which no longer exists — and never names
           ;; `dest`, which does; `deleteFile dest` then throws NoSuchFile for a
           ;; file that is right there, and `createOutput dest` sees no conflict
           ;; and DELETES the renamed content. Lucene's commit path renames a
           ;; file that is already in the manifest, which is why this stayed
           ;; invisible: the suite and Lucene's own testRename never assert
           ;; `listAll` after a rename.
           (swap! session #(if (contains? % source) (-> % (disj source) (conj dest)) %))
           (when-let [a (get @manifest source)]
             (let [m (-> @manifest (dissoc source) (assoc dest a))]
               (k/assoc store (manifest-key branch) m {:sync? true})
               (reset! manifest m)))))

       (deleteFile [name]
         ;; Deleting what is not there is an error, not a no-op: Lucene relies
         ;; on it to notice that its bookkeeping and the directory disagree.
         (when-not (has? name)
           (throw (NoSuchFileException. name)))
         (when (.exists (view-file cache branch name)) (.deleteFile live name))
         (swap! session disj name)
         ;; Drop the reference only. The blob stays until a GC finds it
         ;; unreachable from EVERY manifest, so a branch or a reader still
         ;; holding an older manifest keeps working.
         (locking lock
           (when (contains? @manifest name)
             (let [m (dissoc @manifest name)]
               (k/assoc store (manifest-key branch) m {:sync? true})
               (reset! manifest m)))))

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
  ref-counting deletion policy is needed. `store-id` must be the one every
  writer on these bytes passes to `konserve-directory` — see there for why
  agreeing matters and which way it fails — so the sweep can see their
  in-flight writes.

  Collection is EVENTUAL, not immediate. Write stamps have millisecond
  granularity and the sweep spares ties, so a blob written in the same
  millisecond as the call survives to the next cycle.

  `cutoff` defaults to now and can only make a collection MORE conservative:
  the sweep takes `min(cutoff, safe-point)`, and the safe point never runs
  ahead of now, so passing a later instant cannot force an earlier collection.
  Pass one to hold back a collection (\"nothing newer than X\"), not to hurry it.

  Synchronous, returning the set of collected keys.

  It sweeps with `{:sync? true}` rather than taking konserve's channel with
  `<!!`, which is what this did before. `<!!` DEADLOCKS inside a go block, and
  `gc!` is reachable from a datahike writer that does run in async contexts — so
  that was a latent hang, not a style choice. Staying synchronous also keeps the
  call stack sync all the way up through `scriptum.core/gc!` to datahike's
  secondary-index adapter, which has no async seam to thread this through.

  NOTE for shared stores: this collects blobs that no CURRENT manifest names.
  A reader on another machine pinned to an older manifest can still be holding
  one. Readers on a shared store therefore need a root of their own before this
  is safe to run there."
  ([store store-id] (gc! store store-id (ku/now)))
  ([store store-id ts]
   ;; THE GUARD IS READ BEFORE THE MANIFESTS ARE WALKED, and that order is the
   ;; whole point — `sweep!` cannot do it for us, because by the time it has a
   ;; whitelist the walk has already happened.
   ;;
   ;; A sync that lands its manifest between the walk and the sweep is invisible
   ;; to a cutoff taken later: the walk did not see its blobs (the manifest
   ;; still named the old ones) and by sweep time the guard is closed again, so
   ;; the cutoff snaps back to `ts` and blobs stamped earlier are swept out from
   ;; under a manifest that now names them.
   ;;
   ;; Reading here pins the cutoff at or before any sequence already open, so
   ;; whatever a concurrent sync writes is younger than the cutoff and survives
   ;; however the walk turned out.
   (let [cutoff (if store-id (guard/cutoff store-id ts) ts)
         keep (into #{} (map blob-key) (reachable-addresses store))
         manifests (into #{} (map manifest-key) (branches store))]
     (kgc/sweep! store (into keep manifests) cutoff 1000 {:sync? true}))))
