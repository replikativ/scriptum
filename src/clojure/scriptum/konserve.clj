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
  branch, readers unconstrained. Lucene's own `write.lock` lives in the
  per-branch view, so a second writer fails loudly with
  LockObtainFailedException while writers on different branches proceed in
  parallel.

  THAT LOCK'S SCOPE IS THE CACHE DIRECTORY, NOT THE MACHINE, and `cache` is a
  caller-supplied string with no enforced relationship to branch identity. Two
  writers on one branch with DIFFERENT cache paths — a per-process temp cache,
  a container remount, a symlink — both succeed, on one machine, silently.
  Within a single cache path it is solid: NativeFSLockFactory keeps a
  process-wide held set (so a second writer in this JVM is refused by name) and
  an OS advisory lock the kernel releases on abnormal exit (so another process
  on the same machine is refused too). Nothing spans machines: the lock file is
  in a local cache, so there is no shared file to contend on.

  ACROSS RUNTIMES that is only half true, and the halves differ:

  - DIFFERENT branches are safe by construction, and this is the point of the
    manifest. Two writers on two branches touch disjoint keys — one manifest
    each, and blobs whose keys are content hashes, so a segment both happen to
    write is the same key holding the same bytes. Nothing needs coordinating
    because nothing is shared.
  - The SAME branch is NOT protected. `write.lock` lives in the local cache,
    which is per-machine, so two processes both open a writer, both commit, and
    the manifest write is last-writer-wins: the loser's segments are silently
    orphaned. The manifest is written unconditionally, which is the same shape
    as datahike's branch head (datahike#878) and carries the same verdict.

    RESERVED CONCURRENCY 1 IS NOT A FIX. It narrows the window; it does not
    close it, because a deploy or container replacement still runs two
    environments at once. Nor does a single serialized stream suffice on its
    own: Lambda FREEZES an environment rather than terminating it, so a thawed
    one holds a manifest atom, a Lucene writer and a `/tmp` cache from before
    another environment advanced the branch. Open and close a writer per
    invocation rather than caching one, or that stale writer derives its next
    manifest from a superseded one. (The view cache is safe either way — a
    stale entry is repaired by inode on first touch; see `link-into-view!`.)

    The fix is a compare-and-set on the manifest. `konserve-s3` already
    implements `put-object-conditional`, so on S3 the primitive exists and is
    opt-in via `:config {:optimistic-locking-retries n}` — but taking it needs
    `sync` restructured to RETRY from a re-read manifest rather than write one
    it computed from a cached value, and the GC guard is in-process regardless,
    so a collection on one machine still cannot see another's in-flight blobs.
    Until both land, treat multi-writer as unsupported: one writer per branch,
    in one JVM.

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
           [org.apache.lucene.store AlreadyClosedException Directory FilterDirectory MMapDirectory]
           [java.nio.file Paths Files LinkOption StandardCopyOption
            FileAlreadyExistsException NoSuchFileException]))

;; =============================================================================
;; Keys
;; =============================================================================

(defn- ->path ^java.nio.file.Path [^String s]
  (Paths/get s (make-array String 0)))

(defn manifest-key
  "The branch pointer. Holds a SNAPSHOT ADDRESS, not the file map — see
  `snapshot-key`. Kept under this name because it is what a branch resolves
  through, and renaming it would be a third layout."
  [branch]
  [:scriptum :manifest branch])

(defn blob-key [address] [:scriptum :blob address])

(defn snapshot-key
  "An immutable `{lucene-filename -> blob-address}` map, addressed by content."
  [address]
  [:scriptum :snapshot address])

(defn snapshot-address
  "The content address of a file map: a merkle root over the blobs it names.

  `ContentHash/hashMap` sorts keys before serializing, so this is deterministic
  and — being the same hash family the leaf addresses use — makes the snapshot a
  genuine interior node rather than a checksum that happens to sit above one."
  [files]
  (ContentHash/hashMap files))

(defn read-snapshot
  "The file map at `address`. Throws when the address names nothing.

  NOT `{}` on a miss. A pointer into a snapshot that does not exist is
  corruption — a swept snapshot, an interrupted fork, an unmigrated store — and
  returning an empty map made every one of those look like an empty branch.
  That is the worst possible reading: `reachable-addresses` then whitelists
  nothing for the branch, so the next `gc!` deletes the blobs that would have
  made the damage recoverable. Loud beats quiet here."
  [store address]
  (or (k/get store (snapshot-key address) nil {:sync? true})
      (throw (ex-info (str "scriptum: snapshot " address " is missing — the branch "
                           "points at a tree that is not in the store")
                      {:address address}))))

(defn branch-snapshot
  "The snapshot address `branch` points at, or nil for a branch with no commit.

  THE ONE MUTABLE CELL. Every other key in the store is immutable and content-
  addressed, which is what makes a snapshot address something a caller can hold:
  it names an index state that cannot change under them, where a branch name
  cannot. datahike's secondary-index key-map wants this rather than the branch,
  for the same reason proximum's carries a `:commit-id`.

  THE ONE CHOKE POINT for an unmigrated v1 cell, which is why it throws rather
  than passing the value along: a v1 cell holds the file map itself, and a map
  used as a snapshot address resolves to nothing. Every read of a branch goes
  through here, so catching it once catches it everywhere."
  [store branch]
  (let [v (k/get store (manifest-key branch) nil {:sync? true})]
    (when (map? v)
      (throw (ex-info (str "scriptum: branch " branch " still holds a v1 file map — "
                           "the store was stamped without migrating this branch")
                      {:branch branch :layout 1})))
    v))

(def format-key
  "Where the store records which manifest layout it is in."
  [:scriptum :format])

(def format-version
  "The manifest layout this code writes and understands.

  1 — `[:scriptum :manifest <branch>]` held the file map itself: a mutable cell
      containing the whole tree.
  2 — that cell holds a SNAPSHOT ADDRESS, and the tree lives at
      `[:scriptum :snapshot <address>]` as an immutable, content-addressed
      value. One mutable pointer per branch, everything below it a value."
  2)

(def branches-key
  "Where the branch registry lives: a set of branch names."
  [:scriptum :branches])

(defn manifest-branches
  "Every branch with a manifest key, by KEYSPACE SCAN rather than the registry.

  Expensive, and deliberately not on any read path — `branches` answers from
  the registry. This is for `repair-branches!`, the one job where trusting the
  registry is the bug, because rebuilding it is the point."
  [store]
  (into #{}
        (comp (map :key)
              (filter #(and (vector? %) (= 3 (count %))
                            (= [:scriptum :manifest] (subvec % 0 2))))
              (map #(nth % 2)))
        (k/keys store {:sync? true})))

(defn ensure-format!
  "Stamp a fresh store with this layout, or refuse one we cannot read.

  THE REFUSAL IS THE POINT. Without it, a store written by a different scriptum
  is read as though it were this one, and fails as corruption somewhere far from
  the cause. That has already happened once in miniature: the blob address
  function changed from `hasch/uuid` to `ContentHash` during development, and
  nothing could distinguish a store written before from one written after —
  both produce valid-looking UUIDs for the same bytes, so the only symptom was
  a manifest naming blobs that were not there.

  NO MIGRATION, DELIBERATELY. Layout 1 was never released — no released scriptum
  contains this namespace at all, and the version stamp itself postdates every
  build that exists — so the only v1 stores are development ones, which are
  cheaper to discard than to convert. Nothing here migrates path-based
  (`BranchedDirectory`) indices either; that is a different storage model and a
  separate problem.

  Writing the converter anyway was actively harmful: it read the branch REGISTRY
  to decide what to convert, and an incomplete registry is exactly what this
  repository's earlier missing-GC-root bug produced. Branches it missed kept
  their v1 maps, the stamp then recorded the store as converted so it never
  retried, those branches read as empty, and the next `gc!` swept their blobs.
  Refusing has none of that surface.

  A store is fresh iff nothing has registered a branch in it — `register-branch!`
  runs immediately after this on the first open, so the registry existing means
  the store predates this layout. One extra key read, and only when unstamped.

  Costs one key read per Directory open, next to the one `register-branch!`
  already does — nothing on the `listAll` path. Returns the store's version."
  [store]
  (let [v (:version (k/get store format-key nil {:sync? true}))]
    (cond
      (= v format-version) v

      (some? v)
      (throw (ex-info (str "scriptum: this store is manifest layout " v
                           ", and this scriptum reads only " format-version
                           (when (< v format-version)
                             " — layout 1 was never released, so there is no migration"))
                      {:store-version v :supported format-version}))

      ;; Unstamped: fresh, or written before the stamp existed.
      (k/exists? store branches-key {:sync? true})
      (throw (ex-info (str "scriptum: this store predates manifest layout " format-version
                           " and cannot be read — layout 1 was never released, so there is "
                           "no migration from it")
                      {:store-version :pre-stamp :supported format-version}))

      :else
      (do (k/assoc store format-key {:version format-version} {:sync? true})
          format-version))))

(defn read-manifest
  "The branch's `{lucene-filename -> address}` map, or `{}` when it has none.

  Two reads, not one: the pointer, then the snapshot it names. Against a remote
  store that is the cheaper shape rather than the more expensive one — the
  pointer is a few bytes and the snapshot is immutable, so a poller re-reads
  only the pointer and a cache keyed by address never needs invalidating."
  [store branch]
  (if-let [address (branch-snapshot store branch)]
    (read-snapshot store address)
    {}))

(defn branches
  "Every branch of this index, from the registry.

  A REGISTRY rather than a scan of the keyspace. The manifest key encodes its
  branch name, so branches could be derived by filtering `k/keys` — and were —
  but `k/keys` is not a listing: konserve OPENS AND READS EVERY BLOB to recover
  its key. That made enumerating two branches cost one read per segment in the
  store. Measured on a 155-key index it was 20.4 ms against 0.23 ms for a single
  lookup, ~90x, and on an object store it is one GET per object.

  proximum (`:branches`) and stratum (`[:datasets :branches]`) both keep the
  same registry; this was the outlier."
  [store]
  (or (k/get store branches-key nil {:sync? true}) #{}))

(defn register-branch!
  "Record `branch` in the registry, if it is not already there.

  MUST HAPPEN BEFORE THE BRANCH'S FIRST MANIFEST WRITE, and that ordering is
  the opposite of the values-then-pointer rule everywhere else here. Elsewhere
  the pointer is written last because it makes values REACHABLE. The registry
  is a GC ROOT: `gc!` whitelists a manifest only for a branch the registry
  names, so a branch with a manifest the registry has forgotten has its
  manifest and every blob it names swept. Registering first means a crash
  leaves a registered branch with no manifest — harmless, `read-manifest`
  returns `{}` — never the reverse.

  Returns true when it wrote."
  [store branch]
  (when-not (contains? (branches store) branch)
    (k/update store branches-key #(conj (or % #{}) branch) {:sync? true})
    true))

(defn repair-branches!
  "Rebuild the registry by scanning the keyspace for manifests.

  The registry is authoritative, so nothing consults the keyspace on the read
  path — which means drift, from a crash between registering and writing or
  from a store assembled by other means, cannot repair itself. This is the way
  back, and the expensive scan `branches` used to do on every call.

  Returns the repaired set."
  [store]
  (ensure-format! store)
  (let [found (manifest-branches store)
        merged (into (branches store) found)]
    (k/assoc store branches-key merged {:sync? true})
    merged))

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

(defn- same-inode? [^java.io.File a ^java.io.File b]
  (= (Files/getAttribute (->path (.getPath a)) "unix:ino" (make-array LinkOption 0))
     (Files/getAttribute (->path (.getPath b)) "unix:ino" (make-array LinkOption 0))))

(defn- link-into-view!
  "Hard-link the pooled blob into `branch`'s view under its Lucene name.

  A hard link rather than a copy: branches that share a segment then share one
  inode, so the bytes sit on disk once and mmap'd pages are shared between
  branches instead of duplicated.

  WRITTEN AS A CONVERGENCE LOOP, not a sequence of steps, because `openInput`
  is an unconstrained-concurrency path and every step here races something:
  two readers both finding the name absent and both linking, one repairing a
  stale entry while another reads it, a pooled blob being renamed into place
  underneath. Each of those was a different exception escaping `openInput` —
  and `Directory` permits only NoSuchFile/FileNotFound or a plain IOException
  there, never FileAlreadyExists.

  So the postcondition is CHECKED rather than assumed: the loop ends when the
  view entry is the pooled inode, whoever put it there. Losing a race is
  success, and any of these failures simply means someone else is mid-repair,
  so it retries.

  A view entry that is not the pooled inode is stale — the same Lucene name
  mapped to a different address in an earlier session. Serving it would hand
  Lucene content the manifest does not name; Lucene notices some of these and
  calls them index corruption, which is wrong, since the store is intact and
  only the derived cache is stale. Repaired, not raised.

  Compared by INODE rather than by re-hashing, because the pool's filename IS
  the content address. Two stats, no read."
  [store cache branch name address]
  (let [pf (ensure-pooled! store cache address)
        vf (view-file cache branch name)]
    (io/make-parents vf)
    (loop [attempt 0]
      (cond
        (and (.exists vf) (same-inode? vf pf))
        vf

        (> attempt 8)
        (throw (java.io.IOException.
                (str "scriptum: could not materialize " name " for branch " branch)))

        :else
        (do
          (try
            (if (.exists vf)
              ;; Stale entry. Link under a private name and move it into place:
              ;; unlink-then-relink would leave a window where the name does not
              ;; exist at all, which a concurrent reader sees as NoSuchFile for a
              ;; file the manifest names.
              (let [tmp (io/file (.getParentFile vf)
                                 (str "." name "." (random-uuid) ".link"))]
                (try
                  (Files/createLink (->path (.getPath tmp)) (->path (.getPath pf)))
                  (Files/move (->path (.getPath tmp)) (->path (.getPath vf))
                              (into-array java.nio.file.CopyOption
                                          [StandardCopyOption/REPLACE_EXISTING
                                           StandardCopyOption/ATOMIC_MOVE]))
                  (finally (.delete tmp))))
              (Files/createLink (->path (.getPath vf)) (->path (.getPath pf))))
            (catch FileAlreadyExistsException _ nil)
            (catch NoSuchFileException _ nil))
          (recur (inc attempt)))))))

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

(def reserved-branch-names
  "Branch names that would collide with the cache's own layout.

  A branch's view is `cache/<branch>`, and `pool` and `snapshots` are siblings
  of it, so a branch with either name SHARES a directory with them. The damage
  is not symmetric and `pool` is the bad one: opening that branch runs the
  open-time reconcile, which deletes every file the branch's manifest does not
  name — i.e. the entire content-addressed pool, for every branch. `snapshots`
  costs a reader its view and leaks the branch's own.

  Rejected at open rather than escaped, because a name is a user-facing
  identifier and silently rewriting it is worse than refusing two words."
  #{"pool" "snapshots"})

(defn- check-branch-name [branch]
  (when (contains? reserved-branch-names branch)
    (throw (ex-info (str "scriptum: '" branch "' is reserved — it would share a cache "
                         "directory with the content pool or the snapshot views")
                    {:branch branch :reserved reserved-branch-names}))))

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
   (check-branch-name branch)
   (.mkdirs (io/file cache branch))
   ;; Refuse a layout we cannot read before touching anything, then register the
   ;; branch before anything can write a manifest through this Directory. One key
   ;; read each for a store and branch already known, plus a write the first time
   ;; — not per commit. See `register-branch!` for why the ordering there is the
   ;; inverse of the values-then-pointer rule.
   (ensure-format! store)
   (register-branch! store branch)
   (let [live (MMapDirectory/open (->path (str cache "/" branch)))
         ;; ONE atom holding BOTH halves, and that is a correctness requirement
         ;; rather than tidiness. As two atoms, `listAll` claimed the pointer with
         ;; one `compare-and-set!` and installed the map with a second, and a
         ;; reader preempted between them let a slower reader win the pointer CAS
         ;; and lose the map CAS. The result — a new pointer with an old map — is
         ;; PERMANENT: every later `listAll` finds the store pointer equal to the
         ;; one it holds, re-reads nothing, and the Directory is blind to every
         ;; subsequent commit. That is precisely the failure the CAS was added to
         ;; prevent, reintroduced one level up. One atom, one CAS, no window.
         state (atom (let [a (branch-snapshot store branch)]
                       {:pointer a :files (if a (read-snapshot store a) {})}))
         files-now (fn [] (:files @state))
         ;; Has the in-memory map moved off `pointer`? Set by every edit,
         ;; cleared by the flip in `syncMetaData`. A flag rather than comparing
         ;; `snapshot-address` against `pointer`, because `listAll` asks on every
         ;; call and that comparison is a SHA-512 over the serialized map.
         dirty (atom false)
         ;; Has a `sync` landed blobs since the last flip? Distinct from `dirty`,
         ;; and the distinction is worth two store writes per commit.
         ;;
         ;; Lucene deletes a superseded `segments_N` AFTER the commit that
         ;; supersedes it, so every commit ends with an edit that arrives too
         ;; late for its own flip. Flipping on `dirty` alone therefore fires
         ;; twice per commit: once at the NEXT commit's `prepareCommit` to
         ;; publish that trailing delete, and again at its `finishCommit`.
         ;; Measured at 8 store writes per commit against 6.
         ;;
         ;; Gating on content instead lets the trailing delete ride the next
         ;; commit's flip, which publishes the whole map anyway. `dirty` still
         ;; guards `listAll` in the meantime, so the delete is never undone —
         ;; only deferred.
         ;;
         ;; INVARIANT: `synced-since-flip` IMPLIES `dirty`. Every site that sets
         ;; the former sets the latter, and `flip!` clears both under `lock`.
         ;; This is what keeps `listAll` safe during the flip's store writes —
         ;; the only moment the store pointer and the in-memory one disagree —
         ;; because `dirty` is necessarily true there and `listAll` skips. Set
         ;; `synced-since-flip` without `dirty`, or clear `dirty` first in
         ;; `flip!`, and a poll can adopt the store's map and resurrect a name
         ;; this Directory has deleted.
         synced-since-flip (atom false)
         ;; The open unreferenced-write sequence, if any. Blobs land in `sync`
         ;; and the pointer flips in `syncMetaData`, so the window the guard has
         ;; to cover spans two Directory calls and cannot be a scoped macro —
         ;; hence `writing!`/`done!` directly. A process that dies mid-sequence
         ;; drops its entry, which is correct: what it wrote is unreachable.
         guard-token (atom nil)
         ;; Files created through this Directory but not yet synced. Tracked
         ;; explicitly because the local cache is NOT authoritative — it can
         ;; hold debris from an interrupted earlier session, and Lucene must
         ;; never see that. Manifest + session is what this index contains.
         session (atom #{})
         ;; Logical existence, which is NOT the same as a file being present in
         ;; the cache: the manifest names what is durable, the session names
         ;; what this Directory has created since. Every contract check below
         ;; asks this rather than the filesystem.
         has? (fn [name] (or (contains? (files-now) name) (contains? @session name)))
         ;; Every read-modify-write of the manifest holds this. The atom alone
         ;; is not enough: `sync`, `rename` and `deleteFile` each read the
         ;; manifest, derive a new one, put it in the store and only then
         ;; install it — three steps that interleave and lose one of two edits.
         lock (Object.)
         ;; Our own closed flag. `FilterDirectory` delegates `ensureOpen` to the
         ;; live directory, which is right for anything that touches it — but
         ;; `listAll`, `deleteFile`, `syncMetaData` and `fileLength` all reach
         ;; the STORE first and can return, or write, without ever touching
         ;; `live`. So nothing checked, and `deleteFile` on a closed Directory
         ;; silently wrote a new manifest with the file's reference removed,
         ;; leaving `segments_N` naming a blob no manifest reaches. The TCK's
         ;; `testDetectClose` probes only `createOutput`, which does reach
         ;; `live`, which is why 57/57 passed.
         closed? (atom false)
         ensure-open! (fn []
                        (when @closed?
                          (throw (AlreadyClosedException.
                                  (str "scriptum: directory for branch " branch " is closed")))))
         open-guard! (fn []
                       (when (and store-id (nil? @guard-token))
                         (reset! guard-token (guard/writing! store-id))))
         close-guard! (fn []
                        (when-let [t @guard-token]
                          (guard/done! store-id t)
                          (reset! guard-token nil)))
         ;; THE COMMIT POINT. Everything else edits memory; this is the only
         ;; thing that moves the branch, and Lucene calls it exactly where a
         ;; commit becomes durable — `SegmentInfos.finishCommit` renames
         ;; `pending_segments_N` and then calls this, deleting the renamed file
         ;; if it throws. So a failure here un-commits, which is precisely the
         ;; behaviour a pointer flip wants, CAS conflicts included.
         ;;
         ;; `prepareCommit` also calls it, before anything has been synced —
         ;; nothing is dirty then, so that call costs nothing.
         flip! (fn []
                 (locking lock
                   (when @synced-since-flip
                     (let [m (files-now)
                           address (snapshot-address m)]
                       ;; Values then pointer: the snapshot must exist before
                       ;; anything names it. Being content-addressed, rewriting
                       ;; an identical snapshot is harmless.
                       (k/assoc store (snapshot-key address) m {:sync? true})
                       (k/assoc store (manifest-key branch) address {:sync? true})
                       (swap! state assoc :pointer address)
                       (reset! dirty false)
                       (reset! synced-since-flip false))))
                 ;; After the pointer lands, everything this sequence wrote is
                 ;; reachable — or garbage, if a later pointer superseded it.
                 (close-guard!))]
     ;; NOT materialized eagerly. `openInput` and `fileLength` link a file into
     ;; the view on first touch, so fetching the whole manifest here only moved
     ;; that cost to open time and paid it for files no query ever reads. On a
     ;; local filestore that was a hard-link walk; against a remote store it is
     ;; the entire branch downloaded before the first query — the difference
     ;; between fetching a term dictionary and fetching a gigabyte.
     ;;
     ;; Reconcile the view with the manifest before anything reads it. A cached
     ;; file the manifest does not name is debris from a session that died
     ;; mid-write, and nothing else will ever remove it: `listAll` never names
     ;; it, so Lucene's `IndexFileDeleter` never asks for it, and `deleteFile`
     ;; now refuses it. Left alone it pins an inode in the view forever.
     ;;
     ;; `write.lock` is exempt — it is Lucene's, not ours, and deleting one held
     ;; by a live writer in this JVM would break the exclusion it provides.
     (doseq [^String n (.list (io/file cache branch))]
       (when-not (or (contains? (files-now) n) (= n IndexWriter/WRITE_LOCK_NAME))
         (.delete (view-file cache branch n))))
     ;; FilterDirectory over the live MMapDirectory, not a bare Directory: this
     ;; IS an mmap'd directory with a store-backed materialization layer in
     ;; front, and `FilterDirectory.unwrap` is how Lucene detects that. A bare
     ;; Directory denies being mmap-backed while handing out IndexInputs that
     ;; report otherwise — an inconsistency BaseDirectoryTestCase catches
     ;; (testIsLoaded), and which would also hide preload/madvise from Lucene.
     (proxy [FilterDirectory] [live]
       (listAll []
         (ensure-open!)
         ;; Re-read rather than serving the cached manifest: this is what
         ;; DirectoryReader.openIfChanged consults, so a stale manifest leaves a
         ;; long-lived reader permanently blind to new commits. Cheap and right
         ;; for remote stores too — the manifest is a small mutable pointer, so
         ;; a reader polls the pointer and never re-reads immutable segment data.
         ;;
         ;; SKIPPED WHILE DIRTY. This Directory holds edits the store has not
         ;; seen — an unflushed rename or delete — and adopting the store's
         ;; snapshot would silently undo them, resurrecting a deleted name or
         ;; un-renaming `segments_N`. A reader is never dirty, so it still sees
         ;; every commit; a writer mid-commit is the only thing this skips, and
         ;; its own memory is the newest state there is.
         ;;
         ;; The POINTER is what gets polled, not the map: it is a few bytes, and
         ;; when it has not moved there is nothing to re-read at all. The
         ;; snapshot behind it is immutable, so this is also the only read that
         ;; can ever return something new.
         ;;
         ;; `compare-and-set!`, never `reset!`: this runs on READER threads,
         ;; which are unconstrained, and a blind overwrite can install a
         ;; manifest OLDER than one a commit just installed. Claiming the
         ;; pointer first means a racing poll loses the CAS and installs
         ;; nothing, rather than winning with a stale map.
         (when-not @dirty
           (let [before @state
                 a (branch-snapshot store branch)]
             (when (not= a (:pointer before))
               (compare-and-set! state before
                                 {:pointer a
                                  :files (if a (read-snapshot store a) {})}))))
         (let [{:keys [files]} @state]
           (into-array String (sort (into (set (keys files)) @session)))))

       (fileLength [name]
         (ensure-open!)
         (when-let [a (get (files-now) name)] (link-into-view! store cache branch name a))
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
         ;; Where write-once files become durable and shareable — but NOT where
         ;; they become reachable. This writes blobs and edits the in-memory map;
         ;; the pointer moves in `syncMetaData`, which is where Lucene's commit
         ;; actually lands. Measured against the previous shape, where `sync`,
         ;; `rename` and `deleteFile` each wrote the manifest, a commit costs
         ;; 6 store writes rather than 8.
         ;;
         ;; The guard opens here and closes at the flip, because that whole span
         ;; is one values-then-pointer sequence: a collection landing inside it
         ;; would sweep blobs the snapshot is about to name. See konserve.gc-guard.
         ;; MATERIALIZE BEFORE FSYNC. `.sync` opens each name for WRITE, so a
         ;; name the manifest holds but this view has not touched is absent and
         ;; throws NoSuchFileException. IndexWriter.startCommit syncs every file
         ;; of the commit, inherited ones included, and today they happen to be
         ;; materialized by the writer's constructor — an accidental invariant
         ;; Lucene does not promise. `rename` already orders it this way.
         (doseq [^String n names]
           (when-let [a (get (files-now) n)]
             (link-into-view! store cache branch n a)))
         (.sync live names)
         (open-guard!)
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
                                          (files-now) names)]
                            (when (not= m (files-now))
                              (swap! state assoc :files m)
                              (reset! dirty true)
                              (reset! synced-since-flip true))
                          ;; Synced names are the manifest's now. Handing them
                          ;; over keeps a name from living in both sets, so
                          ;; `rename` and `deleteFile` have one place to edit.
                            (swap! session #(reduce disj % names)))))]
           (write!)))

       (syncMetaData []
         (ensure-open!)
         (flip!)
         nil)

       (rename [source dest]
         ;; Materialize first: nothing else guarantees `source` is local now
         ;; that files are fetched on first touch, and `.rename` on the live
         ;; directory needs the file to be there. In practice Lucene renames
         ;; `pending_segments_N`, which this session just wrote — but a manifest
         ;; file renamed without ever being read would otherwise fail.
         (when-let [a (get (files-now) source)]
           (when-not (.exists (view-file cache branch source))
             (link-into-view! store cache branch source a)))
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
           (when (contains? (files-now) source)
             ;; Memory only. The rename Lucene cares about — pending_segments_N
             ;; to segments_N — is immediately followed by `syncMetaData`, which
             ;; publishes it; and a rename that never reaches a flip belongs to a
             ;; commit that never happened.
             (swap! state update :files
                    #(let [a (get % source)]
                       (-> % (dissoc source) (assoc dest a))))
             (reset! dirty true))))

       (deleteFile [name]
         (ensure-open!)
         ;; Deleting what is not there is an error, not a no-op: Lucene relies
         ;; on it to notice that its bookkeeping and the directory disagree.
         (when-not (has? name)
           (throw (NoSuchFileException. name)))
         (when (.exists (view-file cache branch name)) (.deleteFile live name))
         (swap! session disj name)
         ;; Drop the reference only. The blob stays until a GC finds it
         ;; unreachable from EVERY snapshot, so a branch or a reader still
         ;; holding an older one keeps working.
         ;;
         ;; Memory only, published by the next flip. Lucene deletes a superseded
         ;; `segments_N` AFTER the commit that supersedes it, so the drop rides
         ;; along with the following commit — one flip later than before. That
         ;; lag only ever keeps a blob alive longer, which is the safe direction.
         (locking lock
           (when (contains? (files-now) name)
             (swap! state update :files dissoc name)
             (reset! dirty true))))

       (openInput [name context]
         (when-let [a (get (files-now) name)] (link-into-view! store cache branch name a))
         (.openInput live name context))

       ;; Lucene's own lock, in the per-branch view: one writer per branch, and
       ;; writers on different branches do not see each other. Exactly scriptum's
       ;; contract, with no lock of our own.
       (obtainLock [name] (.obtainLock live name))
       (close []
         (reset! closed? true)
         ;; Release the guard WITHOUT flipping. Unflushed edits belong to a
         ;; commit that never completed, so the blobs behind them are garbage —
         ;; and holding the sequence open past close would stall every
         ;; collection on this store id for the life of the process.
         (close-guard!)
         (.close live))
       (getPendingDeletions [] (.getPendingDeletions live))))))

;; =============================================================================
;; Branch operations
;; =============================================================================

(def snapshot-view-dir
  "Where read-only snapshot views live under the cache.

  A sibling of `pool` rather than of the branch views, so `gc-cache!` can tell
  them apart: it deletes a directory whose name is not a live branch, and a
  snapshot view is by definition not one."
  "snapshots")

(defn snapshot-directory
  "A READ-ONLY Directory over the index state at `address`.

  This is what makes a snapshot address worth handing out. A caller who stored
  one — datahike's key-map, a reader pinned to a point in history — can open
  exactly the state it named, on any machine with the store, whatever the branch
  has done since.

  Writes throw. The state is immutable by construction, and a Directory that
  accepted writes would have nowhere to put the result: there is no pointer to
  advance, only an address that already describes its own contents.

  Materialization is lazy and shares the pool with every branch view, so opening
  a snapshot fetches only the files actually read and shares inodes with any
  branch holding the same blobs."
  ^Directory [store ^String cache address]
  (let [files (read-snapshot store address)
        view (str cache "/" snapshot-view-dir "/" address)
        _ (.mkdirs (io/file view))
        live (MMapDirectory/open (->path view))
        materialize! (fn [^String name]
                       (when-let [a (get files name)]
                         (link-into-view! store cache
                                          (str snapshot-view-dir "/" address) name a)))
        read-only (fn [& _]
                    (throw (UnsupportedOperationException.
                            (str "scriptum: snapshot " address " is immutable"))))]
    (proxy [FilterDirectory] [live]
      (listAll [] (into-array String (sort (keys files))))
      (fileLength [name] (materialize! name) (.fileLength live name))
      (openInput [name context] (materialize! name) (.openInput live name context))
      (createOutput [name context] (read-only))
      (createTempOutput [prefix suffix context] (read-only))
      (deleteFile [name] (read-only))
      (rename [source dest] (read-only))
      (obtainLock [name] (read-only))
      (sync [names] nil)
      (syncMetaData [] nil)
      (close [] (.close live)))))

(defn fork!
  "Branch `from` as `to`: copy the manifest. O(1) — no segment bytes move, and
  the two branches share every blob they have in common."
  [store from to]
  (ensure-format! store)
  (check-branch-name to)
  ;; `k/exists?` on the one key, not a scan: existence of a branch IS existence
  ;; of its manifest, so this needs a lookup rather than an enumeration.
  (when (k/exists? store (manifest-key to) {:sync? true})
    (throw (ex-info "scriptum: branch already exists" {:branch to})))
  ;; Copy the POINTER, not the map: both branches now name the same immutable
  ;; snapshot, so a fork writes one small value and the two histories share a
  ;; tree until either commits. Under the previous layout this rewrote the whole
  ;; file map into a second key.
  ;;
  ;; VERIFIED AFTER THE WRITE, because the snapshot being copied is OLD and the
  ;; guard cannot protect it: `writing!` spares objects written inside its
  ;; window, and this one was written by some earlier commit. So a `gc!` that
  ;; runs between reading `from`'s pointer and writing `to`'s — while `from`
  ;; itself moves on — sees the snapshot referenced by nothing and sweeps it,
  ;; and the fork is left pointing at a tree that no longer exists. Re-reading
  ;; afterwards catches exactly that: if the source still names the address and
  ;; the snapshot is still there, the pointer we wrote is a root and no
  ;; subsequent sweep can take it.
  (loop [attempt 0]
    (let [address (branch-snapshot store from)]
      (register-branch! store to)        ; registry first — see register-branch!
      (if-not address
        {}
        (do
          (k/assoc store (manifest-key to) address {:sync? true})
          (if (and (= address (branch-snapshot store from))
                   (k/exists? store (snapshot-key address) {:sync? true}))
            (read-snapshot store address)
            (if (< attempt 3)
              (recur (inc attempt))
              ;; LEAVE NO DANGLING POINTER BEHIND. `mark` resolves every branch
              ;; pointer, and a branch naming a snapshot that is gone makes it
              ;; throw — so one abandoned fork would disable collection for the
              ;; whole store until someone found and deleted the branch. Undo
              ;; before failing: pointer first, then the registry, the same
              ;; order `delete-branch!` uses and for the same reason.
              (do (k/dissoc store (manifest-key to) {:sync? true})
                  (k/update store branches-key #(disj (or % #{}) to) {:sync? true})
                  (throw (ex-info (str "scriptum: could not fork " from " to " to
                                       " — its snapshot was collected mid-fork "
                                       (inc attempt) " times")
                                  {:from from :to to :address address}))))))))))

(defn delete-branch!
  "Forget `branch`. Blobs it referenced survive until `gc!` finds them
  unreachable from every remaining manifest."
  [store branch]
  ;; Destructive on a layout we may not understand — check before dropping a
  ;; branch we cannot read back.
  (ensure-format! store)
  ;; Manifest first, then the registry: the reverse order would leave a moment
  ;; where the manifest exists and no root names it, which is exactly when a
  ;; concurrent `gc!` would sweep the blobs it still names.
  (k/dissoc store (manifest-key branch) {:sync? true})
  (k/update store branches-key #(disj (or % #{}) branch) {:sync? true})
  nil)

(defn reachable-snapshots
  "Snapshot addresses reachable from the branch pointers, plus `extra`.

  `extra` is how an EXTERNAL HOLDER keeps an index state alive. A snapshot
  address is immutable and safe to hand out, so a caller can store one and give
  it back at collection time — which is exactly datahike's `mark-from-key-map`
  contract. Without it, an index embedded in someone else's store is reachable
  only from branch pointers, and a state they still reference but no branch
  names is collected out from under them."
  ([store] (reachable-snapshots store nil))
  ([store extra]
   (into (set extra) (keep #(branch-snapshot store %)) (branches store))))

(defn reachable-addresses
  "Every blob address named by a reachable snapshot — the GC root set."
  ([store] (reachable-addresses store nil))
  ([store extra-snapshots]
   (into #{}
         (mapcat #(vals (read-snapshot store %)))
         (reachable-snapshots store extra-snapshots))))

(defn mark
  "Every store key scriptum needs kept — the mark half of a mark-and-sweep.

  EXPORTED BECAUSE AN EMBEDDER HAS TO CALL IT. When scriptum's blobs live in a
  store it does not own — datahike's, via `sec/mark-from-key-map` — that store's
  collector builds one whitelist from every index and sweeps everything else.
  Leaving this inline in `gc!` meant such a caller had to re-derive the root set
  by hand, and the two roots that are easy to miss are exactly the two already
  missed once here: the branch registry and the format stamp. A swept registry
  makes the next mark find no branches and take the whole index with it.

  So this is the contract, in one place, and `gc!` is a caller of it like any
  other.

  `extra-snapshots` names index states an external holder still references; see
  `reachable-snapshots`. Superseded snapshots are deliberately NOT included —
  collecting them is what stops a long history accumulating one tree per commit."
  ([store] (mark store nil))
  ([store extra-snapshots]
   ;; ONE walk, feeding both halves. Reading the pointers twice — once for the
   ;; snapshot roots and again for the blob roots — let a commit land in between,
   ;; so the snapshot set came from the OLD pointers and the blob set from the
   ;; NEW ones. The live branch was never the victim (the blob walk is second, so
   ;; never the older of the two), but the superseded snapshot stayed whitelisted
   ;; while the blobs it names were swept: a root that is re-protected every
   ;; cycle, never reclaimed, and fails anything that resolves it from a cold
   ;; cache. Reproduced, so this is not a tidiness argument.
   (let [known (branches store)
         snapshots (into (set extra-snapshots) (keep #(branch-snapshot store %)) known)
         files (map #(read-snapshot store %) snapshots)]
     (-> #{branches-key format-key}
         (into (map manifest-key) known)
         (into (map snapshot-key) snapshots)
         (into (comp (mapcat vals) (map blob-key)) files)))))

(defn gc-cache!
  "Delete pooled blobs no branch's manifest names, and views of branches that
  are gone. Returns `{:blobs n :views n}`.

  THE STORE COLLECTOR DOES NOT TOUCH THE CACHE, and the cache is where the
  bytes actually sit on a machine. Measured on a merge-heavy workload: `gc!`
  reclaimed 82% of the store and 0% of the pool, which held 73 blobs against 14
  live addresses. On a long-running container that grows without bound; on AWS
  Lambda, whose `/tmp` is 512 MB, it is a hard failure with a store a fraction
  of the size.

  Safe by construction, in a way the store collector is not: the pool is a
  DERIVED cache, so anything deleted here can be fetched again. The worst case
  is a re-download, never a dangling reference — which is why this needs no
  guard, no cutoff and no in-flight-write protection.

  Nor does it disturb a running reader. Unlinking a mapped file is safe on
  POSIX — the inode outlives the directory entry for as long as anything maps
  it — and a live branch view holds a hard link to the same inode regardless.

  A DEAD BRANCH'S VIEW KEEPS ITS `write.lock`, exempted exactly as the open-time
  reconcile exempts it. The lock is Lucene's, not ours, and deleting one out
  from under a live writer breaks the exclusion it provides: a writer whose
  branch has been dropped from the registry — or deleted while it was open —
  otherwise fails its next commit with NoSuchFileException on the lock itself.
  Leaving one file behind is the cheaper mistake.

  `extra-snapshots` must name the same held states passed to `gc!`. Without
  them a snapshot view is reclaimed and its blobs re-downloaded on every call,
  which is a cost bug rather than a correctness one — but a pointless one.

  Same root set as `gc!`: reachability from the live manifests. Anything in the
  pool whose name is not a live address is garbage, including the `.tmp` debris
  of an interrupted materialization."
  ([store ^String cache] (gc-cache! store cache nil))
  ([store ^String cache extra-snapshots]
   ;; Deletes local files from a root set it computes by reading the store. On a
   ;; layout it cannot read that set comes back empty and this wipes the entire
   ;; pool and every view — recoverable, since the cache is derived, but a full
   ;; re-download rather than a no-op.
   (ensure-format! store)
   (let [live (into #{} (map str) (reachable-addresses store extra-snapshots))
         live-snapshots (into #{} (map str) (reachable-snapshots store extra-snapshots))
         known (branches store)
         rm-dir! (fn [^java.io.File d]
                   (run! #(.delete ^java.io.File %)
                         (remove #(= IndexWriter/WRITE_LOCK_NAME (.getName ^java.io.File %))
                                 (.listFiles d)))
                   (.delete d))          ; fails while write.lock remains, by design
         pool-dir (io/file cache "pool")
         blobs (if (.isDirectory pool-dir)
                 (count (filterv (fn [^java.io.File f]
                                   (and (not (contains? live (.getName f)))
                                        (.delete f)))
                                 (.listFiles pool-dir)))
                 0)
         ;; SNAPSHOT VIEWS ARE RECLAIMED BY ADDRESS, not exempted wholesale.
         ;; Exempting the directory left it growing once per address ever
         ;; opened, and — because its entries are hard links into the pool — a
         ;; pool blob deleted above kept its inode alive underneath, so this
         ;; reported bytes it had not freed. On the 512 MB Lambda `/tmp` this
         ;; docstring is written for, that is the exact failure it claims to
         ;; prevent.
         snap-dir (io/file cache snapshot-view-dir)
         snap-views (if (.isDirectory snap-dir)
                      (count (filterv (fn [^java.io.File d]
                                        (and (.isDirectory d)
                                             (not (contains? live-snapshots (.getName d)))
                                             (rm-dir! d)))
                                      (.listFiles snap-dir)))
                      0)
         views (count (filterv (fn [^java.io.File d]
                                 (and (.isDirectory d)
                                      (not= "pool" (.getName d))
                                      (not= snapshot-view-dir (.getName d))
                                      (not (contains? known (.getName d)))
                                      (rm-dir! d)))
                               (or (.listFiles (io/file cache)) [])))]
     {:blobs blobs :views views :snapshot-views snap-views})))

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

  `extra-snapshots` protects index states an EXTERNAL holder still references —
  see `reachable-snapshots`. Anything embedding scriptum in a store it does not
  own must pass them, or a state no branch names is collected out from under it.

  NOTE for shared stores: this collects blobs no reachable snapshot names. A
  reader on another machine pinned to an older snapshot can still be holding
  one — and now has an address it can pass as `extra-snapshots` to say so."
  ([store store-id] (gc! store store-id (ku/now) nil))
  ([store store-id ts] (gc! store store-id ts nil))
  ([store store-id ts extra-snapshots]
   ;; REFUSE A LAYOUT WE CANNOT READ BEFORE DELETING ANYTHING. `gc!` is reachable
   ;; without ever opening a Directory, so it cannot rely on the check there, and
   ;; it is the one operation where misreading a manifest destroys data rather
   ;; than merely failing: `reachable-addresses` takes `vals` of every manifest
   ;; and assumes each is a whole-blob address. Under a layout whose entries are
   ;; not addresses — the batched `[address offset length]` form this version
   ;; marker exists to make possible — every val misses every blob key, and
   ;; `sweep!` being allow-list then deletes THE ENTIRE STORE.
   (ensure-format! store)
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
   (let [cutoff (if store-id (guard/cutoff store-id ts) ts)]
     ;; ONE root set, shared with any embedding collector — see `mark`. Inlining
     ;; it here is what let the registry and the format stamp each be forgotten
     ;; once; `sweep!` is allow-list, so a root omitted is a key deleted.
     (kgc/sweep! store (mark store extra-snapshots) cutoff 1000 {:sync? true}))))
