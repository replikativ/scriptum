(ns scriptum.core
  "COW branching semantics on top of Apache Lucene.

  Provides fast forking (~3-5ms), structural sharing of immutable segments,
  branch-isolated indexing/searching, snapshot retention, and explicit GC.

  Key concepts:
  - Writer: mutable handle to a branch (one per branch per JVM)
  - Snapshot: immutable DirectoryReader at a specific commit point
  - Branch: COW overlay sharing base segments with the trunk
  - GC: explicit cleanup of old snapshots respecting branch references"
  (:require [scriptum.konserve :as sk]
            [scriptum.metadata :as metadata]
            [konserve.gc-guard :as guard])
  (:import [java.io File]
           [java.lang.ref Cleaner Cleaner$Cleanable]
           [java.nio.file Path Paths]
           [java.time Instant Duration]
           [java.util IdentityHashMap]
           [org.apache.lucene.analysis Analyzer]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.document Document Field$Store TextField StringField
            IntField LongField FloatField DoubleField StoredField
            KnnFloatVectorField]
           [org.apache.lucene.index DirectoryReader IndexableField Term
            VectorSimilarityFunction]
           [org.apache.lucene.search IndexSearcher TermQuery TermInSetQuery PrefixQuery ConstantScoreQuery BooleanQuery
            BooleanQuery$Builder BooleanClause$Occur TopDocs ScoreDoc
            FieldDoc Sort MatchAllDocsQuery KnnFloatVectorQuery]
           [org.apache.lucene.queryparser.classic QueryParser MultiFieldQueryParser]
           [org.apache.lucene.store Directory FSDirectory]
           [org.apache.lucene.util BytesRef]
           [org.replikativ.scriptum BranchIndexWriter BranchedDirectory]))

(defn- ->path
  "Convert a string to a java.nio.file.Path."
  ^Path [^String s]
  (Paths/get s (make-array String 0)))

;; --- ScriptumWriter wrapper ---

(defrecord ScriptumWriter [writer metadata-index backing]
  ;; `backing` is nil for a directory-backed index and
  ;; `{:store s :cache c :directory d}` for a store-backed one. Everything a
  ;; writer does to DOCUMENTS is pure Lucene and identical either way; only
  ;; branch TOPOLOGY — fork, branch discovery, collection — differs, because a
  ;; branch is a directory in one model and a manifest in the other.
  )

(defonce ^:private ^Cleaner snapshot-cleaner (Cleaner/create))
(defonce ^:private snapshot-resource-lock (Object.))
(defonce ^:private ^IdentityHashMap snapshot-resources (IdentityHashMap.))

(defrecord StoreSnapshotResource [store resource-key directory reader refs])

(defn- release-snapshot-resource! [^StoreSnapshotResource resource]
  (locking snapshot-resource-lock
    (let [store (:store resource)
          resource-key (:resource-key resource)
          resources (.get snapshot-resources store)
          registered (get resources resource-key)]
      (when-not (identical? resource registered)
        (throw (ex-info "scriptum: snapshot lease resource is no longer registered"
                        {:snapshot-resource resource-key})))
      (if (> (long @(:refs resource)) 1)
        (swap! (:refs resource) dec)
        (try
          ;; A DirectoryReader retains the Directory, so release it first.
          (try
            (.close ^DirectoryReader (:reader resource))
            (finally
              (.close ^Directory (:directory resource))))
          (finally
            (let [remaining (dissoc resources resource-key)]
              (if (seq remaining)
                (.put snapshot-resources store remaining)
                (.remove snapshot-resources store)))))))))

(defrecord SnapshotLeaseCleanup [resource closed?]
  Runnable
  (run [_]
    (when (compare-and-set! closed? false true)
      (try
        (release-snapshot-resource! resource)
        ;; Cleaner fallback cannot report to a caller. Explicit close invokes
        ;; the same action synchronously and therefore still surfaces failures.
        (catch Throwable _)))))

(defrecord StoreSnapshot [snapshot-address directory reader resource cleanable*
                          closed?]
  java.io.Closeable
  (close [_]
    (locking closed?
      (when-not @closed?
        ;; Explicit close is synchronous and reports native/Lucene failures.
        (try
          (release-snapshot-resource! resource)
          (finally
            ;; Native close is not a safely retryable protocol after partial
            ;; progress. Mark this logical lease consumed either way.
            (reset! closed? true)
            (.clean ^Cleaner$Cleanable @cleanable*)))))))

(defn- snapshot-resource-key [^String cache address]
  [(.getCanonicalPath (File. cache)) address])

(defn- acquire-snapshot-resource! [store ^String cache address]
  (let [resource-key (snapshot-resource-key cache address)]
    (locking snapshot-resource-lock
      (let [resources (or (.get snapshot-resources store) {})]
        (if-let [resource (get resources resource-key)]
          (do
            (swap! (:refs resource) inc)
            resource)
          (let [^Directory directory (sk/snapshot-directory store cache address)]
            (try
              (let [resource (->StoreSnapshotResource
                              store resource-key directory
                              (DirectoryReader/open directory) (atom 1))]
                (.put snapshot-resources store (assoc resources resource-key resource))
                resource)
              (catch Throwable failure
                (.close directory)
                (throw failure)))))))))

(defn- snapshot-lease [^StoreSnapshotResource resource address]
  (let [cleanable* (atom nil)
        closed? (atom false)
        snapshot (->StoreSnapshot address (:directory resource) (:reader resource)
                                  resource cleanable* closed?)
        cleanup (->SnapshotLeaseCleanup resource closed?)]
    (try
      (reset! cleanable* (.register snapshot-cleaner snapshot cleanup))
      snapshot
      (catch Throwable failure
        (release-snapshot-resource! resource)
        (throw failure)))))

(defn generation?
  "Whether `sw` is a detached immutable-generation writer."
  [sw]
  (boolean (and (instance? ScriptumWriter sw)
                (= :generation (get-in sw [:backing :mode])))))

(defn- generation-backing
  [sw]
  (when-not (generation? sw)
    (throw (ex-info "scriptum: operation requires a detached generation"
                    {:value-type (type sw)})))
  (:backing sw))

(defrecord GenerationPublicationHold
           [store-id guard-token generation-address completed? outcome])

(defn- release-generation-guard!
  [sw]
  (let [{:keys [store-id guard-token lifecycle]} (generation-backing sw)]
    (locking guard-token
      (when-let [token @guard-token]
        (guard/done! store-id token)
        (reset! guard-token nil))
      (swap! lifecycle assoc :released? true))))

(defn- cleanup-generation-workspace!
  [sw]
  (let [{:keys [cache workspace workspace-cleaned?]} (generation-backing sw)]
    (when-not @workspace-cleaned?
      (when (sk/cleanup-detached-workspace! cache workspace)
        (reset! workspace-cleaned? true)))
    @workspace-cleaned?))

(defn store-backed?
  "Is this writer backed by a konserve store rather than a directory tree?"
  [sw]
  (boolean (and (instance? ScriptumWriter sw) (:backing sw))))

(defn ->writer
  "Extract the BranchIndexWriter from a ScriptumWriter or pass through a raw writer."
  ^BranchIndexWriter [sw-or-writer]
  (if (instance? ScriptumWriter sw-or-writer)
    (:writer sw-or-writer)
    sw-or-writer))

(defn- ->metadata-index
  "Extract the MetadataIndex from a ScriptumWriter, or nil."
  [sw-or-writer]
  (when (instance? ScriptumWriter sw-or-writer)
    (:metadata-index sw-or-writer)))

;; --- Index Lifecycle ---

(defn- tune!
  "Apply the segment-size knobs to a freshly opened writer.

  Both are live settings on Lucene's side — the merge policy re-reads its cap on
  every merge decision and the flush buffer applies to the next flush — so they
  are set after construction rather than threaded through the Java
  constructors."
  [^BranchIndexWriter writer max-merged-segment-mb ram-buffer-mb]
  (when max-merged-segment-mb (.setMaxMergedSegmentMB writer (double max-merged-segment-mb)))
  (when ram-buffer-mb (.setRAMBufferSizeMB writer (double ram-buffer-mb)))
  writer)

(defn create-index
  "Create a new branched index at the given path.

  On creation, discovers existing branches and protects their shared segments.

  Options:
    :analyzer - the Lucene Analyzer to use (default: StandardAnalyzer)
    :crypto-hash? - enable merkle hashing for commits (default: false)
    :max-merged-segment-mb - cap on a merged segment, in MB (Lucene default: 5120)
    :ram-buffer-mb - flush buffer, in MB (Lucene default: 16)

  THE TWO SIZE KNOBS ARE THE ONES THAT MATTER FOR A REMOTE STORE. Lucene's
  defaults are tuned for a local disk, where a segment is just a file and 5 GB
  costs nothing to leave lying there. Against an object store a segment is a
  blob written and read whole, so the merged-segment cap sets the peak memory a
  commit costs — konserve's S3 backing holds a blob in the heap to PUT it — and
  it has to stay clear of S3's 5 GB single-PUT limit. A few hundred MB is a
  reasonable cap there; `scriptum.konserve/remote-tuning` carries defaults.

  The flush buffer sets the other end of the distribution: it bounds segments
  created by a flush, before any merge, and so governs how small the small
  objects are.

  Returns a ScriptumWriter wrapping BranchIndexWriter + metadata index."
  ([^String path ^String branch-name]
   (create-index path branch-name {}))
  ([^String path ^String branch-name {:keys [analyzer crypto-hash?
                                             max-merged-segment-mb ram-buffer-mb]}]
   (let [base-path (->path path)
         analyzer (or analyzer (StandardAnalyzer.))
         crypto-hash (boolean crypto-hash?)
         writer (BranchIndexWriter/create base-path branch-name analyzer crypto-hash)
         mi (metadata/create-metadata-index path)]
     (tune! writer max-merged-segment-mb ram-buffer-mb)
     (->ScriptumWriter writer mi nil))))

(defn open-branch
  "Open an existing branch writer (for out-of-process branch access).

  Opens a BranchedDirectory with the base as read-only and overlay for writes.

  Options:
    :analyzer - the Lucene Analyzer to use (default: StandardAnalyzer)
    :metadata-index - shared metadata index (default: creates new one)
    :max-merged-segment-mb - cap on a merged segment, in MB (Lucene default: 5120)
    :ram-buffer-mb - flush buffer, in MB (Lucene default: 16)

  THE TWO SIZE KNOBS ARE THE ONES THAT MATTER FOR A REMOTE STORE. Lucene's
  defaults are tuned for a local disk, where a segment is just a file and 5 GB
  costs nothing to leave lying there. Against an object store a segment is a
  blob written and read whole, so the merged-segment cap sets the peak memory a
  commit costs — konserve's S3 backing holds a blob in the heap to PUT it — and
  it has to stay clear of S3's 5 GB single-PUT limit. A few hundred MB is a
  reasonable cap there; `scriptum.konserve/remote-tuning` carries defaults.

  The flush buffer sets the other end of the distribution: it bounds segments
  created by a flush, before any merge, and so governs how small the small
  objects are."
  ([^String path ^String branch-name]
   (open-branch path branch-name {}))
  ([^String path ^String branch-name {:keys [analyzer metadata-index
                                             max-merged-segment-mb ram-buffer-mb]}]
   (let [base-path (->path path)
         analyzer (or analyzer (StandardAnalyzer.))
         writer (BranchIndexWriter/open base-path branch-name analyzer)
         mi (or metadata-index (metadata/create-metadata-index path))]
     (tune! writer max-merged-segment-mb ram-buffer-mb)
     (->ScriptumWriter writer mi nil))))

(declare open-store-index)

(defn fork
  "Fork the index into a new branch. Returns the new branch writer.

  The new branch shares all existing segments with the parent.
  Cost: ~3-5ms (flush buffer + copy manifest)."
  [sw ^String new-branch-name]
  (if (store-backed? sw)
    ;; Forking a store-backed index copies a manifest — no bytes move and no
    ;; directory is created. The parent must land its buffered writes first, or
    ;; the copy names a manifest that does not yet describe them.
    (let [{:keys [store cache store-id analyzer
                  max-merged-segment-mb ram-buffer-mb]} (:backing sw)]
      ;; CHECK BEFORE COMMITTING. This committed the parent first, so a fork onto
      ;; a name that already exists left a commit point on the source and then
      ;; threw — the same defect the Java `fork` was rewritten to remove, still
      ;; here on the store path.
      (when (sk/branch-exists? store new-branch-name)
        (throw (ex-info "scriptum: branch already exists" {:branch new-branch-name})))
      (.commit (->writer sw))
      (sk/fork! store (.getBranchName (->writer sw)) new-branch-name)
      ;; CARRY THE PARENT'S GUARD ID. Dropping it left a caller who passed
      ;; `:store-id` explicitly with a parent guarded under their id and a fork
      ;; guarded under the derived one — two ids for one store, which is the
      ;; case `konserve.gc-guard` names as deleting live data, and which
      ;; measured as thousands of lost blobs and a branch that would not open.
      ;; CARRY THE PARENT'S SETTINGS, not just its guard id. A fork builds a
      ;; fresh IndexWriterConfig, so anything not passed reverts to Lucene's
      ;; defaults — including the 5 GB merged-segment cap the remote-store
      ;; guidance exists to keep clear of S3's single-PUT limit. A parent tuned
      ;; to 256 MB silently produced a fork at 5120.
      (open-store-index store cache new-branch-name
                        {:metadata-index (->metadata-index sw)
                         :store-id store-id
                         :analyzer analyzer
                         :max-merged-segment-mb max-merged-segment-mb
                         :ram-buffer-mb ram-buffer-mb}))
    (let [w (->writer sw)
          mi (->metadata-index sw)
          new-writer (.fork w new-branch-name)]
      (->ScriptumWriter new-writer mi nil))))

(defn open-store-index
  "Open `branch` of a konserve-backed index, materializing through `cache`.

  The store is the source of truth; `cache` is a derived local directory that
  may be deleted at any time — see `scriptum.konserve`. Lucene still mmaps
  local files, so a cache is required even when the store is remote; what the
  store buys is that it is the only thing that must be durable.

  Takes a CONNECTED store. A secondary index that must reconnect from a
  serialized key-map (datahike's `-sec-restore`) connects it itself and passes
  the store in — that belongs in the adapter, which owns the config, not here.

  Options:
    :analyzer - the Lucene Analyzer (default: StandardAnalyzer)
    :metadata-index - shared metadata index (default: none)
    :store-id - id for konserve.gc-guard, so a collection cannot sweep this
                index's in-flight segment writes. Defaults to the store's own
                id, which is what keeps two components on one store from
                disagreeing about its name.
    :max-merged-segment-mb / :ram-buffer-mb - see `create-index`. Against a
                remote store start from `scriptum.konserve/remote-tuning`.

  Returns a ScriptumWriter. Document operations, search, commit and readers
  behave exactly as for a directory-backed index, and `fork` and `branches`
  answer from the manifests instead of the filesystem. COLLECTION IS DIFFERENT:
  `scriptum.core/gc!` throws here and `scriptum.konserve/gc!` is the one to
  call, because a store-backed index collects by reachability and has to read
  the gc-guard's cutoff before walking, which the directory-backed collector
  does not do."
  ([store cache branch] (open-store-index store cache branch {}))
  ([store ^String cache ^String branch
    {:keys [analyzer metadata-index store-id max-merged-segment-mb ram-buffer-mb]}]
   (let [analyzer (or analyzer (StandardAnalyzer.))
         ;; Default the guard id off the store rather than making the caller
         ;; track it, so two components on one store cannot disagree about its
         ;; name — see konserve.gc-guard on why only that direction is unsafe.
         ;;
         ;; A nil id silently disables the guard — a collection then sweeps an
         ;; in-flight commit's blobs and bricks the branch — so connect the store
         ;; with `konserve.store/connect-store`, which requires a UUID `:id` and
         ;; attaches it. `konserve.filestore/connect-fs-store` carries no config
         ;; and answers nil; see `scriptum.konserve/store-id-for`.
         store-id (or store-id (sk/store-id-for store))
         ;; Where `merge-from!` records the lineage it brought in, so the next
         ;; commit can name it as a parent. It has to be created out here
         ;; because the Directory is a proxy with no way to reach into it.
         pending-parents (atom #{})
         dir (sk/konserve-directory store cache branch store-id pending-parents)
         ;; CLOSE THE DIRECTORY IF THE WRITER CANNOT BE BUILT. `createOver` takes
         ;; ownership only once it returns; if it throws — a second writer on the
         ;; branch gives LockObtainFailedException — nobody owned this Directory
         ;; and its mmap arena, and for a store-backed one the gc-guard sequence
         ;; it may have opened, were left to the garbage collector.
         writer (try (BranchIndexWriter/createOver dir branch analyzer)
                     (catch Throwable t
                       (try (.close ^java.io.Closeable dir) (catch Throwable _))
                       (throw t)))]
     (tune! writer max-merged-segment-mb ram-buffer-mb)
     (->ScriptumWriter writer metadata-index
                       {:store store :cache cache :directory dir :store-id store-id
                        :pending-parents pending-parents
                        ;; Kept so `fork` can reproduce them. Lucene's config is
                        ;; per-writer and a fork builds a fresh one, so anything
                        ;; not carried here silently reverts to Lucene's defaults.
                        :analyzer analyzer
                        :max-merged-segment-mb max-merged-segment-mb
                        :ram-buffer-mb ram-buffer-mb}))))

(defn begin-generation
  "Begin a private, writable generation derived from `base-address`.

  Unlike `open-store-index`, this creates no branch, branch-registry entry or
  mutable Scriptum ref. A successful `seal-generation!` writes immutable blobs
  and a snapshot and returns its content address. The caller publishes that
  address through its own root and then calls `release-generation!`.

  A GC guard is held for the complete lifecycle, including the interval after
  sealing and before external publication. `base-address` itself must already
  be rooted while this call begins; for an embedded secondary that is normally
  the currently published database root.

  Options are the tuning options of `open-store-index`, plus:
    :workspace-id - unique local cache/lock identity (default random UUID)
    :logical-name - commit metadata name (default \"detached\")
    :store-id     - shared konserve GC identity (defaults from store)."
  ([store cache base-address]
   (begin-generation store cache base-address {}))
  ([store ^String cache base-address
    {:keys [analyzer store-id workspace-id logical-name
            max-merged-segment-mb ram-buffer-mb]
     :as opts}]
   (let [live-store-id (sk/store-id-for store)
         _ (when (and store-id live-store-id (not= store-id live-store-id))
             (throw (ex-info
                     "scriptum: detached generation store id differs from its live Konserve identity"
                     {:type :scriptum/generation-store-id-mismatch
                      :configured store-id
                      :actual live-store-id})))
         store-id (or live-store-id store-id)
         _ (when-not store-id
             (throw (ex-info
                     "scriptum: a detached generation requires a konserve store id"
                     {:store-type (type store)})))
         analyzer (or analyzer (StandardAnalyzer.))
         workspace (str (or workspace-id (random-uuid)))
         token (guard/writing! store-id)
         guard-token (atom token)
         lifecycle (atom {:status :open :released? false})
         publication-hold-transferred? (atom false)
         workspace-cleaned? (atom false)]
     (try
       (let [{:keys [directory state pending-parents]}
             (sk/detached-directory store cache workspace base-address
                                    (cond-> {:store-id store-id}
                                      (:pending-parents opts)
                                      (assoc :pending-parents (:pending-parents opts))))
             writer (try (BranchIndexWriter/createDetachedOver
                          directory (str (or logical-name "detached")) analyzer)
                         (catch Throwable failure
                           (try (.close ^java.io.Closeable directory) (catch Throwable _))
                           (throw failure)))
             _ (try
                 (tune! writer max-merged-segment-mb ram-buffer-mb)
                 (catch Throwable failure
                   (try (.rollback ^BranchIndexWriter writer) (catch Throwable _))
                   (throw failure)))]
         (->ScriptumWriter
          writer nil
          {:mode :generation
           :store store
           :cache cache
           :workspace workspace
           :directory directory
           :state state
           :store-id store-id
           :pending-parents pending-parents
           :guard-token guard-token
           :publication-hold-transferred? publication-hold-transferred?
           :lifecycle lifecycle
           :workspace-cleaned? workspace-cleaned?
           :analyzer analyzer
           :max-merged-segment-mb max-merged-segment-mb
           :ram-buffer-mb ram-buffer-mb}))
       (catch Throwable failure
         (try
           (sk/cleanup-detached-workspace! cache workspace)
           (finally
             (when-let [held @guard-token]
               (guard/done! store-id held)
               (reset! guard-token nil))))
         (throw failure))))))

(defn snapshot-address
  "The immutable address of this branch's current index state, or nil.

  THE VALUE A CALLER HOLDS TO COME BACK TO THIS EXACT STATE. A branch name is a
  mutable cell and says nothing about which commit it is on; this is content-
  addressed and cannot change under the holder. It is what a secondary-index
  key-map should carry — datahike's already carries `:commit-id` for proximum
  and `:dataset-commit-id` for stratum, and scriptum was the outlier naming a
  branch.

  It is also a merkle root over the whole history — `ContentHash/hashMap` over
  the file map AND the parents, whose values are themselves content hashes of
  segments — so it doubles as a content hash without `:crypto-hash?` being on.

  Reflects the last COMMIT, since the branch pointer moves at commit time —
  buffered writes are not in it. Store-backed indices only; nil otherwise."
  [sw]
  (when (store-backed? sw)
    (let [{:keys [mode state store]} (:backing sw)]
      (if (= :generation mode)
        (:pointer @state)
        (sk/branch-snapshot store (.getBranchName (->writer sw)))))))

(defn mark-generation
  "The exact konserve keys required to open immutable generation `address`.

  Intended for an embedding collector that unions marks from all of its roots."
  [store address]
  (sk/mark-snapshot store address))

(defn verify-generation
  "Recompute an immutable generation's snapshot and blob addresses.

  This is the branch-free audit entry point for embedded indices. See
  `scriptum.konserve/verify-snapshot` for the structured result."
  [store address]
  (sk/verify-snapshot store address))

(defn open-store-index-at
  "Open `branch` as a WRITABLE index at the state named by `address`.

  Points the branch at `address` and opens it — the restore half of
  `snapshot-address`. Without it a holder could read a snapshot
  (`scriptum.konserve/snapshot-directory`) but never write from one, so
  restoring a secondary index to a specific state was impossible and opening the
  branch silently gave whatever it had moved on to instead.

  THIS MOVES THE BRANCH. Whatever it named before becomes unreachable and
  collectable; hold that address yourself if you still want it. Do not call it
  on a branch another writer has open — see
  `scriptum.konserve/point-branch-at!`.

  Takes the same options as `open-store-index`."
  ([store cache branch address] (open-store-index-at store cache branch address {}))
  ([store ^String cache ^String branch address opts]
   (sk/point-branch-at! store branch address)
   (open-store-index store cache branch opts)))

(defn retain!
  "Drop old commit points from a store-backed index, bounding its growth.

  THE THING THAT MAKES A STORE-BACKED INDEX FINITE. Nothing else prunes it:
  every commit point is kept, so the branch's file map is cumulative — 30
  commits of 30 documents were measured naming 130 files, 30 of them commit
  points — and all of it is legitimately reachable, so `scriptum.konserve/gc!`
  correctly reclaims nothing. Dropping a commit point removes its files from the
  manifest, and the collector can then take the blobs no other branch names.

  Two ways to say what goes, because two callers ask different questions:

    :before     — an Instant; drop commit points committed before it. This is
                  yggdrasil's `:remove-before`, and the timestamp compared is
                  the real commit time from user-data.
    :commit-ids — drop exactly these `snapshot-id`s. yggdrasil's coordinator
                  computes reachability itself, from every system's `gc-roots`
                  and the commit graph, and hands each adapter its own
                  candidates; a cutoff cannot express that, since an unreachable
                  commit may be newer than a reachable one elsewhere.

  Issues a commit WHEN IT WILL ACTUALLY DROP SOMETHING, because `onCommit` is
  the only place Lucene lets a deletion policy act — and because committing is
  not free here, since the commit itself becomes a commit point. A sweep that
  matches nothing returns 0 without touching the index; doing otherwise made
  the collector grow the index on every cycle under yggdrasil, whose candidates
  come from a registry that never names scriptum's own bookkeeping commits.

  THE BRANCH HEAD IS NEVER DROPPED, whatever the cutoff says, so a caller who
  passes `:before (now)` does not lose the commit `gc-roots` just reported.

  THE SHRINK IS PUBLISHED AT THE NEXT COMMIT, not this one: Lucene removes a
  dropped commit point's files during the checkpoint that follows the flip, so
  the manifest for the retain commit is already written by then. `retain!`
  reports what it dropped; the manifest reflects it once you commit again.

  IT ALSO COMMITS WHATEVER IS BUFFERED, because `IndexWriter.commit` cannot do
  otherwise. Collection should not be what makes a caller's in-flight writes
  durable — commit first if that distinction matters to you.

  READING A DROPPED COMMIT BY GENERATION STOPS WORKING — that is the trade. Its
  state is still reachable by snapshot address, which is what
  `scriptum.konserve/snapshot-directory` opens and what yggdrasil's `as-of`
  maps onto.

  HOLDING AN ADDRESS PINS NOTHING. `snapshot-address` hands you a value; it
  registers no claim on it, and once no branch names that state `gc!` collects
  it. To keep one you must pass it as `extra-snapshots` on EVERY collection —
  to `scriptum.konserve/gc!` for the store and to `gc-cache!` for the local
  cache, which take it separately.

  Returns the number of commit points dropped, or nil for a directory-backed
  index, which must use `gc!` — there a commit point holds real files another
  branch may share."
  [sw {:keys [before commit-ids]}]
  (when (store-backed? sw)
    (.retain (->writer sw) before (when commit-ids (set (map str commit-ids))))))

(defn warm!
  "Materialize this branch's segments into the local cache, in parallel.

  FOR A COLD MACHINE — a fresh container, a thawed Lambda, a cache that was
  wiped. The store has everything and this machine has nothing, and Lucene will
  otherwise fetch one file per round trip in sequence, because
  `StandardDirectoryReader` opens segment readers serially. Measured on a
  35-segment index at 60 ms latency: 2.2 s lazily against 275 ms warmed.

  Explicit rather than automatic: materialization is lazy by design, since a
  selective query should not pay for segments it never reads. Warming is worth
  it when you know the machine is cold and about to serve.

  Options: `:only` (a predicate on the Lucene filename) and `:budget` (max
  files materialized, a hard ceiling in manifest order). Returns the
  ecosystem's warm-report envelope — {:fetched :ms :budget-left
  :budget-exhausted?}, `:fetched` in FILES — so warming scriptum next to
  datahike's and stratum's trees reads one shape. Store-backed indices only;
  nil otherwise."
  ([sw] (warm! sw {}))
  ([sw opts]
   (when (store-backed? sw)
     (let [{:keys [store cache]} (:backing sw)]
       (sk/warm! store cache (.getBranchName (->writer sw)) opts)))))

(defn branches
  "Every branch of a store-backed index, from its manifests."
  [sw]
  (if (store-backed? sw)
    (sk/branches (:store (:backing sw)))
    (throw (ex-info "scriptum: branches is for store-backed indices; use discover-branches for a path"
                    {:writer sw}))))

(defn discover-branches
  "Discover all branch names at the given path.

  Returns a set of branch name strings."
  [^String path]
  (BranchIndexWriter/discoverBranches (->path path)))

;; --- Document Operations ---

(defn add-document
  "Add a pre-built Lucene document or other iterable of IndexableField values.

   This is the allocation-conscious integration path for callers with a fixed
   schema. `add-doc` remains the convenient data-driven map API. Keeping this
   operation here avoids exposing BranchIndexWriter as part of an adapter's
   mutation protocol."
  [sw doc]
  (.addDocument ^BranchIndexWriter (->writer sw) doc))

(defn add-doc
  "Add a document to the branch.

  doc-map is a map of field-name -> value.
  Options per field: {:value v :type ... :store? bool}

  Types:
    :text        - Analyzed full-text (TextField) - default
    :string      - Exact match (StringField)
    :int         - Integer with range queries + sorting (IntField)
    :long        - Long with range queries + sorting (LongField)
    :float       - Float with range queries + sorting (FloatField)
    :double      - Double with range queries + sorting (DoubleField)
    :stored-only - Store but don't index (StoredField)
    :vector      - KNN float vector search (KnnFloatVectorField)

  Auto-detection:
    - java.time.Instant → :long (epoch millis)
    - java.util.Date → :long (epoch millis)
    - Vector of values → multi-valued field

  Simple usage:
    (add-doc writer {:subject \"Meeting notes\"
                     :from \"alice@example.com\"
                     :date (Instant/now)})

  Advanced usage:
    (add-doc writer {:subject {:value \"Meeting\" :type :text :store? true}
                     :from {:value \"alice@example.com\" :type :string}
                     :to {:value [\"bob@example.com\" \"charlie@example.com\"] :type :string}
                     :date {:value (Instant/now) :type :long :store? true}
                     :size {:value 42000 :type :int :store? false}
                     :headers {:value \"{...}\" :type :stored-only}
                     :embedding {:value (float-array [...]) :type :vector
                                 :similarity :cosine}})

  For fine-grained control, use Lucene classes directly:
    (let [doc (Document.)]
      (.add doc (TextField. \"body\" text Field$Store/NO))
      (.add doc (StoredField. \"body\" text))
      (.addDocument writer doc))"
  [sw doc-map]
  (let [^BranchIndexWriter writer (->writer sw)
        doc (Document.)]
    (doseq [[field-name value-or-opts] doc-map]
      (let [fname (name field-name)
            opts (if (map? value-or-opts) value-or-opts {:value value-or-opts})
            {:keys [value stored? store? type similarity]} opts
            store? (cond
                     (contains? opts :stored?) stored?
                     (contains? opts :store?) store?
                     :else true)
            store (if store? Field$Store/YES Field$Store/NO)

            ;; Auto-detect type from value (only if type not explicitly provided)
            [detected-type value']
            (if (contains? opts :type)
              [type value]
              (cond
                (instance? java.time.Instant value) [:long (.toEpochMilli ^Instant value)]
                (instance? java.util.Date value) [:long (.getTime ^java.util.Date value)]
                (and (class value) (= (.getName (class value)) "[F")) [:vector value]
                :else [:text value]))

            final-type detected-type

            ;; Handle multi-valued fields (vector of values)
            values (if (and (vector? value') (not= final-type :vector))
                     value'
                     [value'])]

        (doseq [v values]
          (case final-type
            :text
            (.add doc (TextField. fname (str v) store))

            :string
            (.add doc (StringField. fname (str v) store))

            :int
            (.add doc (IntField. fname (int v) store))

            :long
            (.add doc (LongField. fname (long v) store))

            :float
            (.add doc (FloatField. fname (float v) store))

            :double
            (.add doc (DoubleField. fname (double v) store))

            :stored-only
            (.add doc (StoredField. fname
                                    (cond
                                      (string? v) v
                                      (int? v) (int v)
                                      (instance? Long v) (long v)
                                      (instance? Float v) (float v)
                                      (instance? Double v) (double v)
                                      (bytes? v) v
                                      :else (str v))))

            :vector
            (let [sim (case (or similarity :euclidean)
                        :euclidean VectorSimilarityFunction/EUCLIDEAN
                        :cosine VectorSimilarityFunction/COSINE
                        :dot-product VectorSimilarityFunction/DOT_PRODUCT
                        :max-inner-product VectorSimilarityFunction/MAXIMUM_INNER_PRODUCT)]
              (.add doc (KnnFloatVectorField. fname ^floats v sim)))))))
    (.addDocument writer doc)))

(defn delete-docs
  "Delete documents matching the given term field and value."
  [sw ^String field ^String value]
  (let [^BranchIndexWriter writer (->writer sw)]
    (.deleteDocuments writer (into-array Term [(Term. field value)]))))

(defn update-doc
  "Update a document identified by the given term.

  Replaces the document matching (field, value) with the new doc-map.
  doc-map uses the same format as add-doc (supports all field types, multi-valued fields, auto-detection)."
  [sw ^String field ^String value doc-map]
  (let [^BranchIndexWriter writer (->writer sw)
        doc (Document.)]
    (doseq [[field-name value-or-opts] doc-map]
      (let [fname (name field-name)
            opts (if (map? value-or-opts) value-or-opts {:value value-or-opts})
            {:keys [value stored? store? type similarity]} opts
            store? (cond
                     (contains? opts :stored?) stored?
                     (contains? opts :store?) store?
                     :else true)
            store (if store? Field$Store/YES Field$Store/NO)

            ;; Auto-detect type from value (only if type not explicitly provided)
            [detected-type value']
            (if (contains? opts :type)
              [type value]
              (cond
                (instance? java.time.Instant value) [:long (.toEpochMilli ^Instant value)]
                (instance? java.util.Date value) [:long (.getTime ^java.util.Date value)]
                (and (class value) (= (.getName (class value)) "[F")) [:vector value]
                :else [:text value]))

            final-type detected-type

            ;; Handle multi-valued fields (vector of values)
            values (if (and (vector? value') (not= final-type :vector))
                     value'
                     [value'])]

        (doseq [v values]
          (case final-type
            :text
            (.add doc (TextField. fname (str v) store))

            :string
            (.add doc (StringField. fname (str v) store))

            :int
            (.add doc (IntField. fname (int v) store))

            :long
            (.add doc (LongField. fname (long v) store))

            :float
            (.add doc (FloatField. fname (float v) store))

            :double
            (.add doc (DoubleField. fname (double v) store))

            :stored-only
            (.add doc (StoredField. fname
                                    (cond
                                      (string? v) v
                                      (int? v) (int v)
                                      (instance? Long v) (long v)
                                      (instance? Float v) (float v)
                                      (instance? Double v) (double v)
                                      (bytes? v) v
                                      :else (str v))))

            :vector
            (let [sim (case (or similarity :euclidean)
                        :euclidean VectorSimilarityFunction/EUCLIDEAN
                        :cosine VectorSimilarityFunction/COSINE
                        :dot-product VectorSimilarityFunction/DOT_PRODUCT
                        :max-inner-product VectorSimilarityFunction/MAXIMUM_INNER_PRODUCT)]
              (.add doc (KnnFloatVectorField. fname ^floats v sim)))))))
    (.updateDocument writer (Term. field value) doc)))

;; --- Commit & Sync ---

(defn commit!
  "Commit changes on a branch. Stores timestamp in commit user-data.

  Optional message is stored for history/log purposes.
  Optional metadata is a map of string keys to string values stored in commit user-data.
  Metadata keys must NOT use the \"scriptum.\" prefix (reserved for internal use).

  Returns a map with:
    :generation - the commit generation number
    :commit-id - Lucene's internal commit UUID
    :content-hash - content-addressable merkle root (only when :crypto-hash? enabled)

  When :crypto-hash? is not enabled, returns just the generation number for backward compatibility.

  Example with metadata (for secondary index sync):
    (commit! writer \"Indexed tx\" {\"datahike.tx\" \"536870915\"})"
  ([sw]
   (commit! sw nil))
  ([sw ^String message]
   (commit! sw message nil))
  ([sw ^String message metadata]
   (let [^BranchIndexWriter writer (->writer sw)
         mi (->metadata-index sw)
         gen (.commit writer message
                      (when metadata
                        (java.util.HashMap. ^java.util.Map metadata)))
         commit-id (.getLastCommitId writer)
         content-hash (.getLastContentHash writer)]
     ;; Index metadata in PSS and flush
     (when mi
       (metadata/index! mi (.getBranchName writer) metadata gen)
       (metadata/flush-index! mi))
     (if content-hash
       {:generation gen
        :commit-id commit-id
        :content-hash content-hash}
       gen))))

(defn seal-generation!
  "Seal a detached generation and return its immutable snapshot address.

  The writer is closed, but its GC guard remains held. Publish the address in
  the embedding system's root and then call `release-generation!`. On failure,
  the guard is released and any written-but-unreachable objects are left for
  ordinary content-addressed GC."
  ([sw]
   (seal-generation! sw nil nil))
  ([sw message]
   (seal-generation! sw message nil))
  ([sw message metadata]
   (let [{:keys [lifecycle directory]} (generation-backing sw)]
     (when-not (= :open (:status @lifecycle))
       (throw (ex-info "scriptum: detached generation is not open"
                       {:lifecycle @lifecycle})))
     (try
       (commit! sw message metadata)
       ;; Latest-only deletion runs from Lucene's post-commit policy callback.
       ;; It removes the superseded segments_N after the commit's first metadata
       ;; sync. Lucene has no reason to sync again after its deletion policy
       ;; returns, so explicitly publish the deletion-only file map before the
       ;; Directory is closed. The detached outer guard covers both immutable
       ;; snapshots, and only this final address escapes to the embedder.
       (.syncMetaData ^Directory directory)
       (.close ^BranchIndexWriter (->writer sw))
       (let [address (snapshot-address sw)]
         (when-not address
           (throw (ex-info "scriptum: detached generation sealed without an address" {})))
         (cleanup-generation-workspace! sw)
         (reset! lifecycle {:status :sealed
                            :address address
                            :released? false})
         address)
       (catch Throwable failure
         (try (.rollback ^BranchIndexWriter (->writer sw)) (catch Throwable _))
         (try
           (cleanup-generation-workspace! sw)
           (finally
             (release-generation-guard! sw)))
         (reset! lifecycle {:status :failed
                            :failure failure
                            :released? true})
         (throw failure))))))

(defn release-generation!
  "Release a sealed generation's GC guard after an external root names it.

  Idempotent. This does not move or delete any Scriptum ref; the immutable
  generation remains live exactly while the embedding root marks its address."
  [sw]
  (let [{:keys [lifecycle publication-hold-transferred?]}
        (generation-backing sw)
        guard-token (:guard-token (generation-backing sw))]
    (locking guard-token
      (let [{:keys [status released?]} @lifecycle]
        (when @publication-hold-transferred?
          (throw (ex-info "scriptum: generation publication hold belongs to another owner"
                          {:type :scriptum/generation-publication-hold-transferred
                           :address (:address @lifecycle)})))
        (when-not (or (= :sealed status) released?)
          (throw (ex-info "scriptum: only a sealed generation can be released"
                          {:lifecycle @lifecycle})))
        (when-not released?
          (release-generation-guard! sw))
        (cleanup-generation-workspace! sw)
        nil))))

(defn abort-generation!
  "Discard an open detached writer and release its GC guard.

  Already sealed generations cannot be unwritten; aborting one merely releases
  its guard so the unreachable snapshot can be collected. Idempotent."
  [sw]
  (let [{:keys [lifecycle publication-hold-transferred?]}
        (generation-backing sw)
        guard-token (:guard-token (generation-backing sw))]
    (locking guard-token
      (let [{:keys [status released?]} @lifecycle]
        (when @publication-hold-transferred?
          (throw (ex-info "scriptum: generation publication hold belongs to another owner"
                          {:type :scriptum/generation-publication-hold-transferred
                           :address (:address @lifecycle)})))
        (when-not released?
          (try
            (when (= :open status)
              (.rollback ^BranchIndexWriter (->writer sw))
              (swap! lifecycle assoc :status :aborted))
            (finally
              (try
                (cleanup-generation-workspace! sw)
                (finally
                  ;; A rollback failure must not pin every future collection at this
                  ;; generation's start time. Its objects are unreachable garbage.
                  (release-generation-guard! sw))))))
        nil))))

(defn take-generation-publication-hold!
  "Detach a sealed generation's lightweight GC hold from its closed writer.

   Carry this value until the embedding owner publishes the snapshot address.
   It retains no Lucene writer, Directory, reader, or local cache. Complete it
   with `root-generation-publication!` after publication or
   `abort-generation-publication!` after a definitive abort. An ambiguous
   publication must retain the hold."
  [sw]
  (let [{:keys [store-id guard-token lifecycle
                publication-hold-transferred?]}
        (generation-backing sw)]
    (locking guard-token
      (let [{:keys [status address released?]} @lifecycle]
        (when-not (and (= :sealed status) (not released?))
          (throw (ex-info "scriptum: only a live sealed generation can transfer its publication hold"
                          {:type :scriptum/invalid-generation-publication-hold
                           :lifecycle @lifecycle})))
        (when-not (compare-and-set! publication-hold-transferred? false true)
          (throw (ex-info "scriptum: generation publication hold was already transferred"
                          {:type :scriptum/generation-publication-hold-transferred
                           :address address})))
        (let [token @guard-token]
          (when-not token
            (throw (ex-info "scriptum: sealed generation has no publication guard"
                            {:type :scriptum/missing-generation-publication-guard
                             :address address})))
          (reset! guard-token nil)
          (->GenerationPublicationHold store-id token address
                                       (atom false) (atom nil)))))))

(defn- complete-generation-publication!
  [^GenerationPublicationHold hold completion]
  (locking hold
    (when-not @(:completed? hold)
      (guard/done! (:store-id hold) (:guard-token hold))
      (reset! (:outcome hold) completion)
      (reset! (:completed? hold) true)))
  hold)

(defn root-generation-publication!
  "Complete a transferred hold after the owner root durably names its address."
  [hold]
  (complete-generation-publication! hold :committed))

(defn abort-generation-publication!
  "Complete a transferred hold after publication is definitively aborted."
  [hold]
  (complete-generation-publication! hold :aborted))

(defn verify-commit
  "Verify the cryptographic integrity of a commit by recomputing its merkle hash.

  Requires that the index was created with :crypto-hash? true.

  Options:
    :generation - commit generation to verify (default: -1 for current HEAD)

  Returns a map with:
    :valid? - boolean indicating if verification passed
    :commit-id - the commit UUID that was verified
    :errors - vector of error messages (empty if valid)

  Example:
    (verify-commit writer)                    ; verify current commit
    (verify-commit writer {:generation 5})    ; verify specific generation"
  ([sw]
   (verify-commit sw {}))
  ([sw {:keys [generation] :or {generation -1}}]
   (let [^BranchIndexWriter writer (->writer sw)
         result (.verifyCommit writer (long generation))]
     {:valid? (.get result "valid")
      :commit-id (.get result "commitId")
      :errors (vec (.get result "errors"))})))

(defn flush!
  "Flush pending changes without committing (no durability, but NRT visible)."
  [sw]
  (let [^BranchIndexWriter writer (->writer sw)]
    (.flush writer)))

;; --- Query Builders ---

(defn term-query
  "Build an analyzer-free exact query for one indexed term.

  This is the query counterpart of a `:string` (`StringField`) value: the term
  is used exactly as supplied, with no tokenization, stemming, case folding or
  stop-word removal. It is therefore suitable for already-normalized tokens
  such as PostgreSQL lexemes. It is not a shortcut for querying `:text`
  (`TextField`) values; callers of analyzed fields should use `text-query`.

  The query is constant-score deliberately. Scriptum still exposes Lucene's
  score in search results and candidate continuations, because `searchAfter`
  needs the complete sort cursor, but that score has no semantic meaning here:
  it is neither PostgreSQL text-search rank nor evidence of a better match."
  [field term]
  (ConstantScoreQuery.
   (TermQuery. (Term. (name field) (str term)))))

(defn terms-query
  "Build an analyzer-free exact query matching any value in `values`.

  This is the set counterpart of `term-query`, intended for `:string`
  (`StringField`) values. Lucene builds one automaton-backed query rather than
  one Boolean clause per value, so this also avoids `BooleanQuery`'s clause
  limit. It is useful for intersecting a full-text candidate scan with a set of
  externally selected document identities before collection. Empty input is a
  valid query matching no documents. Like `term-query`, the result is constant
  score; application-visible ranking remains the caller's responsibility."
  [field values]
  (TermInSetQuery.
   (name field)
   (mapv #(BytesRef. (str %)) values)))

(defn prefix-query
  "Build an analyzer-free prefix query over indexed terms.

  Like `term-query`, this is intended for `:string` (`StringField`) values and
  already-normalized tokens. `prefix` is matched byte-for-byte at the start of
  each complete indexed term; no analyzer runs and case is preserved. A
  multi-valued StringField document is returned once when one or more of its
  values match.

  The constant Lucene score is intentionally non-semantic. It exists in
  candidate pages only as part of the immutable `searchAfter` cursor; callers
  implementing PostgreSQL ranking must compute that rank themselves."
  [field prefix]
  (ConstantScoreQuery.
   (PrefixQuery. (Term. (name field) (str prefix)))))

(defn text-query
  "Parse a text query string against a single field using the given analyzer.

  Uses Lucene's QueryParser to handle operators (+, -, AND, OR, NOT),
  phrases (\"quoted text\"), wildcards (*), and fuzzy matching (~).

  Args:
    field    - field name to search (string or keyword)
    text     - query string
    analyzer - Lucene Analyzer (optional, defaults to StandardAnalyzer)"
  ([field text]
   (text-query field text (StandardAnalyzer.)))
  ([field text ^Analyzer analyzer]
   (.parse (QueryParser. (name field) analyzer) text)))

(defn multi-field-query
  "Parse a text query string across multiple fields.

  Each token is searched across all given fields with SHOULD semantics
  (match in any field counts).

  Args:
    fields   - seq of field names (strings or keywords)
    text     - query string
    analyzer - Lucene Analyzer (optional, defaults to StandardAnalyzer)

  Example:
    (multi-field-query [\"title\" \"content\"] \"clojure reactive\")
    ;; Matches docs where title OR content contains 'clojure reactive'"
  ([fields text]
   (multi-field-query fields text (StandardAnalyzer.)))
  ([fields text ^Analyzer analyzer]
   (let [field-arr  (into-array String (map name fields))
         occurs-arr (into-array BooleanClause$Occur
                                (repeat (count fields) BooleanClause$Occur/SHOULD))]
     (MultiFieldQueryParser/parse text field-arr occurs-arr analyzer))))

(defn bool-query
  "Build a BooleanQuery from clause specs.

  Each clause is a vector of [query occur] where occur is one of:
    :must, :should, :must-not, :filter

  Example:
    (bool-query [[(text-query \"title\" \"clojure\") :should]
                 [(text-query \"content\" \"clojure\") :should]
                 [{:term [:source \"youtube\"]} :filter]])"
  [clauses]
  (let [builder (BooleanQuery$Builder.)]
    (doseq [[q occur] clauses]
      (let [lucene-q (cond
                       (instance? org.apache.lucene.search.Query q) q
                       (map? q) (let [[field value] (:term q)]
                                  (TermQuery. (Term. (name field) (str value))))
                       :else (throw (ex-info "Unknown query type" {:query q})))
            lucene-occur (case occur
                           :must     BooleanClause$Occur/MUST
                           :should   BooleanClause$Occur/SHOULD
                           :must-not BooleanClause$Occur/MUST_NOT
                           :filter   BooleanClause$Occur/FILTER)]
        (.add builder lucene-q lucene-occur)))
    (.build builder)))

;; --- Search ---

(defn- ->query
  "Normalize Scriptum's public query forms against `reader`."
  [^DirectoryReader reader query]
  (cond
    (instance? org.apache.lucene.search.Query query)
    query

    (map? query)
    (let [[field value] (:term query)]
      (TermQuery. (Term. (name field) (str value))))

    (string? query)
    (let [fields (into-array String
                             (sort (org.apache.lucene.index.FieldInfos/getIndexedFields
                                    reader)))]
      (if (zero? (alength fields))
        (MatchAllDocsQuery.)
        (.parse (MultiFieldQueryParser. fields (StandardAnalyzer.)) ^String query)))

    (= :all query)
    (MatchAllDocsQuery.)

    :else
    (throw (ex-info (str "scriptum: unsupported query " (pr-str query)
                         " — pass :all, {:term [field value]}, a string, "
                         "or a Lucene Query")
                    {:query query}))))

(defn- hits->results
  [^IndexSearcher searcher hits fields]
  (let [sf (.storedFields searcher)
        keep? (if (seq fields) (set (map name fields)) (constantly true))]
    (mapv (fn [^ScoreDoc sd]
            (let [stored (.document sf (.-doc sd))
                  field-map (into {}
                                  (comp (filter (fn [^IndexableField f]
                                                  (keep? (.name f))))
                                        (map (fn [^IndexableField f]
                                               [(.name f) (.stringValue f)])))
                                  (.getFields stored))]
              (assoc field-map
                     :doc-id (.-doc sd)
                     :score (.-score sd))))
          hits)))

(defn search
  "Search a branch. Returns a vector of maps with :doc-id, :score, and field values.

  query can be:
    - A Lucene Query object
    - A map {:term [field value]} for a term query
    - A string (matches all documents containing this term in any field)

  Options:
    :limit - max results (default 10)
    :fields - fields to retrieve (default: all stored fields)
    :reader - search THIS reader instead of opening one (see below)

  BY DEFAULT THIS OPENS A FRESH NRT READER, so it reflects the writer's state
  including uncommitted changes — add a document and it is findable before any
  commit. That is the semantics a git-like writer wants and it is not free:
  `DirectoryReader.open(writer)` flushes every in-memory buffer, so a loop that
  alternates writing and searching materializes a segment PER SEARCH. Measured
  at 5.08 ms per write-then-search cycle against 0.159 ms reusing a reader —
  32x, and the cost is segment churn rather than reader construction, which is
  cheap (0.016 ms at one segment, 0.121 ms at 54).

  So pass `:reader` when searching repeatedly without writing, or when writing
  and searching in a loop. `snapshot`, `with-snapshot` and `open-reader-at`
  hand out exactly the right object, which is also what makes this compose with
  time travel. Whoever opens the reader closes it; scriptum owns no lifecycle
  here, deliberately.

  A held reader is a POINT IN TIME. It will not show later writes, and — more
  sharply — it will still return documents deleted since, so a caller filtering
  on identity must expect rows it has already removed. Reopen or take a fresh
  `snapshot` to move forward.

  Scriptum caches no searcher of its own, and that is a decision rather than an
  omission: a cached NRT searcher refreshed on `commit!` was measured to break
  read-your-own-writes, resurrect deleted documents between refreshes, and miss
  `merge-from!` entirely — 5 documents against 13 — because that path commits
  inside the Java layer without passing through `commit!`. The realistic gain
  was 1.05-3.5x on a mixed query load, which is a poor price for those."
  ([sw query]
   (search sw query {}))
  ([sw query {:keys [limit fields reader] :or {limit 10}}]
   (let [^BranchIndexWriter writer (->writer sw)
         own-reader? (nil? reader)
         ^DirectoryReader reader (or reader (.openReader writer))]
     (try
       (let [searcher (IndexSearcher. reader)
             q (->query reader query)
             top-docs (.search searcher q (int limit))
             hits (.-scoreDocs top-docs)]
         (hits->results searcher hits fields))
       ;; Close only what we opened. A caller-supplied reader outlives this
       ;; call by design — that is the whole point of passing one.
       (finally
         (when own-reader? (.close reader)))))))

(defn open-store-snapshot
  "Open the immutable konserve index state named by `address`.

  Unlike `open-store-index`, this does not resolve or create a branch and never
  opens an IndexWriter. The returned Closeable owns one logical lease on a
  read-only Directory and DirectoryReader pinned to the exact snapshot address.
  Opens of the same store/cache/address share that physical resource safely;
  it closes after the last lease. Branch commits cannot change what it sees.

  The caller must keep `address` in the `extra-snapshots` root set passed to
  `scriptum.konserve/gc!` for as long as the handle may need to be reopened.
  An already-open reader remains usable from its materialized files, but an
  unrooted snapshot is intentionally eligible for durable collection."
  ^StoreSnapshot [store ^String cache address]
  (snapshot-lease (acquire-snapshot-resource! store cache address) address))

(defn retain-store-snapshot
  "Return an independent closeable lease on the same immutable snapshot.

  The physical Directory and DirectoryReader remain open until every retained
  lease closes. This is the operation immutable DB wrappers use when preparation
  returns a new wrapper over an unchanged generation: sharing the same
  StoreSnapshot object gives two owners one close bit and lets either invalidate
  the other. A Cleaner releases an abandoned logical lease eventually, but
  callers should close explicitly for deterministic cache/resource reclamation."
  ^StoreSnapshot [^StoreSnapshot snapshot]
  (let [resource (:resource snapshot)]
    (locking snapshot-resource-lock
      (when @(:closed? snapshot)
        (throw (ex-info "scriptum: cannot retain a closed snapshot lease"
                        {:snapshot-address (:snapshot-address snapshot)})))
      (let [resources (.get snapshot-resources (:store resource))]
        (when-not (identical? resource (get resources (:resource-key resource)))
          (throw (ex-info "scriptum: snapshot resource is no longer registered"
                          {:snapshot-address (:snapshot-address snapshot)})))
        (swap! (:refs resource) inc)))
    (snapshot-lease resource (:snapshot-address snapshot))))

(defn search-store-snapshot
  "Search an immutable `StoreSnapshot`, returning the same result vector as
  `search` without requiring a live branch or writer.

  Options are `:limit` (default 10) and `:fields`. For complete, resumable
  candidate enumeration use `candidate-page` instead."
  ([snapshot query]
   (search-store-snapshot snapshot query {}))
  ([^StoreSnapshot snapshot query {:keys [limit fields] :or {limit 10}}]
   (let [^DirectoryReader reader (:reader snapshot)
         searcher (IndexSearcher. reader)
         top-docs (.search searcher (->query reader query) (int limit))]
     (hits->results searcher (.-scoreDocs top-docs) fields))))

(defn count-store-snapshot
  "Return the exact number of documents matching `query` in an immutable
  `StoreSnapshot` without materializing hits.

  This is the selectivity primitive for callers choosing between an inverted
  index and a primary scan.  It uses the snapshot's already-open reader, so the
  estimate names exactly the same generation as a subsequent candidate scan."
  [^StoreSnapshot snapshot query]
  (let [^DirectoryReader reader (:reader snapshot)
        searcher (IndexSearcher. reader)]
    (.count searcher (->query reader query))))

(defn candidate-page
  "Return one resumable page of candidates from an immutable snapshot.

  The envelope is stable for the lifetime of the snapshot:

    {:snapshot-address <address>
     :candidates         [{:doc-id n :score f ...stored fields...} ...]
     :continuation       {:version 3 :snapshot-address <address>
                          :query <lucene-query>
                          :doc-id n :score f} ; nil when exhausted
     :exhausted?         boolean
     :ordering           :score-desc-doc-id-asc}

  `:continuation` is passed back as `:after`. It is deliberately bound to the
  snapshot address: resuming it against another generation throws instead of
  silently skipping or repeating candidates. Lucene's `searchAfter` supplies
  the ordering, so callers can keep requesting pages until `:exhausted?`
  without imposing a fixed top-N cutoff.

  `:score` and the score-first `:ordering` describe Lucene's paging cursor,
  not application semantics. In particular, the analyzer-free `term-query`
  and `prefix-query` builders use constant scores. A PostgreSQL adapter must
  recheck candidates and compute `ts_rank` or any other user-visible rank from
  PostgreSQL semantics rather than interpreting this score.

  Options:
    :page-size - positive number of candidates to return (default 100)
    :after     - continuation from the preceding page, or nil
    :fields    - stored fields to include (default all)
    :query-id  - optional caller-supplied stable query fingerprint
    :order     - :score (default) or :doc-id

  `:doc-id` uses Lucene's exact index order without computing scores. It is the
  cheaper collector for callers that need a complete candidate set and apply
  their own authoritative predicate/ranking afterwards. Its continuation is
  still snapshot- and query-bound, but its candidates have NaN `:score` values
  because no score was computed. Index order is stable only within the named
  immutable snapshot; never carry this ordering across generations.

  The opaque continuation stores Lucene's normalized Query and checks its
  structural equality on resume. This makes changing the actual query between
  pages an error even when `:query-id` is omitted or accidentally reused.
  `:query-id` remains useful as an adapter-owned semantic fingerprint in
  addition to that structural check; it is copied into the continuation and
  checked on resume."
  ([snapshot query]
   (candidate-page snapshot query {}))
  ([^StoreSnapshot snapshot query {:keys [page-size after fields query-id order]
                                   :or {page-size 100 order :score}}]
   (when-not (and (integer? page-size)
                  (pos? page-size)
                  (< page-size Integer/MAX_VALUE))
     (throw (ex-info "scriptum: page-size must be a positive integer below Integer/MAX_VALUE"
                     {:page-size page-size})))
   (when-not (contains? #{:score :doc-id} order)
     (throw (ex-info "scriptum: candidate order must be :score or :doc-id"
                     {:order order})))
   (let [address (:snapshot-address snapshot)
         ^DirectoryReader reader (:reader snapshot)
         searcher (IndexSearcher. reader)
         q (->query reader query)]
     (when (and after
                (or (not= (if (= :doc-id order) 4 3) (:version after))
                    (not= address (:snapshot-address after))
                    (not= query-id (:query-id after))
                    (not= q (:query after))
                    (not (integer? (:doc-id after)))
                    (if (= :doc-id order)
                      (not= :doc-id (:order after))
                      (not (number? (:score after))))))
       (throw (ex-info "scriptum: continuation does not belong to this snapshot/query"
                       {:type :scriptum/continuation-mismatch
                        :snapshot-address address
                        :query-id query-id
                        :order order
                        :continuation after})))
     (let [^ScoreDoc after-doc (when after
                                 (if (= :doc-id order)
                                   (FieldDoc. (int (:doc-id after)) Float/NaN
                                              (object-array
                                               [(int (:doc-id after))]))
                                   (ScoreDoc. (int (:doc-id after))
                                              (float (:score after)))))
           ;; Ask for one extra candidate so exhaustion is exact even when the
           ;; remaining hit count is exactly one full page.
           top-docs (if (= :doc-id order)
                      (.searchAfter searcher after-doc q (int (inc page-size))
                                    Sort/INDEXORDER false)
                      (.searchAfter searcher after-doc q (int (inc page-size))))
           score-docs (vec (.-scoreDocs top-docs))
           more? (> (count score-docs) page-size)
           page-hits (if more? (subvec score-docs 0 page-size) score-docs)
           candidates (hits->results searcher page-hits fields)
           last-hit (peek page-hits)]
       {:snapshot-address address
        :candidates candidates
        :continuation (when more?
                        (cond->
                         {:version (if (= :doc-id order) 4 3)
                          :snapshot-address address
                          :query-id query-id
                          :query q
                          :doc-id (.-doc ^ScoreDoc last-hit)}
                          (= :doc-id order) (assoc :order :doc-id)
                          (= :score order)
                          (assoc :score (.-score ^ScoreDoc last-hit))))
        :exhausted? (not more?)
        :ordering (if (= :doc-id order)
                    :doc-id-asc
                    :score-desc-doc-id-asc)}))))

;; --- Snapshots & Time-Travel ---

(defn list-snapshots
  "List all available snapshots (commit points) for this branch.

  Returns a vector of maps with :generation, :snapshot-id, :timestamp,
  :message, :branch, :segment-count, :parent-ids, and :custom-metadata.

  :custom-metadata is a map of any non-scriptum keys stored in commit user-data."
  [sw]
  (let [^BranchIndexWriter writer (->writer sw)]
    (mapv (fn [m]
            (let [base {:generation (.get m "generation")
                        :snapshot-id (.get m "snapshotId")
                        :segment-count (.get m "segmentCount")
                        :timestamp (.get m "timestamp")
                        :message (.get m "message")
                        :branch (.get m "branch")
                        :parent-ids (.get m "parentIds")}
                  custom (.get m "customMetadata")]
              (if custom
                (assoc base :custom-metadata (into {} custom))
                base)))
          (.listSnapshots writer))))

(defn open-reader-at
  "Open a reader at a specific commit generation (time-travel).

  The caller is responsible for closing the reader.
  Throws if the generation has been GC'd."
  ^DirectoryReader [sw ^long generation]
  (let [^BranchIndexWriter writer (->writer sw)]
    (.openReaderAt writer generation)))

(defn commit-available?
  "Check if a specific commit generation is still available (not GC'd)."
  [sw ^long generation]
  (let [^BranchIndexWriter writer (->writer sw)]
    (.isCommitAvailable writer generation)))

(defn find-generation
  "Find the commit generation matching a custom metadata key/value.

  mode can be:
    :exact - exact match (default)
    :floor - latest commit whose metadata value <= target (for monotonic values like tx IDs)

  Returns nil if no match, or a map with :generation (and :indexed-value for :floor mode).

  Example:
    (find-generation writer \"datahike/tx\" \"536870915\")
    (find-generation writer \"datahike/tx\" \"536870915\" :floor)"
  ([sw ^String key ^String value]
   (find-generation sw key value :exact))
  ([sw ^String key ^String value mode]
   (let [^BranchIndexWriter writer (->writer sw)
         mi (->metadata-index sw)
         branch (.getBranchName writer)]
     (if mi
       (case mode
         :exact (metadata/find-exact mi branch key value)
         :floor (metadata/find-floor mi branch key value))
       ;; Fallback for raw BranchIndexWriter (shouldn't happen normally)
       nil))))

(defn snapshot
  "Take an immutable snapshot (DirectoryReader) of the current branch.
   The caller is responsible for closing the reader."
  ^DirectoryReader [sw]
  (let [^BranchIndexWriter writer (->writer sw)]
    (.openReader writer)))

(defn with-snapshot
  "Execute f with an immutable snapshot reader. Reader is closed after."
  [sw f]
  (with-open [reader (snapshot sw)]
    (f reader)))

;; --- GC ---

(defn gc!
  "Garbage collect old commit points and unreferenced segment files.

  Only callable on the main branch writer. Scans all branches to determine
  which files are still needed before removing anything.

  IT RECLAIMS NOTHING WHILE A BRANCH STILL SHARES FILES WITH MAIN, which after
  an ordinary fork is always. Protection is per COMMIT POINT: one is spared if
  it references any file some branch references, and a fresh fork shares every
  base segment by construction. Measured over 6 commits:

    no branch                  6 removed
    one fork, untouched        0 removed   (and the call adds a commit point)
    one fork, closed           0 removed
    fork force-merged onto
      its own segment          7 removed
    empty branch directory     6 removed

  So the condition is narrower than `a branch exists`: a branch that has merged
  away its inherited segments stops protecting them, and an empty directory
  protects nothing. But the common case — fork and keep working — does pin every
  commit point on main indefinitely, and a call that reclaims nothing still adds
  one, so history grows.

  The conservatism is in the safe direction: nothing is deleted that a branch
  might need. Fixing it means protecting FILES rather than commit points, which
  Lucene's deletion-policy interface cannot express directly.

  The store-backed model does not have the SHARING problem — reachability is
  computed across every branch's manifest, so a shared segment is protected by
  being named rather than by freezing the commit point that names it. It still
  needs `retain!` to drop commit points before anything becomes unreachable;
  neither model collects history you have not asked it to drop.

  before: java.time.Instant — delete commits older than this
  Returns the number of commit points removed."
  [sw ^Instant before]
  (when (store-backed? sw)
    ;; A store-backed index collects by reachability from the live manifests,
    ;; not by ageing commit points out of a directory — and its cutoff has to
    ;; be derived from konserve.gc-guard BEFORE the manifests are walked, which
    ;; `scriptum.konserve/gc!` does and this cannot.
    (throw (ex-info "scriptum: use scriptum.konserve/gc! for a store-backed index"
                    {:branch (.getBranchName (->writer sw))})))
  (let [^BranchIndexWriter writer (->writer sw)
        mi (->metadata-index sw)
        removed (.gc writer before)]
    ;; Rebuild metadata index from surviving snapshots
    (when mi
      (let [base-path (str (.getBasePath writer))
            ;; Collect surviving snapshots from main branch
            main-snaps (list-snapshots sw)
            ;; Also collect from all known branches
            branch-names (discover-branches base-path)
            snapshots-by-branch
            (reduce
             (fn [acc bname]
               ;; A branch we cannot read is OMITTED, and omission now means
               ;; "leave its entries alone" rather than "it has none" — see
               ;; `metadata/rebuild-from-snapshots!`. The common reason to land
               ;; here is LockObtainFailedException from a branch whose writer is
               ;; open, i.e. the documented main+feature workflow, and erasing
               ;; that branch's metadata on every collection of main is not a
               ;; defensible reading of a lock being held.
               (let [bw (try (BranchIndexWriter/open (->path base-path) bname
                                                     (StandardAnalyzer.))
                             (catch Exception _ nil))]
                 (if-not bw
                   acc
                   (try
                     (assoc acc bname
                            (mapv (fn [m]
                                    {:generation (.get m "generation")
                                     :custom-metadata
                                     (when-let [cm (.get m "customMetadata")]
                                       (into {} cm))})
                                  (.listSnapshots bw)))
                     (catch Exception _ acc)
                     ;; `.close` in a finally: it was after `.listSnapshots`, so a
                     ;; throw there leaked the writer AND left its lock held.
                     (finally (.close bw))))))
             {(.getBranchName writer) (mapv (fn [s] {:generation (:generation s)
                                                     :custom-metadata (:custom-metadata s)})
                                            main-snaps)}
             branch-names)]
        (metadata/rebuild-from-snapshots! mi snapshots-by-branch)
        (metadata/flush-index! mi)))
    removed))

;; --- Accessors ---

(defn num-docs
  "Returns the number of documents in this branch (excluding deletions)."
  [sw]
  (let [^BranchIndexWriter writer (->writer sw)]
    (.numDocs writer)))

(defn max-doc
  "Returns the total number of documents (including deletions)."
  [sw]
  (let [^BranchIndexWriter writer (->writer sw)]
    (.maxDoc writer)))

(defn branch-name
  "Returns the branch name."
  [sw]
  (let [^BranchIndexWriter writer (->writer sw)]
    (.getBranchName writer)))

(defn base-path
  "Returns the base path of the index."
  [sw]
  (let [^BranchIndexWriter writer (->writer sw)]
    (str (.getBasePath writer))))

(defn main-branch?
  "Returns true if this is the main (trunk) branch."
  [sw]
  (let [^BranchIndexWriter writer (->writer sw)]
    (.isMainBranch writer)))

(defn merge-from!
  "Merge segments from a source branch into this branch.

  Uses reader-based addIndexes to avoid lock conflicts with source writer."
  [target source]
  (let [^BranchIndexWriter tw (->writer target)
        ^BranchIndexWriter sw (->writer source)]
    ;; RECORD THE LINEAGE BEING MERGED IN, so the commit that follows names it
    ;; as a parent. Without this the merged branch's history is not an ancestor
    ;; of the result: it survives as segments, but nothing walking parents can
    ;; reach it, and the head address stops covering it.
    ;; COMMIT THE SOURCE FIRST, THEN RECORD, THEN MERGE — and the ordering took
    ;; two attempts to get right, so it is worth spelling out. `mergeFrom` commits
    ;; the source (for a consistent read), then commits the TARGET twice: once
    ;; pre-merge and once for the merge itself.
    ;;
    ;; Recording before that sequence attached the source's PRE-commit head — not
    ;; the state actually merged. Recording after it attached nothing at all: the
    ;; target's own commits inside `mergeFrom` had already consumed and cleared
    ;; the pending set, so the address sat in an atom nobody read, and the merged
    ;; lineage was not an ancestor of the result by any route.
    ;;
    ;; Committing the source ourselves makes its address the merged state, and
    ;; recording it before `mergeFrom` lets the target's pre-merge commit carry
    ;; it — which the merge commit then descends from. Both invariants hold: the
    ;; merged lineage is reachable, and it is the lineage that was merged. The
    ;; source commit inside `mergeFrom` is then a no-op, since nothing changed.
    (if (and (store-backed? target) (store-backed? source))
      (do
        ;; Commit the source HERE, so the address recorded is the state the merge
        ;; will actually read, and tell `mergeFrom` not to commit it again —
        ;; `commit` always writes fresh commit data, so it is never a no-op and
        ;; would supersede the address with one nothing points at.
        (.commit sw "Pre-merge snapshot")
        (when-let [a (snapshot-address source)]
          (swap! (:pending-parents (:backing target)) conj a))
        (.mergeFrom tw sw false))
      (.mergeFrom tw sw))))

(defn close!
  "Close a branch writer and its resources."
  [sw]
  (if (generation? sw)
    (let [{:keys [status]} @(get-in sw [:backing :lifecycle])
          transferred? @(get-in sw [:backing :publication-hold-transferred?])]
      ;; Closing an unsealed private generation must never invoke Lucene's
      ;; commit-on-close. A sealed one has already closed its writer and only
      ;; retains the publication guard.
      (cond
        transferred? (do (cleanup-generation-workspace! sw) nil)
        (= :open status) (abort-generation! sw)
        :else (release-generation! sw)))
    (let [^BranchIndexWriter writer (->writer sw)
          mi (->metadata-index sw)]
      (when mi
        (metadata/close-index! mi))
      ;; ONE close. `BranchIndexWriter.close` closes the Directory it was given —
      ;; `createOver`'s javadoc used to claim the caller owned it, and closing it
      ;; again here on that basis threw AlreadyClosedException on any Directory
      ;; that reports being closed.
      (.close writer))))

(defn abort!
  "Discard uncommitted changes and close a writer without publishing them.

  This is the failure-path counterpart to `close!`: ordinary close uses
  Lucene's commit-on-close semantics, while abort delegates to
  `IndexWriter.rollback`. For a konserve-backed writer its Directory is closed
  without a `syncMetaData` flip, so the branch manifest remains at its last
  successful commit. The writer must not be used afterwards."
  [sw]
  (if (generation? sw)
    (abort-generation! sw)
    (let [^BranchIndexWriter writer (->writer sw)
          mi (->metadata-index sw)]
      ;; Rollback is the load-bearing action: a metadata close failure must not
      ;; leave a commit-on-close writer alive for later accidental publication.
      (try
        (.rollback writer)
        (finally
          (when mi
            (metadata/close-index! mi)))))))
