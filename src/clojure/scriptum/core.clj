(ns scriptum.core
  "COW branching semantics on top of Apache Lucene.

  Provides fast forking (~3-5ms), structural sharing of immutable segments,
  branch-isolated indexing/searching, snapshot retention, and explicit GC.

  Key concepts:
  - Writer: mutable handle to a branch (one per branch per JVM)
  - Snapshot: immutable DirectoryReader at a specific commit point
  - Branch: COW overlay sharing base segments with the trunk
  - GC: explicit cleanup of old snapshots respecting branch references"
  (:require [scriptum.metadata :as metadata])
  (:import [java.nio.file Path Paths]
           [java.time Instant Duration]
           [org.apache.lucene.analysis Analyzer]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.document Document Field$Store TextField StringField
            IntField LongField FloatField DoubleField StoredField
            KnnFloatVectorField]
           [org.apache.lucene.index DirectoryReader IndexableField Term
            VectorSimilarityFunction]
           [org.apache.lucene.search IndexSearcher TermQuery BooleanQuery
            BooleanQuery$Builder BooleanClause$Occur TopDocs ScoreDoc
            MatchAllDocsQuery KnnFloatVectorQuery]
           [org.apache.lucene.queryparser.classic QueryParser MultiFieldQueryParser]
           [org.apache.lucene.store FSDirectory]
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

(defn- sk
  "Resolve a `scriptum.konserve` var on first use.

  Required lazily, not with `:require`, so the DIRECTORY-backed API keeps
  loading against a released konserve. `scriptum.konserve` needs
  `konserve.gc-guard`, which is unreleased; a static require would make every
  existing user of this namespace need it too, for a backing they may never
  touch."
  [sym]
  (or (requiring-resolve (symbol "scriptum.konserve" (name sym)))
      (throw (ex-info "scriptum: scriptum.konserve is unavailable"
                      {:var sym}))))

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
    (let [{:keys [store cache]} (:backing sw)]
      (.commit (->writer sw))
      ((sk 'fork!) store (.getBranchName (->writer sw)) new-branch-name)
      (open-store-index store cache new-branch-name
                        {:metadata-index (->metadata-index sw)}))
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

  Options:
    :analyzer - the Lucene Analyzer (default: StandardAnalyzer)
    :metadata-index - shared metadata index (default: none)
    :store-id - id for konserve.gc-guard, so a collection cannot sweep this
                index's in-flight segment writes. Should come off the store
                itself; see `scriptum.konserve/konserve-directory`.
    :max-merged-segment-mb / :ram-buffer-mb - see `create-index`. Against a
                remote store start from `scriptum.konserve/remote-tuning`.

  Returns a ScriptumWriter. Document operations, search, commit and readers
  behave exactly as for a directory-backed index; `fork`, `branches` and `gc!`
  answer from the manifests instead of the filesystem."
  ([store cache branch] (open-store-index store cache branch {}))
  ([store ^String cache ^String branch
    {:keys [analyzer metadata-index store-id max-merged-segment-mb ram-buffer-mb]}]
   (let [analyzer (or analyzer (StandardAnalyzer.))
         dir ((sk 'konserve-directory) store cache branch store-id)
         writer (BranchIndexWriter/createOver dir branch analyzer)]
     (tune! writer max-merged-segment-mb ram-buffer-mb)
     (->ScriptumWriter writer metadata-index
                       {:store store :cache cache :directory dir}))))

(defn branches
  "Every branch of a store-backed index, from its manifests."
  [sw]
  (if (store-backed? sw)
    ((sk 'branches) (:store (:backing sw)))
    (throw (ex-info "scriptum: branches is for store-backed indices; use discover-branches for a path"
                    {:writer sw}))))

(defn discover-branches
  "Discover all branch names at the given path.

  Returns a set of branch name strings."
  [^String path]
  (BranchIndexWriter/discoverBranches (->path path)))

;; --- Document Operations ---

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

(defn search
  "Search a branch. Returns a vector of maps with :doc-id, :score, and field values.

  query can be:
    - A Lucene Query object
    - A map {:term [field value]} for a term query
    - A string (matches all documents containing this term in any field)

  Options:
    :limit - max results (default 10)
    :fields - fields to retrieve (default: all stored fields)"
  ([sw query]
   (search sw query {}))
  ([sw query {:keys [limit fields] :or {limit 10}}]
   (let [^BranchIndexWriter writer (->writer sw)]
     (with-open [reader (.openReader writer)]
       (let [searcher (IndexSearcher. reader)
             q (cond
                 (instance? org.apache.lucene.search.Query query)
                 query

                 (map? query)
                 (let [[field value] (:term query)]
                   (TermQuery. (Term. (name field) (str value))))

                 :else
                 (MatchAllDocsQuery.))
             top-docs (.search searcher q (int limit))
             hits (.-scoreDocs top-docs)]
         (mapv (fn [^ScoreDoc sd]
                 (let [doc (.storedFields searcher)
                       stored (.document doc (.-doc sd))
                       field-map (into {}
                                       (map (fn [^IndexableField f]
                                              [(.name f) (.stringValue f)]))
                                       (.getFields stored))]
                   (assoc field-map
                          :doc-id (.-doc sd)
                          :score (.-score sd))))
               hits))))))

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
               (try
                 (let [bw (BranchIndexWriter/open (->path base-path) bname (StandardAnalyzer.))
                       snaps (mapv (fn [m]
                                     (let [base {:generation (.get m "generation")
                                                 :custom-metadata
                                                 (when-let [cm (.get m "customMetadata")]
                                                   (into {} cm))}]
                                       base))
                                   (.listSnapshots bw))]
                   (.close bw)
                   (assoc acc bname snaps))
                 (catch Exception _ acc)))
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
    (.mergeFrom tw sw)))

(defn close!
  "Close a branch writer and its resources."
  [sw]
  (let [^BranchIndexWriter writer (->writer sw)
        mi (->metadata-index sw)]
    (when mi
      (metadata/close-index! mi))
    (.close writer)
    ;; A store-backed writer holds a Directory the caller did not construct, so
    ;; closing the writer is not enough — `createOver` takes the Directory as
    ;; given and leaves its lifetime to whoever supplied it, which here is us.
    (when-let [^java.io.Closeable dir (:directory (:backing sw))]
      (.close dir))))
