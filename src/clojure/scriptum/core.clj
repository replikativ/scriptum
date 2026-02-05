(ns scriptum.core
  "COW branching semantics on top of Apache Lucene.

  Provides fast forking (~3-5ms), structural sharing of immutable segments,
  branch-isolated indexing/searching, snapshot retention, and explicit GC.

  Key concepts:
  - Writer: mutable handle to a branch (one per branch per JVM)
  - Snapshot: immutable DirectoryReader at a specific commit point
  - Branch: COW overlay sharing base segments with the trunk
  - GC: explicit cleanup of old snapshots respecting branch references"
  (:import [java.nio.file Path Paths]
           [java.time Instant Duration]
           [org.apache.lucene.analysis Analyzer]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.document Document Field$Store TextField StringField
                                       KnnFloatVectorField]
           [org.apache.lucene.index DirectoryReader IndexableField Term]
           [org.apache.lucene.search IndexSearcher TermQuery BooleanQuery
                                     BooleanClause$Occur TopDocs ScoreDoc
                                     MatchAllDocsQuery KnnFloatVectorQuery]
           [org.apache.lucene.store FSDirectory]
           [org.replikativ.scriptum BranchIndexWriter BranchedDirectory]))

(defn- ->path
  "Convert a string to a java.nio.file.Path."
  ^Path [^String s]
  (Paths/get s (make-array String 0)))

;; --- Index Lifecycle ---

(defn create-index
  "Create a new branched index at the given path.

  On creation, discovers existing branches and protects their shared segments.

  Options:
    :analyzer - the Lucene Analyzer to use (default: StandardAnalyzer)
    :crypto-hash? - enable merkle hashing for commits (default: false)

  Returns a BranchIndexWriter for the given branch."
  ([^String path ^String branch-name]
   (create-index path branch-name {}))
  ([^String path ^String branch-name {:keys [analyzer crypto-hash?]}]
   (let [base-path (->path path)
         analyzer (or analyzer (StandardAnalyzer.))
         crypto-hash (boolean crypto-hash?)]
     (BranchIndexWriter/create base-path branch-name analyzer crypto-hash))))

(defn open-branch
  "Open an existing branch writer (for out-of-process branch access).

  Opens a BranchedDirectory with the base as read-only and overlay for writes.

  Options:
    :analyzer - the Lucene Analyzer to use (default: StandardAnalyzer)"
  ([^String path ^String branch-name]
   (open-branch path branch-name {}))
  ([^String path ^String branch-name {:keys [analyzer]}]
   (let [base-path (->path path)
         analyzer (or analyzer (StandardAnalyzer.))]
     (BranchIndexWriter/open base-path branch-name analyzer))))

(defn fork
  "Fork the index into a new branch. Returns the new branch writer.

  The new branch shares all existing segments with the parent.
  Cost: ~3-5ms (flush buffer + copy manifest)."
  ^BranchIndexWriter [^BranchIndexWriter writer ^String new-branch-name]
  (.fork writer new-branch-name))

(defn discover-branches
  "Discover all branch names at the given path.

  Returns a set of branch name strings."
  [^String path]
  (BranchIndexWriter/discoverBranches (->path path)))

;; --- Document Operations ---

(defn add-doc
  "Add a document to the branch.

  doc-map is a map of field-name -> value.
  Options per field can be specified as {:value v :stored? bool :type :text|:string|:vector}

  Simple usage:
    (add-doc writer {:title \"Hello World\" :body \"Some text\"})

  Advanced usage:
    (add-doc writer {:id {:value \"doc1\" :type :string}
                     :title {:value \"Hello\" :type :text :stored? true}
                     :embedding {:value (float-array [0.1 0.2 ...]) :type :vector}})"
  [^BranchIndexWriter writer doc-map]
  (let [doc (Document.)]
    (doseq [[field-name value-or-opts] doc-map]
      (let [fname (name field-name)
            {:keys [value stored? type]
             :or {stored? true type :text}} (if (map? value-or-opts)
                                              value-or-opts
                                              {:value (str value-or-opts)})
            store (if stored? Field$Store/YES Field$Store/NO)]
        (case type
          :text (.add doc (TextField. fname (str value) store))
          :string (.add doc (StringField. fname (str value) store))
          :vector (.add doc (KnnFloatVectorField. fname ^floats value)))))
    (.addDocument writer doc)))

(defn delete-docs
  "Delete documents matching the given term field and value."
  [^BranchIndexWriter writer ^String field ^String value]
  (.deleteDocuments writer (into-array Term [(Term. field value)])))

(defn update-doc
  "Update a document identified by the given term.

  Replaces the document matching (field, value) with the new doc-map."
  [^BranchIndexWriter writer ^String field ^String value doc-map]
  (let [doc (Document.)]
    (doseq [[fname fval] doc-map]
      (.add doc (TextField. (name fname) (str fval) Field$Store/YES)))
    (.updateDocument writer (Term. field value) doc)))

;; --- Commit & Sync ---

(defn commit!
  "Commit changes on a branch. Stores timestamp in commit user-data.

  Optional message is stored for history/log purposes.

  Returns a map with:
    :generation - the commit generation number
    :commit-id - Lucene's internal commit UUID
    :content-hash - content-addressable merkle root (only when :crypto-hash? enabled)

  When :crypto-hash? is not enabled, returns just the generation number for backward compatibility."
  ([^BranchIndexWriter writer]
   (commit! writer nil))
  ([^BranchIndexWriter writer ^String message]
   (let [gen (if message
               (.commit writer message)
               (.commit writer))
         commit-id (.getLastCommitId writer)
         content-hash (.getLastContentHash writer)]
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
  ([^BranchIndexWriter writer]
   (verify-commit writer {}))
  ([^BranchIndexWriter writer {:keys [generation] :or {generation -1}}]
   (let [result (.verifyCommit writer (long generation))]
     {:valid? (.get result "valid")
      :commit-id (.get result "commitId")
      :errors (vec (.get result "errors"))})))

(defn flush!
  "Flush pending changes without committing (no durability, but NRT visible)."
  [^BranchIndexWriter writer]
  (.flush writer))

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
  ([^BranchIndexWriter writer query]
   (search writer query {}))
  ([^BranchIndexWriter writer query {:keys [limit fields] :or {limit 10}}]
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
             hits)))))

;; --- Snapshots & Time-Travel ---

(defn list-snapshots
  "List all available snapshots (commit points) for this branch.

  Returns a vector of maps with :generation, :snapshot-id, :timestamp,
  :message, :branch, :segment-count, and :parent-ids."
  [^BranchIndexWriter writer]
  (mapv (fn [m]
          {:generation (.get m "generation")
           :snapshot-id (.get m "snapshotId")
           :segment-count (.get m "segmentCount")
           :timestamp (.get m "timestamp")
           :message (.get m "message")
           :branch (.get m "branch")
           :parent-ids (.get m "parentIds")})
        (.listSnapshots writer)))

(defn open-reader-at
  "Open a reader at a specific commit generation (time-travel).

  The caller is responsible for closing the reader.
  Throws if the generation has been GC'd."
  ^DirectoryReader [^BranchIndexWriter writer ^long generation]
  (.openReaderAt writer generation))

(defn commit-available?
  "Check if a specific commit generation is still available (not GC'd)."
  [^BranchIndexWriter writer ^long generation]
  (.isCommitAvailable writer generation))

(defn snapshot
  "Take an immutable snapshot (DirectoryReader) of the current branch.
   The caller is responsible for closing the reader."
  ^DirectoryReader [^BranchIndexWriter writer]
  (.openReader writer))

(defn with-snapshot
  "Execute f with an immutable snapshot reader. Reader is closed after."
  [^BranchIndexWriter writer f]
  (with-open [reader (snapshot writer)]
    (f reader)))

;; --- GC ---

(defn gc!
  "Garbage collect old commit points and unreferenced segment files.

  Only callable on the main branch writer. Scans all branches to determine
  which files are still needed before removing anything.

  before: java.time.Instant — delete commits older than this
  Returns the number of commit points removed."
  [^BranchIndexWriter writer ^Instant before]
  (.gc writer before))

;; --- Accessors ---

(defn num-docs
  "Returns the number of documents in this branch (excluding deletions)."
  [^BranchIndexWriter writer]
  (.numDocs writer))

(defn max-doc
  "Returns the total number of documents (including deletions)."
  [^BranchIndexWriter writer]
  (.maxDoc writer))

(defn branch-name
  "Returns the branch name."
  [^BranchIndexWriter writer]
  (.getBranchName writer))

(defn base-path
  "Returns the base path of the index."
  [^BranchIndexWriter writer]
  (str (.getBasePath writer)))

(defn main-branch?
  "Returns true if this is the main (trunk) branch."
  [^BranchIndexWriter writer]
  (.isMainBranch writer))

(defn merge-from!
  "Merge segments from a source branch into this branch.

  Uses reader-based addIndexes to avoid lock conflicts with source writer."
  [^BranchIndexWriter target ^BranchIndexWriter source]
  (.mergeFrom target source))

(defn close!
  "Close a branch writer and its resources."
  [^BranchIndexWriter writer]
  (.close writer))
