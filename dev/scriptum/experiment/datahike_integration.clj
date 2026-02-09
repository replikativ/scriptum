(ns scriptum.experiment.datahike-integration
  "Experiment: Scriptum as a secondary full-text index for Datahike.

  Demonstrates:
  1. Creating a Datahike database alongside a Scriptum index
  2. Syncing Datahike transactions to Scriptum with T-basis tracking
  3. Storing the Datahike transaction basis (T) in Scriptum commit metadata
  4. Time-travel: querying both systems at a consistent T

  Evaluate the forms in the comment block at the bottom step by step."
  (:require [datahike.api :as d]
            [scriptum.core :as sc])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; ============================================================================
;; Setup helpers
;; ============================================================================

(defn tmp-dir
  "Create a temp directory that will be deleted on JVM shutdown."
  [prefix]
  (let [dir (Files/createTempDirectory prefix (make-array FileAttribute 0))]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (try
                                   (let [walker (Files/walk dir (make-array java.nio.file.FileVisitOption 0))]
                                     (doseq [p (reverse (vec (.iterator walker)))]
                                       (Files/deleteIfExists p)))
                                   (catch Exception _)))))
    (str dir)))

(def schema
  "Datahike schema: articles with title, body (searchable text), and category."
  [{:db/ident :article/title
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/body
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :article/category
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

;; Attributes whose values should be full-text indexed in Scriptum
(def searchable-attrs #{:article/title :article/body})

;; ============================================================================
;; Secondary index sync
;; ============================================================================

(defn index-tx-report!
  "Process a Datahike TxReport and sync changes to Scriptum.

  For each datom in :tx-data:
  - If the attribute is in searchable-attrs and the datom is added,
    index (or update) the entity's text in Scriptum.
  - Commits the Scriptum index with the Datahike tx as custom metadata.

  This is the core of the secondary index pattern."
  [sc-writer tx-report]
  (let [{:keys [tx-data db-after]} tx-report
        tx (:max-tx db-after)
        ;; Collect entities that have searchable attribute changes
        entities-to-index (reduce (fn [acc datom]
                                    (let [e (.-e datom)
                                          a (.-a datom)
                                          added? (pos? (.-tx datom))]
                                      (if (and added? (searchable-attrs a))
                                        (conj acc e)
                                        acc)))
                                  #{}
                                  tx-data)]
    (when (seq entities-to-index)
      ;; For each entity with searchable changes, pull current values and re-index
      (doseq [eid entities-to-index]
        (let [entity (d/pull db-after '[*] eid)
              doc-fields (cond-> {:eid {:type :string :value (str eid)}}
                           (:article/title entity)
                           (assoc :title {:type :text :value (:article/title entity)})
                           (:article/body entity)
                           (assoc :body {:type :text :value (:article/body entity)})
                           (:article/category entity)
                           (assoc :category {:type :string :value (:article/category entity)}))]
          ;; Delete old doc for this entity (if exists), then add new
          (sc/delete-docs sc-writer "eid" (str eid))
          (sc/add-doc sc-writer doc-fields)))
      ;; Commit with the Datahike transaction basis
      (sc/commit! sc-writer
                  (str "Sync datahike tx " tx)
                  {"datahike/tx" (str tx)}))
    tx))

;; ============================================================================
;; Time-travel: find Scriptum generation for a given Datahike T
;; ============================================================================

(defn generation-for-tx
  "Find the Scriptum generation that corresponds to a given Datahike T.

  Uses Scriptum's in-memory metadata index for O(log n) lookup.
  Returns {:generation g :indexed-tx t}, or nil if not caught up."
  [sc-writer datahike-tx]
  (when-let [{:keys [generation indexed-value]}
             (sc/find-generation sc-writer "datahike/tx" (str datahike-tx) :floor)]
    {:generation generation
     :indexed-tx (Long/parseLong indexed-value)}))

(defn search-at-tx
  "Search the Scriptum index as it was at Datahike transaction T.

  Finds the Scriptum commit that corresponds to T, opens a time-travel
  reader at that generation, and runs the search query."
  [sc-writer datahike-tx query & [{:keys [limit] :or {limit 10}}]]
  (if-let [{:keys [generation indexed-tx]} (generation-for-tx sc-writer datahike-tx)]
    (with-open [reader (sc/open-reader-at sc-writer generation)]
      (let [searcher (org.apache.lucene.search.IndexSearcher. reader)
            q (cond
                (instance? org.apache.lucene.search.Query query) query
                (= :all query) (org.apache.lucene.search.MatchAllDocsQuery.)
                (map? query) (let [[field value] (:term query)]
                               (org.apache.lucene.search.TermQuery.
                                (org.apache.lucene.index.Term. (name field) (str value))))
                :else (org.apache.lucene.search.MatchAllDocsQuery.))
            hits (.search searcher q (int limit))]
        {:indexed-tx indexed-tx
         :generation generation
         :results (mapv (fn [^org.apache.lucene.search.ScoreDoc sd]
                          (let [stored (.document (.storedFields searcher) (.-doc sd))]
                            (into {:score (.-score sd)}
                                  (map (fn [^org.apache.lucene.index.IndexableField f]
                                         [(.name f) (.stringValue f)]))
                                  (.getFields stored))))
                        (.-scoreDocs hits))}))
    {:error (str "No Scriptum commit found for datahike tx " datahike-tx)}))

;; ============================================================================
;; REPL walkthrough — evaluate each form step by step
;; ============================================================================

(comment

  ;; --- 1. Setup: create Datahike DB and Scriptum index ---

  (def base (tmp-dir "scriptum-dh-experiment"))

  (def dh-cfg {:store {:backend :file :path (str base "/datahike")
                        :id (java.util.UUID/randomUUID)}
               :keep-history? true
               :schema-flexibility :write
               :initial-tx schema})

  (d/create-database dh-cfg)
  (def conn (d/connect dh-cfg))

  (def sc-writer (sc/create-index (str base "/scriptum") "main"))

  ;; --- 2. First batch: programming & database articles ---

  (def tx1 @(d/transact! conn
                          {:tx-data [{:article/title "Introduction to Clojure"
                                      :article/body "Clojure is a dynamic functional programming language on the JVM"
                                      :article/category "programming"}
                                     {:article/title "Getting Started with Datahike"
                                      :article/body "Datahike is an immutable database with Datalog queries and git-like semantics"
                                      :article/category "databases"}]}))

  (def t1 (:max-tx (:db-after tx1)))
  t1 ;; => 536870914

  ;; Sync this transaction to Scriptum
  (index-tx-report! sc-writer tx1)

  ;; --- 3. Second batch: search-related articles ---

  (def tx2 @(d/transact! conn
                          {:tx-data [{:article/title "Apache Lucene Internals"
                                      :article/body "Lucene uses inverted indices with immutable segment files for full-text search"
                                      :article/category "search"}
                                     {:article/title "Scriptum: Branching for Lucene"
                                      :article/body "Scriptum brings copy-on-write branching to Apache Lucene indices"
                                      :article/category "search"}]}))

  (def t2 (:max-tx (:db-after tx2)))
  t2 ;; => 536870915

  (index-tx-report! sc-writer tx2)

  ;; --- 4. Inspect: Scriptum snapshots now carry the Datahike T ---

  (sc/list-snapshots sc-writer)
  ;; [{:generation 1, :custom-metadata {"datahike/tx" "536870914"}, ...}
  ;;  {:generation 2, :custom-metadata {"datahike/tx" "536870915"}, ...}]

  ;; --- 5. Full-text search on current state ---

  (sc/search sc-writer {:term [:body "lucene"]})
  ;; => two results: "Apache Lucene Internals" and "Scriptum: Branching for Lucene"

  (sc/search sc-writer {:term [:body "clojure"]})
  ;; => one result: "Introduction to Clojure"

  ;; --- 6. Time-travel: search as-of T1 (before Lucene articles existed) ---

  (search-at-tx sc-writer t1 {:term [:body "lucene"]})
  ;; => {:results []} — correct, Lucene articles weren't added until t2

  (search-at-tx sc-writer t1 :all)
  ;; => 2 docs (Clojure + Datahike articles)

  ;; --- 7. Time-travel: search as-of T2 (after Lucene articles) ---

  (search-at-tx sc-writer t2 {:term [:body "lucene"]})
  ;; => 2 results

  (search-at-tx sc-writer t2 :all)
  ;; => 4 docs total

  ;; --- 8. Cross-system consistency ---

  ;; Datahike entity count at each T
  (count (d/q '[:find ?e :where [?e :article/title _]]
              (d/as-of @conn t1)))
  ;; => 2

  (count (d/q '[:find ?e :where [?e :article/title _]]
              (d/as-of @conn t2)))
  ;; => 4

  ;; Scriptum doc count at each T (should match)
  (count (:results (search-at-tx sc-writer t1 :all)))
  ;; => 2

  (count (:results (search-at-tx sc-writer t2 :all)))
  ;; => 4

  ;; --- 9. T-basis: O(log n) lookup ---

  ;; Find which Scriptum generation corresponds to a Datahike T
  (generation-for-tx sc-writer t1)
  ;; => {:generation 1, :indexed-tx 536870914}

  (generation-for-tx sc-writer t2)
  ;; => {:generation 2, :indexed-tx 536870915}

  ;; Current sync status
  (let [current-tx (:max-tx @conn)
        latest     (generation-for-tx sc-writer Long/MAX_VALUE)]
    {:datahike-tx current-tx
     :scriptum-indexed-tx (:indexed-tx latest)
     :in-sync? (= current-tx (:indexed-tx latest))})
  ;; => {:datahike-tx 536870915, :scriptum-indexed-tx 536870915, :in-sync? true}

  ;; --- 10. Cleanup ---

  (sc/close! sc-writer)
  (d/release conn)
  (d/delete-database dh-cfg)

  )
