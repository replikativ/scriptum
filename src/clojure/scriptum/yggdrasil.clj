(ns scriptum.yggdrasil
  "Yggdrasil adapter for scriptum COW indexes.

  Uses VALUE SEMANTICS: mutating operations (branch!, checkout, etc.)
  return new ScriptumSystem values. The underlying BranchIndexWriter
  instances are mutable (Lucene writers), but the system structure
  (which writers exist, which is current) is immutable.

  Snapshot IDs are UUIDs stored in commit user-data."
  (:require [scriptum.core :as pl]
            [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [clojure.string :as str])
  (:import [org.replikativ.scriptum BranchIndexWriter]))

(defn- parse-parent-ids
  "Parse comma-separated UUID string into a set of strings."
  [parent-str]
  (if (or (nil? parent-str) (str/blank? parent-str))
    #{}
    (set (str/split parent-str #","))))

(defn- find-generation-for-uuid
  "Find the commit generation for a given UUID by scanning a writer's snapshots."
  [^BranchIndexWriter writer uuid]
  (let [snaps (pl/list-snapshots writer)]
    (:generation (first (filter #(= (:snapshot-id %) uuid) snaps)))))

(defn- snapshot-index
  "Build a map of snapshot-id -> snapshot-info from a writer's snapshots."
  [^BranchIndexWriter writer]
  (into {}
    (map (fn [s]
           [(:snapshot-id s)
            (-> s
                (update :parent-ids parse-parent-ids))]))
    (pl/list-snapshots writer)))

(defn- all-snapshots
  "Collect snapshot maps from all writers, keyed by snapshot-id."
  [writers]
  (reduce-kv
    (fn [acc _branch-name writer]
      (merge acc (snapshot-index writer)))
    {}
    writers))

(defn- walk-ancestors
  "Walk parent chain from snap-id, collecting all ancestor IDs."
  [index snap-id]
  (loop [queue (vec (get-in index [snap-id :parent-ids]))
         visited #{}]
    (if (empty? queue)
      visited
      (let [current (peek queue)
            queue (pop queue)]
        (if (or (visited current) (nil? (get index current)))
          (recur queue visited)
          (let [parents (get-in index [current :parent-ids] #{})]
            (recur (into queue parents)
                   (conj visited current))))))))

(defn- find-common-ancestor
  "BFS from both sides to find the most recent common ancestor."
  [index a b]
  (let [ancestors-a (conj (walk-ancestors index a) a)]
    (loop [queue [b]
           visited #{}]
      (if (empty? queue)
        nil
        (let [current (first queue)
              queue (vec (rest queue))]
          (cond
            (visited current) (recur queue visited)
            (ancestors-a current) current
            :else
            (let [parents (vec (get-in index [current :parent-ids] #{}))]
              (recur (into queue parents)
                     (conj visited current)))))))))

(defrecord ScriptumSystem [base-path writers current-branch-name system-name]
  p/SystemIdentity
  (system-id [_] (or system-name (str "scriptum:" base-path)))
  (system-type [_] :scriptum)
  (capabilities [_]
    (t/->Capabilities true true true true false false))

  p/Snapshotable
  (snapshot-id [_]
    (let [writer (get writers current-branch-name)]
      (.getLastCommitId writer)))

  (parent-ids [_]
    (let [writer (get writers current-branch-name)
          snaps (pl/list-snapshots writer)
          latest (last snaps)]
      (if latest
        (parse-parent-ids (:parent-ids latest))
        #{})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (let [uuid (str snap-id)]
      ;; Find which writer has this UUID and open reader at its generation
      (some (fn [[_bname writer]]
              (when-let [gen (find-generation-for-uuid writer uuid)]
                (pl/open-reader-at writer gen)))
            writers)))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [index (all-snapshots writers)
          info (get index (str snap-id))]
      (when info
        {:snapshot-id (:snapshot-id info)
         :parent-ids (or (:parent-ids info) #{})
         :timestamp (:timestamp info)
         :message (:message info)
         :branch (:branch info)})))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (set (map keyword (keys writers))))

  (current-branch [_]
    (keyword current-branch-name))

  (branch! [this name]
    (let [branch-str (clojure.core/name name)
          writer (get writers current-branch-name)
          new-writer (pl/fork writer branch-str)]
      (assoc this :writers (assoc writers branch-str new-writer))))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    (let [branch-str (clojure.core/name name)
          from-str (str from)
          source-writer (or (get writers from-str) (get writers current-branch-name))
          new-writer (pl/fork source-writer branch-str)]
      (assoc this :writers (assoc writers branch-str new-writer))))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [branch-str (clojure.core/name name)]
      (when-let [writer (get writers branch-str)]
        (pl/close! writer))
      (assoc this :writers (dissoc writers branch-str))))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (let [branch-str (clojure.core/name name)]
      (when-not (contains? writers branch-str)
        (throw (ex-info (str "Branch not found: " branch-str)
                        {:branch branch-str
                         :available (keys writers)})))
      (assoc this :current-branch-name branch-str)))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [writer (get writers current-branch-name)
          snaps (pl/list-snapshots writer)
          snap-ids (mapv :snapshot-id snaps)
          ;; newest first
          snap-ids (vec (rseq snap-ids))
          snap-ids (if-let [since (:since opts)]
                     (vec (take-while #(not= % (str since)) snap-ids))
                     snap-ids)
          snap-ids (if-let [limit (:limit opts)]
                     (vec (take limit snap-ids))
                     snap-ids)]
      snap-ids))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [index (all-snapshots writers)]
      (vec (walk-ancestors index (str snap-id)))))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (let [index (all-snapshots writers)
          ancestors-b (walk-ancestors index (str b))]
      (contains? ancestors-b (str a))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (let [index (all-snapshots writers)]
      (find-common-ancestor index (str a) (str b))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _opts]
    (let [index (all-snapshots writers)
          branch-heads (into {}
                         (map (fn [[bname writer]]
                                [(keyword bname)
                                 (.getLastCommitId writer)]))
                         writers)]
      {:nodes (into {}
                (map (fn [[id info]]
                       [id {:parent-ids (:parent-ids info)
                            :meta (select-keys info [:timestamp :message :branch])}]))
                index)
       :branches branch-heads
       :roots (set (filter #(empty? (get-in index [% :parent-ids])) (keys index)))}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [_ snap-id _opts]
    (let [index (all-snapshots writers)
          info (get index (str snap-id))]
      (when info
        {:parent-ids (or (:parent-ids info) #{})
         :timestamp (:timestamp info)
         :message (:message info)
         :branch (:branch info)})))

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [this source opts]
    (let [source-str (str (clojure.core/name source))
          target-writer (get writers current-branch-name)
          source-writer (get writers source-str)]
      (when-not source-writer
        (throw (ex-info (str "Source branch not found: " source-str)
                        {:source source-str})))
      (pl/merge-from! target-writer source-writer)
      ;; Return system — merge mutated the writer, snapshot-id now reflects merge
      this))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ a b _opts]
    ;; Scriptum uses add-only merge, no structural conflicts
    [])

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    (let [index (all-snapshots writers)
          info-a (get index (str a))
          info-b (get index (str b))]
      {:snapshot-a (str a)
       :snapshot-b (str b)
       :meta-a (select-keys info-a [:timestamp :message :branch])
       :meta-b (select-keys info-b [:timestamp :message :branch])})))

(defn create
  "Create a ScriptumSystem at the given path.

  Creates a 'main' branch and returns the system.
  Options:
    :system-name - identifier for this system instance
    :analyzer    - Lucene Analyzer (default: StandardAnalyzer)"
  ([^String path] (create path {}))
  ([^String path opts]
   (let [writer (pl/create-index path "main" (select-keys opts [:analyzer]))]
     (->ScriptumSystem
       path
       {"main" writer}
       "main"
       (:system-name opts)))))

(defn close!
  "Close the system and all its branch writers."
  [^ScriptumSystem sys]
  (doseq [[_name writer] (:writers sys)]
    (try (pl/close! writer) (catch Exception _))))
