(ns scriptum.metadata
  "Durable metadata index backed by persistent-sorted-set + konserve.

  Provides O(log n) lookup of commit generations by custom metadata keys,
  with lazy loading from disk (O(1) startup) and incremental updates.

  Each entry is {:branch b :key k :value v :generation g}.
  Sorted by [:branch :key :value] enabling efficient exact and floor queries."
  (:require [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store]]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [org.replikativ.persistent_sorted_set ANode Branch IStorage Leaf Settings]))

;; ============================================================================
;; Comparator
;; ============================================================================

(def metadata-comparator
  "Comparator for metadata entries: [:branch :key :value]"
  (reify java.util.Comparator
    (compare [_ a b]
      (let [c1 (compare (:branch a) (:branch b))]
        (if (not= c1 0)
          c1
          (let [c2 (compare (:key a) (:key b))]
            (if (not= c2 0)
              c2
              (compare (:value a) (:value b)))))))))

;; ============================================================================
;; Serialization boundary — plain maps only (no records in konserve)
;; ============================================================================

(defn- entry->map [e]
  (if (map? e)
    (select-keys e [:branch :key :value :generation])
    e))

(defn- map->entry [m]
  (if (map? m)
    (select-keys m [:branch :key :value :generation])
    m))

;; ============================================================================
;; IStorage implementation backed by konserve
;; ============================================================================

(defrecord ScriptumMetadataStorage [kv-store ^Settings settings cache freed-atom]
  IStorage
  (store [_ node]
    (let [^ANode node node
          address (random-uuid)
          node-data {:level     (.level node)
                     :keys      (mapv entry->map (.keys node))
                     :addresses (when (instance? Branch node)
                                  (vec (.addresses ^Branch node)))}]
      (<!! (k/assoc kv-store address node-data))
      (swap! cache assoc address node)
      address))

  (restore [_ address]
    (or (get @cache address)
        (let [node-data (<!! (k/get kv-store address))
              raw-keys (:keys node-data)
              keys (mapv map->entry raw-keys)
              addresses (:addresses node-data)
              node (if addresses
                     (Branch. (int (:level node-data))
                              ^java.util.List keys
                              ^java.util.List (vec addresses)
                              settings)
                     (Leaf. ^java.util.List keys settings))]
          (swap! cache assoc address node)
          node)))

  (accessed [_ _address] nil)

  (markFreed [_ address]
    (when address
      (swap! freed-atom assoc address (System/currentTimeMillis))))

  (isFreed [_ address]
    (contains? @freed-atom address))

  (freedInfo [_ address]
    (get @freed-atom address)))

;; ============================================================================
;; Store / Storage lifecycle
;; ============================================================================

(defn- create-store [path]
  (let [dir (java.io.File. (str path))]
    (when-not (.exists dir)
      (.mkdirs dir)))
  (<!! (connect-fs-store (str path))))

(defn- create-storage
  ([kv-store]
   (create-storage kv-store (Settings.)))
  ([kv-store ^Settings settings]
   (->ScriptumMetadataStorage kv-store settings (atom {}) (atom {}))))

(defn- save-roots! [kv-store roots]
  (<!! (k/assoc kv-store :metadata/roots roots)))

(defn- load-roots [kv-store]
  (<!! (k/get kv-store :metadata/roots)))

(defn- save-freed! [kv-store freed]
  (<!! (k/assoc kv-store :metadata/freed freed)))

(defn- load-freed [kv-store]
  (or (<!! (k/get kv-store :metadata/freed)) {}))

;; ============================================================================
;; MetadataIndex record
;; ============================================================================

(defrecord MetadataIndex [index-atom kv-store storage dirty-atom])

(defn create-metadata-index
  "Create or restore a metadata index at base-path/scriptum-metadata/.
  Returns a MetadataIndex record."
  [^String base-path]
  (let [store-path (str base-path "/scriptum-metadata")
        kv-store   (create-store store-path)
        storage    (create-storage kv-store)
        roots      (load-roots kv-store)
        freed      (load-freed kv-store)
        _          (reset! (:freed-atom storage) freed)]
    (if roots
      ;; Restore from existing root
      (let [idx (pss/restore-by metadata-comparator (:index roots) storage
                                {:branching-factor 64})]
        (->MetadataIndex (atom idx) kv-store storage (atom false)))
      ;; Fresh index with storage
      (let [idx (into (pss/sorted-set* {:comparator metadata-comparator
                                        :storage storage
                                        :branching-factor 64})
                      [])]
        (->MetadataIndex (atom idx) kv-store storage (atom false))))))

;; ============================================================================
;; CRUD operations
;; ============================================================================

(defn index!
  "Index custom metadata entries for a commit.
  branch-name: string, custom-metadata: map of key->value strings, generation: long."
  [^MetadataIndex mi branch-name custom-metadata generation]
  (when (and custom-metadata (seq custom-metadata))
    (let [entries (for [[k v] custom-metadata
                        :when (not (.startsWith ^String k "scriptum."))]
                    {:branch branch-name :key k :value v :generation generation})]
      (swap! (:index-atom mi)
             (fn [idx]
               (reduce (fn [s entry]
                         (pss/conj s entry metadata-comparator))
                       idx entries)))
      (reset! (:dirty-atom mi) true))))

(defn find-exact
  "Find exact match for branch+key+value. Returns {:generation n} or nil."
  [^MetadataIndex mi branch key value]
  (let [idx @(:index-atom mi)
        probe {:branch branch :key key :value value :generation 0}
        results (pss/slice idx probe probe metadata-comparator)]
    (when-let [entry (first results)]
      (when (and (= (:branch entry) branch)
                 (= (:key entry) key)
                 (= (:value entry) value))
        {:generation (:generation entry)}))))

(defn find-floor
  "Find floor match: latest entry where value <= target for branch+key.
  Returns {:generation n :indexed-value v} or nil."
  [^MetadataIndex mi branch key value]
  (let [idx @(:index-atom mi)
        from {:branch branch :key key :value "" :generation 0}
        to   {:branch branch :key key :value value :generation Long/MAX_VALUE}
        results (pss/slice idx from to metadata-comparator)]
    (when-let [entry (last (seq results))]
      (when (and (= (:branch entry) branch)
                 (= (:key entry) key))
        {:generation (:generation entry)
         :indexed-value (:value entry)}))))

(defn rebuild-from-snapshots!
  "Rebuild the metadata index from surviving snapshots after GC.
  snapshots-by-branch: map of branch-name -> seq of snapshot maps
  (each with :custom-metadata and :generation)."
  [^MetadataIndex mi snapshots-by-branch]
  (let [storage (:storage mi)
        fresh-idx (into (pss/sorted-set* {:comparator metadata-comparator
                                          :storage storage
                                          :branching-factor 64})
                        [])]
    (reset! (:index-atom mi)
            (reduce
             (fn [idx [branch-name snapshots]]
               (reduce
                (fn [idx {:keys [custom-metadata generation]}]
                  (if (and custom-metadata (seq custom-metadata))
                    (reduce
                     (fn [idx [k v]]
                       (if (.startsWith ^String k "scriptum.")
                         idx
                         (pss/conj idx
                                   {:branch branch-name :key k :value v :generation generation}
                                   metadata-comparator)))
                     idx
                     custom-metadata)
                    idx))
                idx
                snapshots))
             fresh-idx
             snapshots-by-branch))
    (reset! (:dirty-atom mi) true)))

(defn flush-index!
  "Persist the PSS index to konserve."
  [^MetadataIndex mi]
  (when @(:dirty-atom mi)
    (let [storage (:storage mi)
          kv-store (:kv-store mi)
          root (pss/store @(:index-atom mi) storage)]
      (save-roots! kv-store {:index root})
      (save-freed! kv-store @(:freed-atom storage))
      (reset! (:dirty-atom mi) false))))

(defn close-index!
  "Flush and close the metadata index store."
  [^MetadataIndex mi]
  (flush-index! mi)
  ;; konserve filestore implements Closeable
  (when-let [store (:kv-store mi)]
    (when (instance? java.io.Closeable store)
      (.close ^java.io.Closeable store))))
