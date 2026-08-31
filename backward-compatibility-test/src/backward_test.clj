(ns backward-test
  "Cross-release persistence fixture; loaded only by the compatibility script."
  (:require [konserve.filestore]
            [konserve.store :as kstore]
            [scriptum.core :as scriptum]))

(def ^:private store-id
  #uuid "8f7ba44c-13bf-45e9-abbe-a0959c4ba682")

(def ^:private index-id
  #uuid "ea0a18a7-7077-4f7f-846e-e1f2e4c625dc")

(defn- store-config []
  {:backend :file
   :path (str (System/getenv "BACK_COMPAT_ROOT") "/store")
   :id store-id})

(defn- cache-path []
  (str (System/getenv "BACK_COMPAT_ROOT") "/"
       (System/getenv "BACK_COMPAT_CACHE")))

(defn- ids [writer]
  (set (map #(get % "id")
            (scriptum/search writer :all {:limit 100}))))

(defn write [_]
  (let [store (kstore/create-store (store-config) {:sync? true})
        main (scriptum/open-store-index store (cache-path) "main"
                                        {:store-id index-id})]
    (try
      (scriptum/add-doc main {:id {:type :string :value "base"}
                              :body {:type :text :value "released brown fox"}})
      (scriptum/commit! main "release base")
      (let [feature (scriptum/fork main "feature")]
        (try
          (scriptum/add-doc feature {:id {:type :string :value "feature"}
                                     :body {:type :text :value "branch only"}})
          (scriptum/commit! feature "release feature")
          (finally
            (scriptum/close! feature))))
      (finally
        (scriptum/close! main)))))

(defn verify [_]
  (let [store (kstore/connect-store (store-config) {:sync? true})
        main (scriptum/open-store-index store (cache-path) "main")
        feature (scriptum/open-store-index store (cache-path) "feature")]
    (try
      (assert (= #{"base"} (ids main)))
      (assert (= #{"base" "feature"} (ids feature)))
      (assert (= 1 (count (scriptum/search main
                                           (scriptum/text-query :body "fox")))))
      (scriptum/add-doc main {:id {:type :string :value "current"}
                              :body {:type :text :value "written by current"}})
      (scriptum/commit! main "current append")
      (finally
        (scriptum/close! feature)
        (scriptum/close! main))))
  (let [store (kstore/connect-store (store-config) {:sync? true})
        main (scriptum/open-store-index store (cache-path) "main")
        feature (scriptum/open-store-index store (cache-path) "feature")]
    (try
      (assert (= #{"base" "current"} (ids main)))
      (assert (= #{"base" "feature"} (ids feature)))
      (finally
        (scriptum/close! feature)
        (scriptum/close! main)))))
