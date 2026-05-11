(ns scriptum.audit-test
  "Tests for `scriptum.audit/verify-chain` and the `IAuditable` protocol.

   Mirrors the test surface of `stratum.audit-test` and
   `datahike.test.audit-verify-test` so the three audit shapes stay
   coordinated.

     - clean walk reports `:ok`
     - tampering a segment file is detected by `verify-chain` and the
       protocol's `-recompute-merkle-root` (returns `:status :mismatch`)
     - `:crypto-hash?` off → `:advisory` with reason
       `:crypto-hash-disabled`
     - `IAuditable` on a live `ScriptumWriter`: `-merkle-root` returns
       the cached content-hash; `-recompute-merkle-root` returns `:ok`
       on a clean store, `:mismatch` after tampering"
  (:require [clojure.test :refer [deftest is testing]]
            [scriptum.audit :as audit]
            [scriptum.core :as sc])
  (:import [java.util UUID]))

(set! *warn-on-reflection* true)

(defn- temp-dir []
  (let [d (java.io.File/createTempFile "scriptum-audit-" "")]
    (.delete d)
    (.mkdirs d)
    (.getAbsolutePath d)))

(defn- delete-dir-recursive [path]
  (let [d (java.io.File. ^String path)]
    (when (.exists d)
      (doseq [f (reverse (file-seq d))]
        (.delete f)))))

(defn- bootstrap-clean
  "Create a writer at a fresh temp dir under :crypto-hash?, write a few
   commits, return [writer path]."
  []
  (let [path (temp-dir)
        w (sc/create-index path "main" {:crypto-hash? true})]
    (sc/add-doc w {:id {:type :string :value "doc-1"}
                   :content {:type :text :value "first commit"}})
    (sc/commit! w "first")
    (sc/add-doc w {:id {:type :string :value "doc-2"}
                   :content {:type :text :value "second commit"}})
    (sc/commit! w "second")
    [w path]))

(defn- tamper-first-segment-file!
  "Corrupt the first .cfs segment file in `path` by appending a byte."
  [path]
  (let [dir (java.io.File. ^String path)
        cfs (first (filter #(.endsWith (.getName ^java.io.File %) ".cfs")
                           (.listFiles dir)))]
    (assert cfs "expected at least one .cfs segment file")
    (with-open [os (java.io.FileOutputStream. ^java.io.File cfs true)]
      (.write os (byte-array [42])))
    cfs))

;; ============================================================================
;; verify-chain — chain walk over Lucene generations
;; ============================================================================

(deftest clean-chain-verifies-ok
  (let [[w path] (bootstrap-clean)]
    (try
      (let [r (audit/verify-chain w)]
        (is (= :ok (:status r)))
        (is (audit/ok? r))
        (is (= 2 (count (:commits r)))
            "two commits, both verified")
        (is (every? #{:ok} (map :status (:commits r))))
        (is (some? (:head r))))
      (finally
        (sc/close! w)
        (delete-dir-recursive path)))))

(deftest tamper-on-segment-file-is-detected
  (let [[w path] (bootstrap-clean)]
    (try
      (tamper-first-segment-file! path)
      (let [r (audit/verify-chain w)
            mism (filterv #(= :mismatch (:status %)) (:commits r))]
        (is (= :mismatch (:status r)))
        (is (false? (audit/ok? r)))
        (is (pos? (count mism))
            "at least one commit should fail")
        (is (= :audit/merkle-mismatch
               (-> mism first :errors first :type))))
      (finally
        (sc/close! w)
        (delete-dir-recursive path)))))

(deftest no-crypto-hash-is-advisory
  (let [path (temp-dir)
        w (sc/create-index path "main" {:crypto-hash? false})]
    (try
      (sc/add-doc w {:id {:type :string :value "x"}
                     :content {:type :text :value "data"}})
      (sc/commit! w "advisory")
      (let [r (audit/verify-chain w)]
        (is (= :advisory (:status r)))
        (is (every? #{:unsupported} (map :status (:commits r))))
        (is (every? #{:crypto-hash-disabled} (map :reason (:commits r)))))
      (finally
        (sc/close! w)
        (delete-dir-recursive path)))))

;; ============================================================================
;; IAuditable protocol on a live ScriptumWriter
;; ============================================================================

(deftest iauditable-clean
  (let [[w path] (bootstrap-clean)]
    (try
      (let [root (audit/-merkle-root w)
            r (audit/-recompute-merkle-root w)]
        (is (uuid? root))
        (is (= :ok (:status r)))
        (is (= root (:root r))
            "recomputed root must equal -merkle-root on a clean store"))
      (finally
        (sc/close! w)
        (delete-dir-recursive path)))))

(deftest iauditable-detects-tampering
  (let [[w path] (bootstrap-clean)]
    (try
      (tamper-first-segment-file! path)
      (let [r (audit/-recompute-merkle-root w)
            err (-> r :errors first)]
        (is (= :mismatch (:status r)))
        (is (= :audit/merkle-mismatch (:type err)))
        (is (uuid? (:address err))))
      (finally
        (sc/close! w)
        (delete-dir-recursive path)))))

(deftest iauditable-no-crypto-hash
  (let [path (temp-dir)
        w (sc/create-index path "main" {:crypto-hash? false})]
    (try
      (sc/add-doc w {:id {:type :string :value "x"}
                     :content {:type :text :value "data"}})
      (sc/commit! w "advisory")
      (is (nil? (audit/-merkle-root w))
          "no content-hash without :crypto-hash?")
      (let [r (audit/-recompute-merkle-root w)]
        (is (= :unsupported (:status r)))
        (is (= :crypto-hash-disabled (:reason r))))
      (finally
        (sc/close! w)
        (delete-dir-recursive path)))))
