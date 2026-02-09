(ns scriptum.yggdrasil-test
  "Yggdrasil compliance tests for scriptum adapter."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [scriptum.yggdrasil :as sy]
            [scriptum.core :as pl]
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as p])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [org.replikativ.scriptum BranchIndexWriter]))

(defn- temp-dir []
  (str (Files/createTempDirectory "scriptum-test-"
                                  (make-array FileAttribute 0))))

(defn- delete-dir-recursive [path]
  (let [dir (java.io.File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(defn- make-fixture
  "Create a compliance test fixture for a fresh scriptum system.
   All fixture functions return the (possibly updated) system value."
  []
  {:create-system (fn []
                    (let [path (temp-dir)]
                      (sy/create path {:system-name "test-scriptum"})))
   :mutate (fn [sys]
             (let [writer (get (:writers sys) (:current-branch-name sys))]
               (pl/add-doc writer {:content (str "doc-" (System/nanoTime))})
               sys))
   :commit (fn [sys msg]
             (let [writer (get (:writers sys) (:current-branch-name sys))]
               (pl/commit! writer msg)
               sys))
   :close! (fn [sys]
             (sy/close! sys))
   ;; Data consistency operations
   :write-entry (fn [sys key value]
                  (let [writer (get (:writers sys) (:current-branch-name sys))]
                    (pl/add-doc writer {:_key {:value key :type :string :stored? true}
                                        :_value {:value value :type :text :stored? true}})
                    sys))
   :read-entry (fn [sys key]
                 (let [writer (get (:writers sys) (:current-branch-name sys))
                       results (pl/search writer {:term [:_key key]})]
                   (get (first results) "_value")))
   :count-entries (fn [sys]
                    (let [writer (get (:writers sys) (:current-branch-name sys))]
                      (pl/num-docs writer)))
   :delete-entry (fn [sys key]
                   (let [writer (get (:writers sys) (:current-branch-name sys))]
                     (pl/delete-docs writer "_key" key)
                     sys))
   :supports-concurrent? false})

;; ============================================================
;; Run all compliance tests
;; ============================================================

(deftest ^:compliance full-compliance-suite
  (testing "scriptum passes yggdrasil compliance suite"
    (compliance/run-compliance-tests (make-fixture))))

;; ============================================================
;; Individual test groups (for targeted debugging)
;; ============================================================

(deftest system-identity-tests
  (let [fix (make-fixture)]
    (compliance/test-system-identity fix)))

(deftest snapshotable-tests
  (let [fix (make-fixture)]
    (compliance/test-snapshot-id-after-commit fix)
    (compliance/test-parent-ids-root-commit fix)
    (compliance/test-parent-ids-chain fix)
    (compliance/test-snapshot-meta fix)
    (compliance/test-as-of fix)))

(deftest branchable-tests
  (let [fix (make-fixture)]
    (compliance/test-initial-branches fix)
    (compliance/test-create-branch fix)
    (compliance/test-checkout fix)
    (compliance/test-branch-isolation fix)
    (compliance/test-delete-branch fix)))

(deftest graphable-tests
  (let [fix (make-fixture)]
    (compliance/test-history fix)
    (compliance/test-history-limit fix)
    (compliance/test-ancestors fix)
    (compliance/test-ancestor-predicate fix)
    (compliance/test-common-ancestor fix)
    (compliance/test-commit-graph fix)
    (compliance/test-commit-info fix)))

(deftest mergeable-tests
  (let [fix (make-fixture)]
    (compliance/test-merge fix)
    (compliance/test-merge-parent-ids fix)
    (compliance/test-conflicts-empty-for-compatible fix)))

(deftest data-consistency-tests
  (let [fix (make-fixture)]
    (compliance/test-write-read-roundtrip fix)
    (compliance/test-count-after-writes fix)
    (compliance/test-multiple-entries-readable fix)
    (compliance/test-branch-data-isolation fix)
    (compliance/test-merge-data-visibility fix)
    (compliance/test-as-of-data-consistency fix)
    (compliance/test-delete-entry-consistency fix)
    (compliance/test-overwrite-entry fix)))
