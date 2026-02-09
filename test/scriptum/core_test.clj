(ns scriptum.core-test
  "Unit tests for scriptum.core Lucene functionality."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [scriptum.core :as sc])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant Duration]
           [org.replikativ.scriptum BranchIndexWriter]))

(defn- temp-dir []
  (str (Files/createTempDirectory "scriptum-core-test-"
                                  (make-array FileAttribute 0))))

(defn- delete-dir-recursive [path]
  (let [dir (java.io.File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

;; ============================================================
;; Document Operations
;; ============================================================

(deftest add-and-search-text-docs
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (sc/add-doc writer {:title "Hello World" :body "This is a test document"})
      (sc/add-doc writer {:title "Second Doc" :body "Another test with different content"})
      (sc/commit! writer)

      (testing "num-docs reflects added documents"
        (is (= 2 (sc/num-docs writer))))

      (testing "search by term returns matching docs"
        (let [results (sc/search writer {:term [:body "test"]})]
          (is (= 2 (count results)))
          (is (every? #(contains? % :score) results))
          (is (every? #(contains? % :doc-id) results))))

      (testing "search by specific term returns subset"
        (let [results (sc/search writer {:term [:title "hello"]})]
          (is (= 1 (count results)))
          (is (= "Hello World" (get (first results) "title")))))

      (testing "match-all query returns all docs"
        (let [results (sc/search writer :all)]
          (is (= 2 (count results)))))

      (testing "search with limit"
        (let [results (sc/search writer :all {:limit 1})]
          (is (= 1 (count results)))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

(deftest add-doc-field-types
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (testing "string field (exact match)"
        (sc/add-doc writer {:id {:value "doc-1" :type :string :stored? true}
                            :title {:value "Test Title" :type :text :stored? true}})
        (sc/commit! writer)

        (let [results (sc/search writer {:term [:id "doc-1"]})]
          (is (= 1 (count results)))
          (is (= "doc-1" (get (first results) "id")))
          (is (= "Test Title" (get (first results) "title")))))

      (testing "non-stored fields are not returned in results"
        (sc/add-doc writer {:visible {:value "I am stored" :type :text :stored? true}
                            :hidden {:value "I am not stored" :type :text :stored? false}})
        (sc/commit! writer)

        (let [results (sc/search writer {:term [:visible "stored"]})]
          (is (= 1 (count results)))
          (is (some? (get (first results) "visible")))
          (is (nil? (get (first results) "hidden")))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

(deftest delete-documents
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (sc/add-doc writer {:id {:value "keep" :type :string} :body "keep this"})
      (sc/add-doc writer {:id {:value "remove" :type :string} :body "delete this"})
      (sc/commit! writer)
      (is (= 2 (sc/num-docs writer)))

      (sc/delete-docs writer "id" "remove")
      (sc/commit! writer)
      (is (= 1 (sc/num-docs writer)))

      (let [results (sc/search writer {:term [:id "remove"]})]
        (is (empty? results)))
      (let [results (sc/search writer {:term [:id "keep"]})]
        (is (= 1 (count results))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

(deftest update-document
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      ;; Use :text type for id since update-doc creates TextFields
      (sc/add-doc writer {:id {:value "doc1" :type :text :stored? true}
                          :title {:value "Original" :type :text :stored? true}})
      (sc/commit! writer)

      (sc/update-doc writer "id" "doc1" {:id "doc1" :title "Updated"})
      (sc/commit! writer)

      (is (= 1 (sc/num-docs writer)))
      (let [results (sc/search writer {:term [:id "doc1"]})]
        (is (= 1 (count results)))
        (is (= "Updated" (get (first results) "title"))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

;; ============================================================
;; Fork & Branch Isolation
;; ============================================================

(deftest fork-isolation
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (sc/add-doc writer {:content "main-doc-1"})
      (sc/commit! writer)

      (let [branch (sc/fork writer "feature")]
        (try
          (testing "fork sees parent docs"
            (is (= 1 (sc/num-docs branch))))

          (testing "adding to branch doesn't affect main"
            (sc/add-doc branch {:content "branch-doc-1"})
            (sc/add-doc branch {:content "branch-doc-2"})
            (sc/commit! branch)
            (is (= 3 (sc/num-docs branch)))
            (is (= 1 (sc/num-docs writer))))

          (testing "adding to main doesn't affect branch"
            (sc/add-doc writer {:content "main-doc-2"})
            (sc/commit! writer)
            (is (= 2 (sc/num-docs writer)))
            (is (= 3 (sc/num-docs branch))))

          (finally
            (sc/close! branch))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

(deftest fork-multiple-branches
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (sc/add-doc writer {:content "base"})
      (sc/commit! writer)

      (let [b1 (sc/fork writer "branch-1")
            b2 (sc/fork writer "branch-2")]
        (try
          (sc/add-doc b1 {:content "b1-only"})
          (sc/commit! b1)

          (sc/add-doc b2 {:content "b2-only"})
          (sc/add-doc b2 {:content "b2-extra"})
          (sc/commit! b2)

          (is (= 2 (sc/num-docs b1)))
          (is (= 3 (sc/num-docs b2)))
          (is (= 1 (sc/num-docs writer)))

          (finally
            (sc/close! b1)
            (sc/close! b2))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

;; ============================================================
;; Snapshots & Time-Travel
;; ============================================================

(deftest list-snapshots-metadata
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (sc/add-doc writer {:content "first"})
      (sc/commit! writer "First commit")

      (sc/add-doc writer {:content "second"})
      (sc/commit! writer "Second commit")

      (let [snaps (sc/list-snapshots writer)]
        (testing "two snapshots exist"
          (is (= 2 (count snaps))))

        (testing "snapshots have UUIDs"
          (is (every? :snapshot-id snaps))
          (is (not= (:snapshot-id (first snaps))
                    (:snapshot-id (second snaps)))))

        (testing "snapshots have timestamps"
          (is (every? :timestamp snaps)))

        (testing "snapshots have messages"
          (is (= "First commit" (:message (first snaps))))
          (is (= "Second commit" (:message (second snaps)))))

        (testing "snapshots have branch name"
          (is (every? #(= "main" (:branch %)) snaps)))

        (testing "second snapshot has first as parent"
          (let [first-id (:snapshot-id (first snaps))
                second-parents (:parent-ids (second snaps))]
            (is (clojure.string/includes? (or second-parents "") first-id)))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

(deftest time-travel-open-reader-at
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (sc/add-doc writer {:content "gen1"})
      (sc/commit! writer "gen1")

      (sc/add-doc writer {:content "gen2"})
      (sc/commit! writer "gen2")

      ;; Get actual generations from list-snapshots (commit! returns sequence
      ;; numbers in Lucene 10.x, not generations)
      (let [snaps (sc/list-snapshots writer)
            gen1 (:generation (first snaps))
            gen2 (:generation (second snaps))]

        (testing "current state has 2 docs"
          (is (= 2 (sc/num-docs writer))))

        (testing "reader at gen1 sees only 1 doc"
          (with-open [reader (sc/open-reader-at writer gen1)]
            (is (= 1 (.numDocs reader)))))

        (testing "reader at gen2 sees 2 docs"
          (with-open [reader (sc/open-reader-at writer gen2)]
            (is (= 2 (.numDocs reader)))))

        (testing "commit-available? works"
          (is (true? (sc/commit-available? writer gen1)))
          (is (true? (sc/commit-available? writer gen2)))
          (is (false? (sc/commit-available? writer 99999)))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

;; ============================================================
;; Merge
;; ============================================================

(deftest merge-from-branch
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (sc/add-doc writer {:content "main-1"})
      (sc/commit! writer)

      (let [branch (sc/fork writer "feature")]
        (try
          (sc/add-doc branch {:content "feature-1"})
          (sc/add-doc branch {:content "feature-2"})
          (sc/commit! branch)

          (testing "before merge, main has 1 doc"
            (is (= 1 (sc/num-docs writer))))

          (sc/merge-from! writer branch)

          (testing "after merge, main has original + all source docs (add-only)"
            ;; merge-from! uses addIndexes which copies ALL docs from source
            ;; including the shared base doc, so: 1 (original) + 3 (source) = 4
            (is (= 4 (sc/num-docs writer))))

          (finally
            (sc/close! branch))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

;; ============================================================
;; GC
;; ============================================================

(deftest gc-removes-old-commits
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      ;; Create several commits
      (dotimes [i 5]
        (sc/add-doc writer {:content (str "doc-" i)})
        (sc/commit! writer (str "commit-" i)))

      (let [snaps-before (sc/list-snapshots writer)
            _ (is (= 5 (count snaps-before)))
            ;; GC everything before "now" (should remove all but last)
            removed (sc/gc! writer (Instant/now))]
        (testing "GC removed old commits"
          (is (pos? removed)))

        (testing "at least the latest commit survives"
          (let [snaps-after (sc/list-snapshots writer)]
            (is (pos? (count snaps-after)))
            (is (< (count snaps-after) (count snaps-before))))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

;; ============================================================
;; Accessors
;; ============================================================

(deftest accessor-functions
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (is (= "main" (sc/branch-name writer)))
      (is (= path (sc/base-path writer)))
      (is (true? (sc/main-branch? writer)))

      (let [branch (sc/fork writer "feature")]
        (try
          (is (= "feature" (sc/branch-name branch)))
          (is (false? (sc/main-branch? branch)))
          (finally
            (sc/close! branch))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

;; ============================================================
;; Discover Branches
;; ============================================================

(deftest discover-branches-test
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (sc/add-doc writer {:content "base"})
      (sc/commit! writer)

      (let [b1 (sc/fork writer "alpha")
            b2 (sc/fork writer "beta")]
        (try
          (let [discovered (sc/discover-branches path)]
            (is (contains? discovered "alpha"))
            (is (contains? discovered "beta")))
          (finally
            (sc/close! b1)
            (sc/close! b2))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))
