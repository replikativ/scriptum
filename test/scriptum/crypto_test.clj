(ns scriptum.crypto-test
  "Tests for ContentHash Java implementation and crypto-hash integration.
   Ensures SHA-512 hashing is consistent, produces valid UUID5s, and
   the merkle tree correctly detects tampering across Lucene operations."
  (:require [clojure.test :refer [deftest is testing]]
            [hasch.core :as hasch]
            [scriptum.core :as sc])
  (:import [org.replikativ.scriptum ContentHash]
           [java.util HashMap LinkedHashMap]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files StandardOpenOption]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant]
           [java.util UUID]))

(deftest hash-bytes-consistency
  (testing "ContentHash.hashBytes produces valid UUID5"
    (let [test-data (.getBytes "Hello, Scriptum!" StandardCharsets/UTF_8)
          java-uuid (ContentHash/hashBytes test-data)]

      ;; Verify it's a valid UUID
      (is (instance? UUID java-uuid))

      ;; Verify UUID version is 5
      (is (= 5 (.version java-uuid))
          "UUID version should be 5")

      ;; Verify UUID variant is RFC4122 (variant 2)
      (is (= 2 (.variant java-uuid))
          "UUID variant should be 2 (RFC4122)")

      ;; Verify determinism
      (is (= java-uuid (ContentHash/hashBytes test-data))
          "Same input produces same UUID")))

  (testing "Different inputs produce different hashes"
    (let [data1 (.getBytes "test1" StandardCharsets/UTF_8)
          data2 (.getBytes "test2" StandardCharsets/UTF_8)
          hash1 (ContentHash/hashBytes data1)
          hash2 (ContentHash/hashBytes data2)]

      (is (not= hash1 hash2)
          "Different inputs should produce different hashes"))))

(deftest hash-file-consistency
  (testing "File hashing is deterministic"
    (let [temp-file (java.io.File/createTempFile "scriptum-test" ".txt")]
      (try
        (spit temp-file "Test file content for hashing\nLine 2\nLine 3")
        (let [file-path (.toPath temp-file)
              hash1 (ContentHash/hashFile file-path)
              hash2 (ContentHash/hashFile file-path)]

          (is (= hash1 hash2)
              "Same file produces same hash")

          (is (= 5 (.version hash1))
              "File hash is UUID5"))
        (finally
          (.delete temp-file))))))

(deftest hash-segment-files
  (testing "Segment file hashing"
    (let [temp-dir (Files/createTempDirectory "scriptum-segment-test"
                                              (into-array java.nio.file.attribute.FileAttribute []))
          file1 (.resolve temp-dir "_0.cfs")
          file2 (.resolve temp-dir "_0.si")]
      (try
        ;; Create test files
        (Files/write file1 (.getBytes "Segment data 1" StandardCharsets/UTF_8)
                     (into-array StandardOpenOption []))
        (Files/write file2 (.getBytes "Segment data 2" StandardCharsets/UTF_8)
                     (into-array StandardOpenOption []))

        (let [file-paths (doto (LinkedHashMap.)
                           (.put "_0.cfs" file1)
                           (.put "_0.si" file2))
              hashes (ContentHash/hashSegmentFiles file-paths)]

          ;; Verify we got hashes for both files
          (is (= 2 (.size hashes)))
          (is (.containsKey hashes "_0.cfs"))
          (is (.containsKey hashes "_0.si"))

          ;; Verify hashes are valid UUID5s
          (is (= 5 (.version (.get hashes "_0.cfs"))))
          (is (= 5 (.version (.get hashes "_0.si"))))

          ;; Verify determinism
          (let [hashes2 (ContentHash/hashSegmentFiles file-paths)]
            (is (= hashes hashes2))))

        (finally
          (Files/delete file1)
          (Files/delete file2)
          (Files/delete temp-dir))))))

(deftest compute-commit-hash
  (testing "Commit hash computation"
    (let [parent-hash (UUID/fromString "550e8400-e29b-41d4-a716-446655440000")
          segment-hashes (doto (LinkedHashMap.)
                           (.put "_0" (doto (HashMap.)
                                        (.put "_0.cfs" (UUID/randomUUID))
                                        (.put "_0.si" (UUID/randomUUID))))
                           (.put "_1" (doto (HashMap.)
                                        (.put "_1.cfs" (UUID/randomUUID)))))
          commit-hash (ContentHash/computeCommitHash parent-hash segment-hashes)]

      ;; Verify it's a valid UUID5
      (is (instance? UUID commit-hash))
      (is (= 5 (.version commit-hash)))

      ;; Verify determinism
      (is (= commit-hash (ContentHash/computeCommitHash parent-hash segment-hashes)))

      ;; Verify different parent produces different hash
      (let [different-parent (UUID/randomUUID)
            different-hash (ContentHash/computeCommitHash different-parent segment-hashes)]
        (is (not= commit-hash different-hash)
            "Different parent should produce different commit hash"))))

  (testing "Root commit (no parent)"
    (let [segment-hashes (doto (HashMap.)
                           (.put "_0" (doto (HashMap.)
                                        (.put "_0.cfs" (UUID/randomUUID)))))
          commit-hash (ContentHash/computeCommitHash nil segment-hashes)]

      (is (instance? UUID commit-hash))
      (is (= 5 (.version commit-hash))))))

(deftest uuid5-bit-patterns
  (testing "UUID5 version and variant bits are correctly set"
    (let [test-cases ["test" "another test" "yet another" ""]
          test-uuids (map #(ContentHash/hashBytes (.getBytes % StandardCharsets/UTF_8))
                          test-cases)]

      (doseq [uuid test-uuids]
        ;; Check version = 5
        (is (= 5 (.version uuid))
            (str "UUID version should be 5 for: " uuid))

        ;; Check variant = 2 (RFC4122)
        (is (= 2 (.variant uuid))
            (str "UUID variant should be 2 for: " uuid))))))

(deftest empty-input-handling
  (testing "Hashing empty data"
    (let [empty-bytes (byte-array 0)
          hash (ContentHash/hashBytes empty-bytes)]

      (is (instance? UUID hash))
      (is (= 5 (.version hash)))))

  (testing "Hashing empty map"
    (let [empty-map (HashMap.)
          hash (ContentHash/hashMap empty-map)]

      (is (instance? UUID hash))
      (is (= 5 (.version hash))))))

(deftest large-data-hashing
  (testing "Hashing large byte arrays"
    ;; Create 1MB of data
    (let [large-data (.getBytes (apply str (repeat 10000 "0123456789"))
                                StandardCharsets/UTF_8)
          hash1 (ContentHash/hashBytes large-data)
          hash2 (ContentHash/hashBytes large-data)]

      (is (= hash1 hash2)
          "Large data produces consistent hashes")
      (is (= 5 (.version hash1))))))

(deftest nested-map-hashing
  (testing "Nested map structures"
    (let [nested-map (doto (HashMap.)
                       (.put "segments"
                             (doto (HashMap.)
                               (.put "_0" (doto (HashMap.)
                                            (.put "file1" (UUID/randomUUID))
                                            (.put "file2" (UUID/randomUUID))))
                               (.put "_1" (doto (HashMap.)
                                            (.put "file1" (UUID/randomUUID)))))))
          hash1 (ContentHash/hashMap nested-map)
          hash2 (ContentHash/hashMap nested-map)]

      (is (= hash1 hash2)
          "Nested maps produce consistent hashes")
      (is (= 5 (.version hash1))))))

;; ============================================================
;; Integration tests: crypto-hash against real Lucene indices
;; ============================================================

(defn- temp-dir []
  (str (Files/createTempDirectory "scriptum-crypto-test-"
                                  (make-array FileAttribute 0))))

(defn- delete-dir-recursive [path]
  (let [dir (java.io.File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(deftest tamper-detection
  (testing "modifying a segment file invalidates verification"
    (let [path (temp-dir)
          writer (sc/create-index path "main" {:crypto-hash? true})]
      (try
        (sc/add-doc writer {:id {:type :string :value "doc1"}
                            :content {:type :text :value "important data"}})
        (sc/commit! writer "initial")

        (is (:valid? (sc/verify-commit writer))
            "clean commit should verify")

        ;; Corrupt a segment file by appending a byte
        (let [dir (java.io.File. path)
              cfs (first (filter #(.endsWith (.getName %) ".cfs")
                                 (.listFiles dir)))]
          (is (some? cfs) "segment file should exist")
          (with-open [os (java.io.FileOutputStream. cfs true)]
            (.write os (byte-array [42]))))

        (let [result (sc/verify-commit writer)]
          (is (false? (:valid? result))
              "tampered commit should fail verification")
          (is (seq (:errors result))
              "should report hash mismatch errors"))

        (finally
          (sc/close! writer)
          (delete-dir-recursive path))))))

(deftest fork-content-hash-isolation
  (testing "divergent writes produce different content-hashes, both verify"
    (let [path (temp-dir)
          writer (sc/create-index path "main" {:crypto-hash? true})]
      (try
        (sc/add-doc writer {:content {:type :text :value "shared"}})
        (sc/commit! writer "base")

        (let [branch (sc/fork writer "feature")]
          (try
            ;; Divergent writes
            (sc/add-doc branch {:content {:type :text :value "branch only"}})
            (let [bc (sc/commit! branch "branch write")]

              (sc/add-doc writer {:content {:type :text :value "main only"}})
              (let [mc (sc/commit! writer "main write")]

                (is (not= (:content-hash bc) (:content-hash mc))
                    "divergent branches should produce different hashes")

                (is (:valid? (sc/verify-commit branch))
                    "branch commit should verify")
                (is (:valid? (sc/verify-commit writer))
                    "main commit should verify")))

            (finally
              (sc/close! branch))))

        (finally
          (sc/close! writer)
          (delete-dir-recursive path))))))

(deftest parent-chain-survives-reopen
  (testing "content-hash chain works after close and reopen"
    (let [path (temp-dir)
          writer (sc/create-index path "main" {:crypto-hash? true})]
      (try
        (sc/add-doc writer {:content {:type :text :value "first"}})
        (let [c1 (sc/commit! writer "first")]

          (sc/close! writer)

          ;; Reopen and continue the chain
          (let [writer2 (sc/create-index path "main" {:crypto-hash? true})]
            (try
              (sc/add-doc writer2 {:content {:type :text :value "second"}})
              (let [c2 (sc/commit! writer2 "second after reopen")]

                (is (not= (:content-hash c1) (:content-hash c2))
                    "new commit should have different hash")

                (is (:valid? (sc/verify-commit writer2))
                    "commit after reopen should verify")

                ;; Verify the parent chain by reading the metadata file
                (let [hash-dir (java.io.File. (str path "/scriptum-hashes"))
                      files (.listFiles hash-dir)
                      c2-file (first (filter #(.contains (.getName %) (:commit-id c2))
                                             files))
                      content (slurp c2-file)]
                  (is (.contains content (:content-hash c1))
                      "second commit metadata should reference first commit's content-hash as parent")))

              (finally
                (sc/close! writer2)))))

        (finally
          (delete-dir-recursive path))))))

(deftest delete-changes-content-hash
  (testing "deleting documents changes the content-hash"
    (let [path (temp-dir)
          writer (sc/create-index path "main" {:crypto-hash? true})]
      (try
        (sc/add-doc writer {:id {:type :string :value "keep"}
                            :content {:type :text :value "keeper"}})
        (sc/add-doc writer {:id {:type :string :value "remove"}
                            :content {:type :text :value "doomed"}})
        (let [before (sc/commit! writer "before delete")]

          (sc/delete-docs writer "id" "remove")
          (let [after (sc/commit! writer "after delete")]

            (is (not= (:content-hash before) (:content-hash after))
                "delete should change content-hash")

            (is (:valid? (sc/verify-commit writer))
                "post-delete commit should verify")))

        (finally
          (sc/close! writer)
          (delete-dir-recursive path))))))

(deftest gc-preserves-crypto-hash-verification
  (testing "GC commit has valid crypto-hash metadata and surviving commits remain verifiable"
    (let [path (temp-dir)
          writer (sc/create-index path "main" {:crypto-hash? true})]
      (try
        ;; Create several commits
        (dotimes [i 5]
          (sc/add-doc writer {:id {:type :string :value (str "doc-" i)}
                              :content {:type :text :value (str "content " i)}})
          (sc/commit! writer (str "commit-" i)))

        (let [snaps-before (sc/list-snapshots writer)]
          (is (= 5 (count snaps-before)))

          ;; GC everything
          (sc/gc! writer (Instant/now))

          ;; HEAD should still verify (the GC commit must have metadata)
          (let [result (sc/verify-commit writer)]
            (is (:valid? result)
                (str "HEAD should verify after GC, errors: " (:errors result)))
            (is (some? (:commit-id result))
                "GC commit should have a commit-id"))

          ;; Hash metadata files should exist for surviving commits
          (let [hash-dir (java.io.File. (str path "/scriptum-hashes"))
                json-files (when (.exists hash-dir)
                             (filter #(.endsWith (.getName %) ".json")
                                     (.listFiles hash-dir)))]
            (is (pos? (count json-files))
                "hash metadata files should exist for surviving commits")))

        (finally
          (sc/close! writer)
          (delete-dir-recursive path))))))
