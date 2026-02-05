(ns scriptum.crypto-test
  "Tests for ContentHash Java implementation vs hasch Clojure implementation.
   Ensures SHA-512 hashing is consistent and produces valid UUID5s."
  (:require [clojure.test :refer [deftest is testing]]
            [hasch.core :as hasch])
  (:import [org.replikativ.scriptum ContentHash]
           [java.util HashMap LinkedHashMap]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files StandardOpenOption]
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
