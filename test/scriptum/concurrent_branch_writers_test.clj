(ns scriptum.concurrent-branch-writers-test
  "Regression test: multiple `BranchIndexWriter` instances on the same
   scriptum tree must coexist in the same JVM, each with its own lock.

   Before this fix, the main branch's writer took `basePath/write.lock`,
   so any second writer (a fork, a reopen, or a parallel branch) that
   touched basePath in the same JVM contended for the same lock and
   failed with `LockObtainFailedException: Lock held by this virtual
   machine`."
  (:require [clojure.test :refer [deftest is testing]]
            [scriptum.core :as sc])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-dir []
  (str (Files/createTempDirectory "scriptum-concurrent-"
                                  (make-array FileAttribute 0))))

(deftest main-and-feature-coexist
  (testing "after a fork, main + feature writers run in parallel without
            lock contention"
    (let [path (temp-dir)
          main (sc/create-index path "main")]
      (try
        (sc/add-doc main {:title "Alice" :body "main content"})
        (sc/commit! main "seed")
        ;; Create the feature branch via fork.
        (let [feature (sc/fork main "feature")]
          (try
            ;; Both writers are now live. Mutate each.
            (sc/add-doc main {:title "Bob" :body "main update"})
            (sc/commit! main "main-update")
            (sc/add-doc feature {:title "Charlie" :body "feature update"})
            (sc/commit! feature "feature-update")
            (is (= 2 (sc/num-docs main))
                "main: seed + main-update = 2")
            (is (= 2 (sc/num-docs feature))
                "feature: seed (inherited at fork) + feature-update = 2;
                 main's post-fork write is NOT visible (COW isolation)")
            (finally (sc/close! feature))))
        (finally (sc/close! main))))))

(deftest reopen-main-while-fork-is-open
  (testing "open-branch \"main\" while a feature fork is still alive
            doesn't contend for the same lock file"
    (let [path (temp-dir)
          main (sc/create-index path "main")]
      (try
        (sc/add-doc main {:title "X" :body "X"})
        (sc/commit! main)
        (let [feature (sc/fork main "feature")]
          (try
            ;; Close main; reopen it while feature is still alive.
            (sc/close! main)
            (let [main2 (sc/open-branch path "main")]
              (try
                (is (= 1 (sc/num-docs main2)))
                (sc/add-doc main2 {:title "Y" :body "Y"})
                (sc/commit! main2)
                (is (= 2 (sc/num-docs main2)))
                (finally (sc/close! main2))))
            (finally (sc/close! feature))))
        (catch Exception e
          (when (.isOpen ^java.io.Closeable main) (sc/close! main))
          (throw e))))))

(deftest two-non-main-branches-coexist
  (testing "two non-main branches each take their own per-branch lock"
    (let [path (temp-dir)
          main (sc/create-index path "main")]
      (try
        (sc/add-doc main {:title "seed" :body "seed"})
        (sc/commit! main)
        (let [b1 (sc/fork main "branch-1")
              b2 (sc/fork main "branch-2")]
          (try
            (sc/add-doc b1 {:title "in-branch-1" :body "b1"})
            (sc/commit! b1)
            (sc/add-doc b2 {:title "in-branch-2" :body "b2"})
            (sc/commit! b2)
            (is (= 2 (sc/num-docs b1)))
            (is (= 2 (sc/num-docs b2)))
            (finally (sc/close! b1) (sc/close! b2))))
        (finally (sc/close! main))))))
