(ns scriptum.tck-runner
  "Runs Lucene's Directory conformance suite against both of scriptum's Directories.

  Kept out of the clojure.test suite deliberately. `BaseDirectoryTestCase` is a
  JUnit class driven by RandomizedRunner, which picks a fresh seed per run and
  spends minutes on the concurrency cases — useful precisely because it is not
  deterministic, and wrong to bury inside a suite people expect to be quick and
  reproducible. Run it on its own:

      clj -T:build compile-tck && clj -M:local:tck -m scriptum.tck-runner

  `:local` selects the konserve working tree. It is no longer strictly required
  — 0.9.375 carries `konserve.gc-guard` — but it is what the rest of this
  branch's tooling uses, so keeping it makes the runs comparable.

  A seed can be pinned to reproduce a failure — as a JVM option, since `-D`
  after `-M` is passed to `clojure.main` as an argument and never reaches the
  JVM:

      clj -J-Dtests.seed=DEADBEEF -M:local:tck -m scriptum.tck-runner"
  (:gen-class)
  (:import [org.junit.runner JUnitCore Result]
           [org.junit.runner.notification Failure]))

(def expected-failures
  "Failures that are known, understood, and not worth failing the build over.

  `BranchedDirectory` extends `Directory` directly, so `FilterDirectory.unwrap`
  cannot see the `MMapDirectory` underneath while the `IndexInput`s it hands out
  do report `isLoaded` — the same inconsistency the konserve Directory was fixed
  for by becoming a `FilterDirectory`. It costs Lucene a capability hint
  (preload/madvise) and nothing else.

  Recorded rather than silenced: a suite that is red on arrival teaches people
  to ignore it, and a suite that hides its known gaps stops being evidence. The
  run reports these and still exits 0; anything NOT listed here fails the build."
  {"org.replikativ.scriptum.BranchedDirectoryTCK"
   #{"testIsLoaded" "testIsLoadedOnSlice"}})

(def suites
  ["org.replikativ.scriptum.ScriptumDirectoryTCK"
   "org.replikativ.scriptum.BranchedDirectoryTCK"])

(defn- describe
  "A Failure's actual cause. `getMessage` is nil for a bare AssertionError, which
  is most of what this suite throws — reporting it alone prints `FAIL x / nil`
  and defeats the point of running the suite at all."
  [^Failure f]
  (let [t (.getException f)
        top (first (remove #(re-find #"^(org\.junit|java\.base|com\.carrotsearch|__randomized)"
                                     (.getClassName ^StackTraceElement %))
                           (.getStackTrace t)))]
    (str (.getName (class t))
         (when-let [m (.getMessage t)] (str ": " m))
         (when top (str "\n            at " top)))))

(defn- method-name [^Failure f]
  (or (.getMethodName (.getDescription f)) (.getTestHeader f)))

(defn -main [& args]
  (let [names (or (seq args) suites)
        unexpected
        (reduce
         (fn [acc n]
           (let [^Result r (.run (JUnitCore.) (into-array Class [(Class/forName n)]))
                 expected (get expected-failures n #{})
                 {known true novel false} (group-by #(contains? expected (method-name %))
                                                    (.getFailures r))]
             (println (format "%-50s %d run, %d failed%s"
                              n (.getRunCount r) (.getFailureCount r)
                              (if (seq known) (str " (" (count known) " known)") "")))
             (doseq [^Failure f known]
               (println "   known" (method-name f)))
             (doseq [^Failure f novel]
               (println "   FAIL " (method-name f))
               (println "        " (describe f)))
             (+ acc (count novel))))
         0 names)]
    (println (if (zero? unexpected)
               "\nAll Directory contracts hold (known gaps aside)."
               (str "\n" unexpected " unexpected failure(s).")))
    (shutdown-agents)
    (System/exit (if (zero? unexpected) 0 1))))
