(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd])
  (:import [clojure.lang ExceptionInfo]))

(def lib 'org.replikativ/scriptum)
(def version (format "0.1.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (delay (b/create-basis {:project "deps.edn"})))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"})
  (b/delete {:path "classes"}))

(defn compile-java [_]
  (b/javac {:src-dirs ["src/java"]
            :class-dir "classes"
            :basis @basis
            :javac-opts ["--release" "21" "-Xlint:unchecked"]}))

(def tck-basis
  "Basis with Lucene's test framework, which carries `BaseDirectoryTestCase`."
  (delay (b/create-basis {:project "deps.edn" :aliases [:tck]})))

(defn compile-tck
  "Compile the JUnit classes that run Lucene's Directory conformance suite.

  Separate from `compile-java` because these are TESTS that happen to be Java —
  they belong on the test classpath, never in the jar, and they need the Lucene
  test framework that production code must not depend on.

  The classpath is passed explicitly: `b/javac` composes one from the basis's
  LIBS alone, so the project's own `:paths` — and hence `classes/`, where
  `compile-java` just put `BranchedDirectory` — are invisible to it. A trailing
  `-classpath` wins over the one it prepends."
  [_]
  (compile-java nil)
  (let [cp (str/join java.io.File/pathSeparator
                     (concat ["classes"] (mapcat :paths (vals (:libs @tck-basis)))))]
    (b/javac {:src-dirs ["test/java"]
              :class-dir "target/test-classes"
              :basis @tck-basis
              :javac-opts ["--release" "21" "-Xlint:unchecked" "-classpath" cp]})))

(defn jar [_]
  (compile-java nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["src/clojure"]
                :scm {:url "https://github.com/replikativ/scriptum"
                      :connection "scm:git:git://github.com/replikativ/scriptum.git"
                      :developerConnection "scm:git:ssh://git@github.com/replikativ/scriptum.git"
                      :tag (str "v" version)}
                :pom-data [[:description "Copy-on-write branching for Apache Lucene with Git-like semantics"]
                           [:url "https://github.com/replikativ/scriptum"]
                           [:licenses
                            [:license
                             [:name "Apache License, Version 2.0"]
                             [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]
                           [:developers
                            [:developer
                             [:id "whilo"]
                             [:name "Christian Weilbach"]
                             [:email "ch_weil@topiq.es"]]]]})
  (b/copy-dir {:src-dirs ["src/clojure" "classes"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn deploy
  "Deploy to Clojars. Set CLOJARS_USERNAME and CLOJARS_PASSWORD env vars."
  [_]
  (jar nil)
  (dd/deploy {:installer :remote
              :artifact jar-file
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})}))

(defn install [_]
  (clean nil)
  (jar nil)
  (b/install {:basis @basis
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir}))
