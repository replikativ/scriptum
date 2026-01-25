(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.build.api :as b]
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
                             [:name "Eclipse Public License 2.0"]
                             [:url "https://www.eclipse.org/legal/epl-2.0/"]]]
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
