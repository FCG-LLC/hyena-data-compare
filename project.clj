(defproject data-compare "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.7.5"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/data.csv "0.1.4"]
                 [org.apache.drill/drill-common "1.12.0"]
                 [org.apache.drill.exec/drill-jdbc-all "1.12.0"]
                 [com.facebook.presto/presto-jdbc "0.201"]
                 [clj-time "0.14.3"]
                 [clojure-term-colors "0.1.0"]]
  :main ^:skip-aot data-compare.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
