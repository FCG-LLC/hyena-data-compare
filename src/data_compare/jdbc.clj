(ns data-compare.jdbc
  (require [clojure.java.jdbc :as javajdbc]))

(defn query [db sql]
  (let [stmt (.createStatement (javajdbc/get-connection db))]
    (.execute stmt sql)
    (javajdbc/result-set-seq (.getResultSet stmt))))
