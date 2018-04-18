(ns data-compare.jdbc
  (require [clojure.java.jdbc :as j]))

(defn query [db sql]
  (let [stmt (.createStatement (j/get-connection db))]
    (.execute stmt sql)
    (j/result-set-seq (.getResultSet stmt))))
