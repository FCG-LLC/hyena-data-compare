(ns data-compare.presto
  (:require [data-compare.schema :refer [Query db-fields extract-data]]
            [clojure.string :as string]))

(def db {:classname "com.facebook.presto.jdbc.PrestoDriver"
         :subprotocol "presto"
         :subname "//localhost:8080/hyena/hyena"
         :user "not important"})

(def scl-db {:classname "com.facebook.presto.jdbc.PrestoDriver"
         :subprotocol "presto"
         :subname "//cskudu:8040/hyena/hyena"
         :user "not important"})

(defn- create-data-row [hyena-fields row]
  (map #(extract-data %1 row) hyena-fields))

(defn schema->Query [schema]
  (let [hyenas (map :hyena schema)
        fields (flatten (map #(db-fields (:hyena %1)) schema))]
    (reify Query
      (query [_ min-ts max-ts]
        (str "select "
              (reduce #(str %1 ", " %2) fields)
              " from hyena.cs"
              " where hyena.cs.timestamp >= " min-ts
;              " and hyena.cs.timestamp <= " max-ts
              ;" limit 1"
              ))
      (process-data [_ rows]
        (map #(create-data-row hyenas %1) rows)
        )
      )))
