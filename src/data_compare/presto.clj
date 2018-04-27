(ns data-compare.presto
  (:require [data-compare.schema :refer [Query db-fields extract-data]]
            [clojure.string :as string]))

(def db-template {:classname "com.facebook.presto.jdbc.PrestoDriver"
                  :subprotocol "presto"
                  :subname "//localhost:8080/hyena/hyena"
                  :user "not important"})

(defn db 
  ([] (db nil))
  ([address]
   (if (nil? address)
     db-template
     (assoc db-template :subname (str "//" address "/hyena/hyena")))))

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
              " where hyena.cs.timestamp >= " (* min-ts 1000)
;              " and hyena.cs.timestamp <= " (* max-ts 1000)
              " order by hyena.cs.timestamp"
              ;" limit 1"
              ))
      (process-data [_ rows]
        (map #(create-data-row hyenas %1) rows)))))
