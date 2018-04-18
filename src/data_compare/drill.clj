(ns data-compare.drill
  (:require [data-compare.schema :refer [Query db-fields extract-data]]))

(def db {:classname "org.apache.drill.jdbc.Driver" 
         :subprotocol "drill" 
         :subname "drillbit=drill:31010"})

(def scl-db {:classname "org.apache.drill.jdbc.Driver" 
         :subprotocol "drill" 
         :subname "drillbit=cskudu:31010"})

(defn- create-data-row [hyena-fields row]
  (map #(extract-data %1 row) hyena-fields))

(defn schema->Query [schema]
  (let [drills (map :kudu schema)
        tables (into #{} (map :table drills))
        table (first tables)
        fields (flatten (map #(db-fields (:kudu %1)) schema))]
    (when (> (count tables) 1)
      (throw (Exception. (str "All kudu fields should come from one table. However, " (count tables) " tables found: " (map str tables)))))
    (reify Query
      (query [_ min-ts max-ts]
        (str "select "
              (reduce #(str %1 ", " %2) fields)
              " from " table
              " where time_stamp_ms >= " (long (/ min-ts 1000))
;              " and hyena.cs.timestamp <= " max-ts
              " limit 1"
              ))
      (process-data [_ rows]
        (map #(create-data-row drills %1) rows)
        )
      )))
