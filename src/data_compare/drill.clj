(ns data-compare.drill
  (:require [data-compare.schema :refer [Query db-fields extract-data]]
            [clojure.string :as string]))

(def db-template {:classname "org.apache.drill.jdbc.Driver" 
                  :subprotocol "drill" 
                  :subname "drillbit=drill:31010"})

(defn db 
  ([] (db nil))
  ([address]
   (if (nil? address)
     db-template
     (assoc db-template :subname (str "drillbit=" address)))))

(defn- create-data-row [hyena-fields row]
  (map #(extract-data %1 row) hyena-fields))

(defn schema->Query [schema]
  (let [drills (map :kudu schema)
        tables (into #{} (map :table drills))
        table (first tables)
        table-no-kudu (if (string/starts-with? table "kudu.")
                        (subs table 5)
                        table)
        fields (flatten (map #(db-fields (:kudu %1)) schema))]
    (when (> (count tables) 1)
      (throw (Exception. (str "All kudu fields should come from one table. However, " (count tables) " tables found: " (map str tables)))))
    (reify Query
      (query [_ min-ts max-ts]
        (str "select "
              (reduce #(str %1 ", " %2) fields)
              " from " table
              " where " table-no-kudu ".time_stamp_bucket >= " (int (/ min-ts 1000))
              "  and " table-no-kudu ".time_stamp_bucket < " (int (/ max-ts 1000))
              " order by " table-no-kudu ".time_stamp_bucket, " table-no-kudu ".time_stamp_remainder"
              ))
      (process-data [_ rows]
        (map #(create-data-row drills %1) rows)))))
