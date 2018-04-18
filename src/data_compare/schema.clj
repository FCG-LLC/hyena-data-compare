(ns data-compare.schema
  (require [clojure.data.csv :as csv]
           [clojure.string :as string]))

(defprotocol Query
  (query [q min-ts max-ts] "Returns a SQL query")
  (process-data [q rows] "Processes query results"))

(defprotocol Field
  (db-fields [field] "Returns database fields")
  (extract-data [field row] "From row takes any data it needs"))

(defrecord KuduField [table fields-map]
  Field
  (db-fields [field]
    (map (fn [[id field]] (str field " AS " (name id))) fields-map))
  (extract-data [field row]
    (let [fields (keys fields-map)]
      (into {} (map (fn [key] [(key fields-map) (key row)]) fields)))))

(defn- create-field-pair [id x field] 
  [(keyword (str "field_" id "_" x)) (string/trim field)])

(defn- parse-table+field [item]
  (let [last-dot (string/last-index-of item ".")
        [table field] (split-at last-dot item)
        table (string/trim (apply str table))
        field (apply str (drop 1 field))]
    [table field]))

(defn- create-kudu-field [id item]
  (let [table+fields (map parse-table+field (string/split item #","))
        table (into #{} (map first table+fields))
        fields (map second table+fields)
        field-pairs (map-indexed #(create-field-pair id %1 %2) fields)
        field-map (apply hash-map (flatten field-pairs))]
    (when (> (count table) 1)
      (throw (Exception. (str "All kudu fields should come from one table. However, " (count table) " tables found: " (mapv #(apply str %1) table)))))
    (->KuduField (first table) field-map)))

(defrecord HyenaField [fields-map]
  Field
  (db-fields [self]
    (map (fn [[id field]] (str field " AS " (name id))) fields-map))
  (extract-data [self row]
    (let [fields-map (:fields-map self)
          fields (keys fields-map)]
      (into {} (map (fn [key] [(key fields-map) (key row)]) fields)))))

(defn- create-hyena-field [id item]
  (let [field-pairs (map-indexed #(create-field-pair id %1 %2) (string/split item #","))
        field-map (apply hash-map (flatten field-pairs))]
    (->HyenaField field-map)))

(defrecord Pair [hyena kudu transform])

(defn- create-pair [id [hyena kudu transform]]
  (->Pair (create-hyena-field id hyena) (create-kudu-field id kudu) transform))

(defn parse [text]
  (map-indexed create-pair (csv/read-csv text)))

(defn verify-row [schema hyena-row kudu-row]
  (let [hyena-values (map #(extract-data hyena-row) (map :hyena schema))
        kudu-values (map #(extract-data kudu-row) (map :kudu schema))]
    (= hyena-values kudu-values)))
