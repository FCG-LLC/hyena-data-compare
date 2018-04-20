(ns data-compare.schema
  (require [clojure.data.csv :as csv]
           [clojure.string :as string]
           [clojure.edn :as edn]))

(defprotocol Query
  (query [q min-ts max-ts] "Returns a SQL query")
  (process-data [q rows] "Processes query results"))

(defprotocol Field
  (db-fields [field] "Returns database fields")
  (extract-data [field row] "From row takes any data it needs"))

(defrecord KuduField [table fields-map transform-fn]
  Field
  (db-fields [field]
    (map (fn [[id field]] (str field " AS " (name id))) fields-map))
  (extract-data [field row]
    (let [fields (sort (keys fields-map))
          values (map (fn [key] (key row)) fields)
          value  (if-not (nil? transform-fn)
                   (apply transform-fn values)
                   values)]
      (if (and (sequential? value) (= 1 (count value)))
        (first value)
        value))))

(defn- kudu-timestamp [bucket remainder]
  (+ (* bucket 1000000) remainder))

(defn- apply-masks [number masks shifts]
  (map #(bit-shift-right (bit-and %1 number) %2) masks shifts))

(defonce ^:const IPV4-PREFIX 0x0064ff9b00000000)
(defonce ^:const IPV4-SHIFTS [24 16 8 0])
(defonce ^:const IPV4-MASKS (map #(bit-shift-left 0xff %1) IPV4-SHIFTS))

(defn long-to-ipv4 [long]
  (let [parts (apply-masks long IPV4-MASKS IPV4-SHIFTS)
        str-parts (map str parts)]
    (string/join "." str-parts)))

(defonce ^:const IPV6-SHIFTS [48 32 16 0])
(defonce ^:const IPV6-MASKS (map #(bit-shift-left 0xffff %1) IPV6-SHIFTS))

(defn- long-pair-to-ipv6 [hi lo]
  (let [parts-hi (apply-masks hi IPV6-MASKS IPV6-SHIFTS)
        parts-lo (apply-masks lo IPV6-MASKS IPV6-SHIFTS)
        parts (map #(format "%x" %) (concat parts-hi parts-lo))
        ip (string/join ":" parts)]
    (string/replace-first ip #"(^|:)(0+(:|$)){2,8}" "::")))

(defn long-pair-to-ip [hi lo]
  (if (= hi IPV4-PREFIX)
    (long-to-ipv4 lo)
    (long-pair-to-ipv6 hi lo)))

(defonce kudu-transforms 
  {:timestamp kudu-timestamp
   :ip        long-pair-to-ip})

(defn- create-field-pair [id x field] 
  [(keyword (str "field_" id "_" x)) (string/trim field)])

(defn- parse-table+field [item]
  (let [last-dot (string/last-index-of item ".")
        [table field] (split-at last-dot item)
        table (string/trim (apply str table))
        field (apply str (drop 1 field))]
    [table field]))

(defn- create-kudu-field [id item transform]
  (let [table+fields (map parse-table+field (string/split item #","))
        table (into #{} (map first table+fields))
        fields (map second table+fields)
        field-pairs (map-indexed #(create-field-pair id %1 %2) fields)
        field-map (apply hash-map (flatten field-pairs))]
    (when (> (count table) 1)
      (throw (Exception. (str "All kudu fields should come from one table. However, " (count table) " tables found: " (mapv #(apply str %1) table)))))
    (->KuduField (first table) field-map (get kudu-transforms transform))))

(defrecord HyenaField [fields-map transform-fn]
  Field
  (db-fields [self]
    (map (fn [[id field]] (str field " AS " (name id))) fields-map))
  (extract-data [self row]
    (let [fields-map (:fields-map self)
          fields (keys fields-map)]
      (into {} (map (fn [key] [(transform-fn (key fields-map)) (key row)]) fields)))))

(defonce hyena-transforms
  {:ip long-pair-to-ip})

(defn- create-hyena-field [id item transform]
  (let [field-pairs (map-indexed #(create-field-pair id %1 %2) (string/split item #","))
        field-map (apply hash-map (flatten field-pairs))]
    (->HyenaField field-map (get hyena-transforms transform))))

(defrecord Pair [hyena kudu])

(defn- create-pair [id [hyena kudu h-transform k-transform]]
  (->Pair (create-hyena-field id hyena (edn/read-string h-transform))
          (create-kudu-field id kudu (edn/read-string k-transform))))

(defn- remove-comments [lines]
  (remove #(string/starts-with? (string/triml %1) "#") lines))

(defn- remove-blank-lines [lines]
  (remove string/blank? lines))

(defn clean-csv-file [text]
  (->> text
       string/split-lines
       remove-comments
       remove-blank-lines
       (string/join "\n")))

(defn parse [text]
  (map-indexed create-pair (csv/read-csv (clean-csv-file text))))

(defn verify-row [schema hyena-row kudu-row]
  (let [hyena-values (map #(extract-data %1 hyena-row) (map :hyena schema))
        kudu-values (map #(extract-data %1 kudu-row) (map :kudu schema))]
    (= hyena-values kudu-values)))
