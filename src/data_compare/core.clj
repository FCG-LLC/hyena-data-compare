(ns data-compare.core
  (require [data-compare.jdbc :as jdbc]
           [data-compare.drill :as drill]
           [data-compare.presto :as presto]
           [data-compare.cli :as cli :refer [when-verbose]]
           [data-compare.schema :as schema]
           [clj-time.core :as t]
           [clj-time.periodic :as p]
           [clj-time.coerce :as c]
           [clojure.pprint :as pp])
  (:gen-class))

(defonce opts (atom {}))

(defn- verify-one [query presto-row drill-row]
  (let [rows-match (schema/verify-row presto-row drill-row)]
    (when (not rows-match)
      (when-verbose 2 opts
        (println "Rows do not match!")
        (println "Presto row:" presto-row)
        (println "drill  row:" drill-row)))
    rows-match))

(defn- verify [query presto-data drill-data]
  (let [verified-rows (map #(verify-one query %1 %2) presto-data drill-data)
        amounts-equal (= (count presto-data) (count drill-data))]
    (when (and (>= (:verbose @opts) 1) (not amounts-equal))
      (println "Number of data rows do not match. Presto:" (count presto-data) "Drill:" (count drill-data)))
    (cond
      (and (= 0 (count presto-data))
           (= 0 (count drill-data)))
      true

      (empty? verified-rows)
      false

      :else
      (and 
        ;; ANDing in this order to compare all rows even if the number of rows don't match
        (every? identity (doall verified-rows)) ;; It should rather be (apply and verified-rows) but `and` is a macro and can't be applied
        amounts-equal)))) 

(defn- build-pairs [list]
  "Group items pair-wise: [1 2 3 4] -> [[1 2] [2 3] [3 4]]"
  (map vector list (rest list)))

(defn calculate-intervals [min-ts max-ts interval]
  (let [intervals (concat (take-while #(t/before? % max-ts) (p/periodic-seq min-ts interval)) [max-ts])]
    (build-pairs intervals)))

(defn run-query [db q min-ts max-ts]
  (when-verbose 1 opts
    (println "Running" (schema/query q min-ts max-ts) "on" (:subname db)))
  (schema/process-data q (jdbc/query db (schema/query q min-ts max-ts))))

(defn- run-and-verify [schema [min-ts max-ts]]
  (let [p-query (presto/schema->Query schema)
        d-query (drill/schema->Query schema)
        p-data (run-query (presto/db (:presto @opts)) p-query (c/to-long min-ts) (c/to-long max-ts))
        d-data (run-query (drill/db (:drill @opts)) d-query (c/to-long min-ts) (c/to-long max-ts))
        ]
    (when (:print-results @opts)
      (when-not (:no-presto @opts)
        (println "Presto results\n")
        (pp/pprint p-data)
        (println "--------------"))
      (when-not (:no-drill @opts)
        (println "Drill results\n")
        (pp/pprint d-data)))
    (verify schema p-data d-data)))

(defn -main
  [& args]
  (reset! opts (cli/parse args))
  (let [filename (:file @opts)
        schm     (schema/parse (slurp filename))
        intervals (calculate-intervals (:min @opts) (:max @opts) (:interval @opts))
        results (doall (map #(run-and-verify schm %1) intervals))
        return-code (if (every? identity results) 0 1)]
    (when (= 1 return-code)
      (println "FAIL: Data is not the same!"))
    (System/exit return-code)))

