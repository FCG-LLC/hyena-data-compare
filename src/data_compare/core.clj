(ns data-compare.core
  (require [data-compare.jdbc :as jdbc]
           [data-compare.drill :as drill]
           [data-compare.presto :as presto]
           [data-compare.cli :as cli]
           [data-compare.schema :as schema])
  (:gen-class))

(defn- verify-one [query presto-row drill-row]
  (println "Presto row:" presto-row)
  (println "drill  row:" drill-row)
  (schema/verify-row query presto-row)
  )

(defn- verify [query presto-data drill-data]
  (let [verified-rows (map #(verify-one query %1 %2) presto-data drill-data)]
    (if (empty? verified-rows)
      false
      (every? identity verified-rows)))) ;; It should rather be (apply and verified-rows) but `and` is a macro and can't be applied

(defn -main
  [& args]
  (let [opts (cli/parse args)
        min-ts   (:min opts)
        max-ts   (:max opts)
        filename (:file opts)
        schm     (schema/parse (slurp filename))
        p-query  (presto/schema->Query schm)
        p-data   (when-not (:no-presto opts)
                   (println "Running" (schema/query p-query min-ts max-ts) "on" (:subname (presto/db (:presto opts))))
                   (jdbc/query (presto/db (:presto opts)) (schema/query p-query min-ts max-ts)))
        p-processed (schema/process-data p-query p-data)
        d-query  (drill/schema->Query schm)
        d-data   (when-not (:no-drill opts)
                   (println "Running" (schema/query d-query min-ts max-ts) "on" (:subname (drill/db (:drill opts))))
                   (jdbc/query (drill/db (:drill opts)) (schema/query d-query min-ts max-ts)))
        d-processed (schema/process-data d-query d-data)
        ]
    (when (:print-results opts)
      (when-not (:no-presto opts)
        (println "Presto results\n" p-data)
        (println "--------------"))
      (when-not (:no-drill opts)
        (println "Drill results\n" d-data)
        (doall (map println d-processed))))
    (println (verify schm p-processed d-processed)))

  ;(println (first (jdbc/query drill/db  "select * from kudu.netflows limit 1")))
  ;(println (first (jdbc/query presto/db "select timestamp from hyena.cs where timestamp > 0 limit 1")))
  ;(println (first (jdbc/query presto/db (schema/query p-query min-ts max-ts))))
  ;(println (jdbc/query presto/db (schema/query p-query min-ts max-ts)))
  )
