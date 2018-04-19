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
  (println "Presto data:" presto-data)
  (println "Drill  data:" drill-data)
  (map #(verify-one query %1 %2) presto-data drill-data))

(defn -main
  [& args]
  (let [opts (cli/parse args)
        min-ts   (:min opts)
        max-ts   (:max opts)
        filename (:file opts)
        schm     (schema/parse (slurp filename))
        p-query  (presto/schema->Query schm)
        _        (println "Running" (schema/query p-query min-ts max-ts) "on" (:subname (presto/db (:presto opts))))
        p-data   (jdbc/query (presto/db (:presto opts)) (schema/query p-query min-ts max-ts))
        d-query  (drill/schema->Query schm)
        _        (println "Running" (schema/query d-query min-ts max-ts) "on" (:subname (drill/db (:drill opts))))
        d-data   (jdbc/query (drill/db (:drill opts)) (schema/query d-query min-ts max-ts))
        ]
    (println (schema/query d-query min-ts max-ts))
    (verify schm p-data d-data))

  ;(println (first (jdbc/query drill/db  "select * from kudu.netflows limit 1")))
  ;(println (first (jdbc/query presto/db "select timestamp from hyena.cs where timestamp > 0 limit 1")))
  ;(println (first (jdbc/query presto/db (schema/query p-query min-ts max-ts))))
  ;(println (jdbc/query presto/db (schema/query p-query min-ts max-ts)))
  )
