(ns data-compare.core
  (require [data-compare.jdbc :as jdbc]
           [data-compare.drill :as drill]
           [data-compare.presto :as presto]
           [data-compare.cli :refer [parse]])
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [opts (parse args)
        min-ts   (:min opts)
        max-ts   (:max opts)
        filename (:file opts)
        ]
    (println min-ts)
    (println max-ts)
    (println filename))
  (println "Hello, World!")
  ;(println (first (jdbc/query drill/db  "select * from kudu.netflows limit 1")))
  ;(println (first (jdbc/query presto/db "select timestamp from hyena.cs where timestamp > 0 limit 1")))
  )
