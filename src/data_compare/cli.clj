(ns data-compare.cli
  (:require [clojure.tools.cli :refer [parse-opts]]))

(defn- positive? [x]
  (>= x 0))

(def options
  [["-h" "--help" "Print this help"]
   ["-m" "--min TIMESTAMP" "Lower bound for timestamps"
    :parse-fn #(Long/parseLong %)
    :validate [positive? "Min timestamp cannot be negative"]]
   ["-x" "--max TIMESTAMP" "Upper bound for timestamps"
    :parse-fn #(Long/parseLong %)
    :validate [positive? "Min timestamp cannot be negative"]]
   ["-f" "--file FILENAME" "Name of the file describing data columns"]
   ["-d" "--drill DRILL_ADDRESS" "Address of the drill server"]
   ["-p" "--presto PRESTO_ADDRESS" "Address of the presto server"]])

(defn- print-help-and-exit! [summary]
  (println summary)
  (System/exit 0))

(defn- required! [opt]
  (println "Error:" (name opt) "is required."))

(defn- verify [{:keys [options summary errors]}]
  (cond
    (:help options)
    (print-help-and-exit! summary)
    
    errors
    (do 
      (doall (map println errors)) 
      (print-help-and-exit! summary))
    
    (nil? (:min options)) 
    (do 
      (required! :min) 
      (print-help-and-exit! summary))

    (nil? (:max options)) 
    (do 
      (required! :max) 
      (print-help-and-exit! summary))

    (nil? (:file options)) 
    (do 
      (required! :file) 
      (print-help-and-exit! summary))

    (< (:max options) (:min options))
    (do
      (println "Min TS must be at lower or equal to max TS!")
      (System/exit 1)))
  options)

(defn parse [args]
  (let [opts (parse-opts args options)]
    (verify opts)))
