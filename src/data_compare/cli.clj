(ns data-compare.cli
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clj-time.coerce :as c]
            [clj-time.core :as t]))

(defn- positive? [x]
  (>= x 0))

(defmacro when-verbose [verbosity opts & body]
  `(when (>= (:verbose (deref ~opts)) ~verbosity)
    ~@body))

(def options
  [["-h" "--help" "Print this help"]
   ["-m" "--min TIMESTAMP" "Lower bound for timestamps (in milliseconds)" 
    :parse-fn #(c/from-long (Long/parseLong %))]
   ["-x" "--max TIMESTAMP" "Upper bound for timestamps (in milliseconds)"
    :parse-fn #(c/from-long (Long/parseLong %))]
   ["-f" "--file FILENAME" "Name of the file describing data columns" :default "schema.csv"]
   ["-d" "--drill DRILL_ADDRESS" "Address of the drill server"]
   ["-p" "--presto PRESTO_ADDRESS:PRESTO_PORT" "Address of the presto server"]
   ["-i" "--interval INTERVAL" "Interval for a single query (in seconds)" 
    :parse-fn #(t/seconds (Long/parseLong %))]
   [nil "--no-drill" "Don't run a Drill query" :default false]
   [nil "--no-presto" "Don't run a Presto query" :default false]
   [nil "--print-results" "Print the query results on the console" :default false]
   ["-v" "--verbose" "Run with more output" 
    :default 0 
    :assoc-fn (fn [m k _] (update-in m [k] inc))]])

(defn- print-help-and-exit! [summary]
  (println summary)
  (System/exit 0))

(defn- required! [opt]
  (println "Error:" (name opt) "is required."))


(defn- add-default-interval [options]
  (let [from-min-to-max (/ (- (c/to-long (:max options))
                              (c/to-long (:min options)))
                           1000)]
    (merge {:interval (t/seconds from-min-to-max)} options)))

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

    (t/before? (:max options) (:min options))
    (do
      (println "Min TS must be at lower or equal to max TS!")
      (System/exit 1)))
  options)

(defn parse [args]
  (let [opts (parse-opts args options)]
    (add-default-interval (verify opts))))
