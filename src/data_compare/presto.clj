(ns data-compare.presto)

(def db {:classname "com.facebook.presto.jdbc.PrestoDriver"
         :subprotocol "presto"
         :subname "//localhost:8080/hyena/hyena"
         :user "not important"})
