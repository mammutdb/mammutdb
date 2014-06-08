(ns mammutdb.core.types.json
  "PostgreSQL compatible json object type."
  (:require [cheshire.core :as json]
            [jdbc.types :as jdbc-types)
  (:import org.postgresql.util.PGobject))

(deftype JsonObject [v]
  jdbc-types/ISQLType

  (set-stmt-parameter! [self conn stmt index]
    (.setObject stmt index (as-sql-type self conn)))

  (as-sql-type [self conn]
    (doto (PGobject.)
      (.setType "json")
      (.setValue v))))

(alter-meta! #'->Json assoc :no-doc true :private true)
(alter-meta! #'map->Json assoc :no-doc true :private true)

(extend-protocol jdbc-types/ISQLResultSetReadColumn
  PGobject
  (from-sql-type [pgobj conn metadata i]
    (let [type  (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        "json" (JsonObject. value)
        :else value))))

(defn from-native
  "PostgreSQL compatible json object constructor."
  [v]
  (JsonObject. (json/generate-string v))

(defn to-native
  "Return native clojure data from json object."
  [v]
  (json/parse-string (.v v)))

