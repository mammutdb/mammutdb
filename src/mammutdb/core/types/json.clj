(ns mammutdb.core.types.json
  (:require [cheshire.core :as json]
            [jdbc.types :as jdbc-types)
  (:import org.postgresql.util.PGobject))

(defrecord Json [value]
  jdbc-types/ISQLType

  (set-stmt-parameter! [self conn stmt index]
    (.setObject stmt index (as-sql-type self conn)))

  (as-sql-type [self conn]
    (doto (PGobject.)
      (.setType "json")
      (.setValue (json/generate-string value)))))

(alter-meta! #'->Json assoc :no-doc true :private true)
(alter-meta! #'map->Json assoc :no-doc true :private true)

(extend-protocol jdbc-types/ISQLResultSetReadColumn
  PGobject
  (from-sql-type [pgobj conn metadata i]
    (let [type  (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        "json" (json/parse-string value)
        :else value))))

(defn json [data] (map->JsonType {:value data}))
