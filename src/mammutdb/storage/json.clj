;; Copyright (c) 2014 Andrey Antukh <niwi@niwi.be>
;; All rights reserved.
;;
;; Redistribution and use in source and binary forms, with or without
;; modification, are permitted provided that the following conditions
;; are met:
;;
;; 1. Redistributions of source code must retain the above copyright
;;    notice, this list of conditions and the following disclaimer.
;; 2. Redistributions in binary form must reproduce the above copyright
;;    notice, this list of conditions and the following disclaimer in the
;;    documentation and/or other materials provided with the distribution.
;;
;; THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
;; IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
;; OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
;; IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
;; INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
;; NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
;; DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
;; THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
;; (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
;; THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

(ns mammutdb.storage.json
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
