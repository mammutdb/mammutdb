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

(ns mammutdb.storage.database
  (:require [cats.types :as t]
            [cats.core :as m]
            [jdbc.core :as j]
            [clojure.string :as str]
            [mammutdb.core.edn :as edn]
            [mammutdb.core.errors :as e]
            [mammutdb.types.database :as tdb]
            [mammutdb.storage.collection :as scoll]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.errors :as serr]
            [mammutdb.storage.connection :as sconn]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype Database [name]
  Object
  (toString [_]
    (with-out-str
      (print [(str/lower-case name)])))

  (equals [_ other]
    (= name (.-name other)))

  tdb/Database
  (get-name [_]
    (str/lower-case name))

  (get-collections [db con]
    (let [sql ["SELECT name FROM mammutdb_collections
                WHERE database = ? ORDER BY name"
               (tdb/get-name db)]]
      (m/mlet [results (sconn/query con sql)]
        (-> (fn [record] (scoll/->collection db (:name record)))
            (mapv results)
            (m/return)))))

  (drop! [db con]
    ;; Drop all collections
    (let [sql1 ["DELETE FROM mammutdb_databases WHERE name = ?;"
                (tdb/get-name db)]]
      (serr/catch-sqlexception
       (j/execute-prepared! con sql1)
       (t/right)))))

(alter-meta! #'->Database assoc :no-doc true :private true)

(defn ->database
  "Default constructor for database instances."
  [^String name]
  (Database. name))

(defn safe-name?
  "Parse collection name and return a safe
  string or nil."
  [name]
  (if (re-matches tdb/*database-safe-rx* name)
    (t/right true)
    (e/error :database-name-unsafe)))

(defn exists?
  "Check if database with given name, are
  previously created."
  [name con]
  (let [sql "SELECT EXISTS(SELECT * FROM mammutdb_databases WHERE name = ?);"
        sql [sql name]]
    (m/mlet [res (sconn/query-first con sql)]
      (if (:exists res)
        (m/return (->database name))
        (e/error :database-not-exists
                 (format "Database '%s' does not exists" name))))))

(defn create
  [name con]
  (let [sql ["INSERT INTO mammutdb_databases (name) VALUES (?)" name]]
    (m/mlet [safe? (safe-name? name)]
      (serr/catch-sqlexception
       (j/execute-prepared! con sql)
       (t/right (->database name))))))

(defn drop!
  [db con]
  (tdb/drop! db con))

