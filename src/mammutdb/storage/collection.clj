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

(ns mammutdb.storage.collection
  (:require [cats.types :as t]
            [cats.core :as m]
            [jdbc.core :as j]
            [clojure.string :as str]
            [mammutdb.core.edn :as edn]
            [mammutdb.core.errors :as e]
            [mammutdb.types.collection :as tcoll]
            [mammutdb.types.database :as tdb]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.errors :as serr]
            [mammutdb.storage.connection :as sconn]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype DocumentCollection [database name]
  Object
  (toString [_]
    (with-out-str
      (print [(tdb/get-name database) name])))

  (equals [_ other]
    (and (= name (.-name other))
         (= database (.database other))))

  tcoll/Collection
  (get-mainstore-tablename [_]
    (format "%s_%s_storage"
            (tdb/get-name database)
            (str/lower-case name)))

  (get-revisions-tablename [_]
    (format "%s_%s_revisions"
            (tdb/get-name database)
            (str/lower-case name)))

  (get-database [_]
    database)

  (drop! [c con]
    (let [sql1 ["DELETE FROM mammutdb_collections
                 WHERE name = ? AND database = ?;"
                (str/lower-case name)
                (tdb/get-name database)]
          sql2 (format "DROP TABLE %s;" (tcoll/get-mainstore-tablename c))
          sql3 (format "DROP TABLE %s;" (tcoll/get-revisions-tablename c))]
      (serr/catch-sqlexception
       (j/execute! con sql3)
       (j/execute! con sql2)
       (j/execute-prepared! con sql1)
       (t/right)))))

(alter-meta! #'->DocumentCollection assoc :no-doc true :private true)

(defn ->collection
  "Default constructor for collection type."
  [database name]
  (DocumentCollection. database name))

(defn safe-name?
  "Parse collection name and return a safe
  string or nil."
  [name]
  (if (re-matches tcoll/*collection-safe-rx* name)
    (t/right true)
    (e/error :collection-name-unsafe)))

(defn exists?
  "Check if collection with given name, are
  previously created."
  [database name con]
  (let [sql "SELECT EXISTS(SELECT * FROM mammutdb_collections
             WHERE name = ? AND database = ?);"
        sql [sql name (tdb/get-name database)]]
    (m/mlet [res (sconn/query-first con sql)]
      (if (:exists res)
        (m/return name)
        (e/error :collection-not-exists
                 (format "Collection '%s' does not exists" name))))))

(defn create
  [database name con]
  (let [sql1 "CREATE TABLE %s (
               id uuid UNIQUE PRIMARY KEY,
               data json,
               revision uuid,
               created_at timestamp with time zone);"
        sql2 "CREATE TABLE %s (
               id uuid DEFAULT uuid_generate_v1() UNIQUE PRIMARY KEY,
               data json,
               revision uuid DEFAULT uuid_generate_v1(),
               created_at timestamp with time zone);"
        sql3 "INSERT INTO mammutdb_collections (name, database)
              VALUES (?, ?);"
        c    (->collection database name)
        sql2 (format sql2 (tcoll/get-revisions-tablename c))
        sql1 (format sql1 (tcoll/get-mainstore-tablename c))]
    (m/mlet [safe? (safe-name? name)]
      (serr/catch-sqlexception
       (j/execute! con sql1)
       (j/execute! con sql2)
       (j/execute-prepared! con [sql3 name (tdb/get-name database)])
       (m/return c)))))

(defn get-by-name
  "Get collection by its name."
  [database name con]
  (let [sql "SELECT * FROM mammutdb_collections
             WHERE name = ? AND database = ?"
        sql [sql name (tdb/get-name database)]]
    (m/mlet [rev (sconn/query-first con sql)]
      (if rev
        (m/return (->collection database name))
        (e/error :collection-not-exists
                 (format "Collection '%s' does not exists" name))))))

(defn drop!
  [coll con]
  (tcoll/drop! coll con))
