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
            [mammutdb.storage.database :as sdb]
            [mammutdb.storage.protocols :as sproto]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.errors :as serr]
            [mammutdb.storage.connection :as sconn])
  (:import mammutdb.storage.database.Database))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *collection-safe-rx* #"[\w\_\-]+")
(declare safe-name?)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Collections Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- make-mainstore-sql
  [coll]
  (->> (sproto/get-mainstore-tablename coll)
       (format "CREATE TABLE %s (
                 id uuid UNIQUE PRIMARY KEY,
                 data json,
                 revision uuid,
                 created_at timestamp with time zone
                );")))

(defn- make-revisions-sql
  [coll]
  (->> (sproto/get-revisions-tablename coll)
       (format "CREATE TABLE %s (
                 id uuid DEFAULT uuid_generate_v1(),
                 data json,
                 revision uuid DEFAULT uuid_generate_v1(),
                 created_at timestamp with time zone,
                 UNIQUE (id, revision)
                );")))

(defn- make-persist-collection-sql
  [db coll type]
  ["INSERT INTO mammutdb_collections (type, name, database)
    VALUES (?, ?, ?);"
   (name type)
   (sproto/get-collection-name coll)
   (sproto/get-database-name db)])

(defn- create-json-collection!
  [db name con]
  (m/mlet [safe? (safe-name? name)
           :let  [coll (sproto/->collection db name :json)
                  sql1 (make-mainstore-sql coll)
                  sql2 (make-revisions-sql coll)
                  sql3 (make-persist-collection-sql db coll :json)]]
    (serr/catch-sqlexception
     (j/execute! con sql1)
     (j/execute! con sql2)
     (j/execute-prepared! con sql3)
     (m/return coll))))

(deftype JsonDocumentCollection [database name]
  java.lang.Object
  (toString [_]
    (with-out-str
      (print [(sproto/get-database-name database) name])))

  (equals [_ other]
    (and (= name (.-name other))
         (= database (.database other))))

  sproto/Collection
  (get-mainstore-tablename [coll]
    (format "%s_%s_storage"
            (sproto/get-database-name (.-database coll))
            (sproto/get-collection-name coll)))

  (get-revisions-tablename [coll]
    (format "%s_%s_revisions"
            (sproto/get-database-name (sproto/get-database coll))
            (sproto/get-collection-name coll)))

  (get-collection-name [coll]
    (.-name coll))

  sproto/Droppable
  (drop! [coll con]
    (let [collnane (sproto/get-collection-name coll)
          dbname   (sproto/get-database-name database)
          tblmain  (sproto/get-mainstore-tablename coll)
          tblrev   (sproto/get-revisions-tablename coll)
          sql1     ["DELETE FROM mammutdb_collections
                     WHERE name = ? AND database = ?;"
                     collnane
                     dbname]
          sql2     (format "DROP TABLE %s;" tblmain)
          sql3     (format "DROP TABLE %s;" tblrev)]
      (serr/catch-sqlexception
       (j/execute! con sql3)
       (j/execute! con sql2)
       (j/execute-prepared! con sql1)
       (t/right))))

  sproto/DatabaseMember
  (get-database [coll]
    (.-database coll)))

(extend-type Database
  sproto/CollectionStore
  (collection-exists-by-name? [db name conn]
    (let [sql "SELECT EXISTS(SELECT * FROM mammutdb_collections
               WHERE name = ? AND database = ?);"
          sql [sql name (sproto/get-database-name db)]]
    (m/mlet [res (sconn/query-first conn sql)]
      (if (:exists res)
        (m/return name)
        (e/error :collection-not-exists
                 (format "Collection '%s' does not exists" name))))))

  (get-collection-by-name [db name conn]
    (m/mlet [safe? (safe-name? name)
             :let  [sql ["SELECT * FROM mammutdb_collections
                          WHERE name = ? AND database = ?"
                         name
                         (sproto/get-database-name db)]]
             rev   (sconn/query-first conn sql)]
      (if rev
        (m/return (sproto/->collection db name :json))
        (e/error :collection-not-exists
                 (format "Collection '%s' does not exists" name)))))

  (create-collection! [db name type conn]
    (case type
      :json (create-json-collection! db name conn)))

  (->collection [db name type]
    (case type
      :json (JsonDocumentCollection. db name))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Aliases
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ->collection
  [db name type]
  (sproto/->collection db name type))

(defn safe-name?
  "Parse collection name and return a safe
  string or nil."
  [name]
  (if (re-matches *collection-safe-rx* name)
    (t/right true)
    (e/error :collection-name-unsafe)))

(defn exists?
  [db name conn]
  (sproto/collection-exists-by-name? db name conn))

(defn get-by-name
  "Get collection by its name."
  [db name conn]
  (sproto/get-collection-by-name db name conn))

(defn create!
  [db name type conn]
  (sproto/create-collection! db name type conn))

(defn drop!
  [coll con]
  (sproto/drop! coll con))
