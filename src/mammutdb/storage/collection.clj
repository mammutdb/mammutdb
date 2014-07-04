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
            [mammutdb.storage.types :as stypes]
            [mammutdb.storage.protocols :as sproto]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.errors :as serr]
            [mammutdb.storage.connection :as sconn])
  (:import mammutdb.storage.types.DocumentCollection))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *collection-safe-rx* #"[\w\_\-]+")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-type DocumentCollection
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

  (get-database [coll]
    (.-database coll))

  sproto/Droppable
  (drop! [coll con]
    (let [collnane (sproto/get-collection-name coll)
          dbname   (sproto/get-database-name (sproto/get-database coll))
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
       (t/right)))))

(defn safe-name?
  "Parse collection name and return a safe
  string or nil."
  [name]
  (if (re-matches *collection-safe-rx* name)
    (t/right true)
    (e/error :collection-name-unsafe)))

(defn exists?
  "Check if collection with given name, are
  previously created."
  [db name con]
  (let [sql "SELECT EXISTS(SELECT * FROM mammutdb_collections
             WHERE name = ? AND database = ?);"
        sql [sql name (sproto/get-database-name db)]]
    (m/mlet [res (sconn/query-first con sql)]
      (if (:exists res)
        (m/return name)
        (e/error :collection-not-exists
                 (format "Collection '%s' does not exists" name))))))

(defn- make-mainstore-sql
  [coll]
  (->> (sproto/get-mainstore-tablename coll)
       (format "CREATE TABLE %s (
                 id uuid UNIQUE PRIMARY KEY,
                 data json,
                 revision uuid,
                 created_at timestamp with time zone);")))

(defn- make-revisions-sql
  [coll]
  (->> (sproto/get-revisions-tablename coll)
       (format "CREATE TABLE %s (
                 id uuid DEFAULT uuid_generate_v1() UNIQUE PRIMARY KEY,
                 data json,
                 revision uuid DEFAULT uuid_generate_v1(),
                 created_at timestamp with time zone);")))

(defn- make-persist-collection-sql
  [db coll type]
  ["INSERT INTO mammutdb_collections (type, name, database)
    VALUES (?, ?, ?);"
   type
   (sproto/get-collection-name coll)
   (sproto/get-database-name db)])

(defn create-document-collection
  [db name con]
  (m/mlet [safe?   (safe-name? name)
           :let    [coll (stypes/->doc-collection db name)
                    sql1 (make-mainstore-sql coll)
                    sql2 (make-revisions-sql coll)
                    sql3 (make-persist-collection-sql db coll "document")]]
    (serr/catch-sqlexception
     (j/execute! con sql1)
     (j/execute! con sql2)
     (j/execute-prepared! con sql3)
     (m/return coll))))

(defn create!
  [type db name con]
  (case (keyword type)
    :document (create-document-collection db name con)))

(defn get-by-name
  "Get collection by its name."
  [db name con]
  (m/mlet [safe? (safe-name? name)
           :let  [sql ["SELECT * FROM mammutdb_collections
                        WHERE name = ? AND database = ?"
                       name
                       (sproto/get-database-name db)]]
           rev   (sconn/query-first con sql)]
    (if rev
      (m/return (stypes/->doc-collection db name))
      (e/error :collection-not-exists
               (format "Collection '%s' does not exists" name)))))

(defn drop!
  [coll con]
  (sproto/drop! coll con))
