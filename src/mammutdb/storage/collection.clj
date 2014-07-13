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
            [swiss.arrows :refer [-<>]]
            [mammutdb.core.edn :as edn]
            [mammutdb.core.errors :as e]
            [mammutdb.storage.database :as sdb]
            [mammutdb.storage.protocols :as sp]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.errors :as serr]
            [mammutdb.storage.connection :as sconn])
  (:import mammutdb.storage.database.Database))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(declare ->collection)
(declare record->collection)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Collections Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype JsonDocumentCollection [database name createdat metadata]
  java.lang.Object
  (toString [_]
    (with-out-str
      (print [(sp/get-database-name database) name])))

  (equals [_ other]
    (and (= name (.-name other))
         (= database (.database other))))

  sp/Collection

  (get-mainstore-tablename [coll]
    (format "%s_%s_storage"
            (sp/get-database-name (.-database coll))
            (sp/get-collection-name coll)))

  (get-revisions-tablename [coll]
    (format "%s_%s_revisions"
            (sp/get-database-name (sp/get-database coll))
            (sp/get-collection-name coll)))

  (get-collection-name [coll]
    (.-name coll))

  sp/Droppable

  (drop [coll con]
    (let [collnane (sp/get-collection-name coll)
          dbname   (sp/get-database-name database)
          tblmain  (sp/get-mainstore-tablename coll)
          tblrev   (sp/get-revisions-tablename coll)
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

  sp/DatabaseMember
  (get-database [coll]
    (.-database coll))

  sp/Serializable
  (to-plain-object [collection]
    {:id (.-name collection)
     :name (.-name collection)
     :createdAt (.-createdat collection)
     :database (.-name (.-database collection))
     :type :json}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database Extension
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- makesql-mainstore-createtable
  [coll]
  (->> (sp/get-mainstore-tablename coll)
       (format "CREATE TABLE %s (
                 id varchar(2048) UNIQUE PRIMARY KEY,
                 data json,
                 revid bigint,
                 revhash varchar(255),
                 created_at timestamp with time zone
                );")))

(defn- makesql-revisions-createtable
  [coll]
  (->> (sp/get-revisions-tablename coll)
       (format "CREATE TABLE %s (
                 id varchar(2048),
                 data json,
                 revid bigint,
                 revhash varchar(255),
                 created_at timestamp with time zone,
                 UNIQUE (id, revid, revhash)
                );")))

(defn- makesql-register-collection
  [db coll type]
  ["INSERT INTO mammutdb_collections (type, name, database)
    VALUES (?, ?, ?);"
   (name type)
   (sp/get-collection-name coll)
   (sp/get-database-name db)])

(defn- create-json-collection
  [db name con]
  ;; TODO: make collection instance with retrieved data
  ;; after collection creation in postgresql
  (let [coll (->collection db name :json)
        sql1 (makesql-mainstore-createtable coll)
        sql2 (makesql-revisions-createtable coll)
        sql3 (makesql-register-collection db coll :json)]
    (serr/catch-sqlexception
     (j/execute! con sql1)
     (j/execute! con sql2)
     (j/execute-prepared! con sql3)
     (t/right coll))))

(extend-type Database
  sp/CollectionStore

  (collection-exists-by-name? [db name conn]
    (m/mlet [:let [dbname (sp/get-database-name db)]
             res  (->> ["SELECT EXISTS(SELECT * FROM mammutdb_collections
                         WHERE name = ? AND database = ?);" name dbname]
                       (sconn/query-first conn))]
      (if (:exists res)
        (m/return name)
        (e/error :collection-does-not-exist
                 (format "Collection '%s' does not exist" name)))))

  (get-all-collections [db conn]
    (m/>>= (->> ["SELECT * FROM mammutdb_collections
                  WHERE database = ?;"
                 (sp/get-database-name db)]
                (sconn/query conn))
           (fn [results]
             (m/return (mapv (partial record->collection db) results)))))

  (get-collection-by-name [db name conn]
    (m/>>= (->> ["SELECT * FROM mammutdb_collections
                  WHERE name = ? AND database = ?" name
                  (sp/get-database-name db)]
                (sconn/query-first conn))
           (fn [record]
             (if record
               (m/return (record->collection db record))
               (e/error :collection-does-not-exist
                        (format "Collection '%s' does not exist" name))))))

  (create-collection [db name type conn]
    (case type
      :json (create-json-collection db name conn))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: add optional metadata field that is now supported
;; by storage but not by the storage api.

(defn ->collection
  ([db name type]
     (->collection db name type nil {}))
  ([db name type createdat metadata]
     (case type
       :json (JsonDocumentCollection. db name createdat metadata))))

(defn record->collection
  [db {:keys [name type metadata created_at]}]
  (->collection db name (keyword type) created_at metadata))

(defn collection?
  [coll]
  (satisfies? sp/Collection coll))

(defn get-all-collections
  [db conn]
  (sp/get-all-collections db conn))

(defn collection-exists?
  [db name conn]
  (sp/collection-exists-by-name? db name conn))

(defn get-collection-by-name
  "Get collection by its name."
  [db name conn]
  (sp/get-collection-by-name db name conn))

(defn create-collection
  [db name type conn]
  (sp/create-collection db name type conn))

(defn drop-collection
  [coll con]
  (sp/drop coll con))
