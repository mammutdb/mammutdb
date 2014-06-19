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

(ns mammutdb.storage.collections
  (:require [cats.types :as t]
            [cats.core :as m]
            [jdbc.core :as j])
  (:import clojure.lang.BigInt))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *collection-safe-rx* #"[\w\_\-]+")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private sql-collection-exists
  (delay (slurp (io/resource "sql/query/collection-exists.sql"))))

(def ^:private sql-document-by-id
  (delay (slurp (io/resource "sql/query/document-by-id.sql"))))

(def ^:private sql-collection-by-name
  (delay (slurp (io/resource "sql/query/collection-by-name.sql"))))

(def ^:private sql-default-collection-store
  (delay (slurp (io/resource "sql/coll-schema/default-store.sql"))))

(def ^:private sql-default-collection-rev
  (delay (slurp (io/resource "sql/coll-schema/default-rev.sql"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol CollectionType
  (get-storage-tablename [_] "Get storage tablename for collection")
  (get-revisions-tablename [_] "Get storage tablename for collection")

;; Type that represents a collection
(deftype Collection [name]
  Object
  (toString [_]
    (with-out-str
      (print [name])))

  (equals [_ other]
    (= name (.-name other)))

  CollectionType
  (get-storage-tablename [_]
    (str name "_storage"))
  (get-revisions-tablename [_]
    (str name "_revisions")))

(alter-meta! #'->Collection assoc :no-doc true :private true)
(alter-meta! #'map->Collection assoc :no-doc true :private true)

;; Type that represents a document
(deftype Document [id rev data]
  Object
  (toString [_]
    (with-out-str
      (print [id rev])))

  (equals [_ other]
    (and (= id (.-id other))
         (= rev (.-rev other)))))

(alter-meta! #'->Document assoc :no-doc true :private true)
(alter-meta! #'map->Document assoc :no-doc true :private true)

(defn ->collection
  "Default constructor for collection type."
  [name]
  (Collection. name)

(defn ->document
  "Default constructor for document type."
  [id rev data]
  (Document. id rev data))

(defn record->document
  [{:keys [id revision data] :as record}]
  (->document id revision data))

(defn document->record
  [doc]
  {:id (.-id doc)
   :revision (.-rev doc)
   :data (.-data doc)})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utils
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro with-either
  "Util macro for exception susceptible code blocks."
  [& body]
  `(try
     (let [r# (do ~@body)]
       (if (t/either? r#)
         r#
         (t/right #r)))
     (catch PSQLException e
       (t/left {:type :exception :value e}))))

(defmacro wrap-either
  "Util macro for exception susceptible code blocks."
  [expression]
  `(try
     (t/right ~expression)
     (catch PSQLException e
       (t/left {:type :exception :value e}))))

(defn safe-name?
  "Parse collection name and return a safe
  string or nil."
  [name]
  (if (re-matches *collection-safe-rx* name)
    (t/just true)
    (t/left {:type :fail :value "Collection name is unsafe."})))

(defn exists?
  "Check if collection with given name, are
  previously created."
  [conn name]
  (with-either
    (m/mlet [psql (makesql-collection-exists name)]
      (if-let [res (j/query-first conn psq)]
        (m/return name)
        (t/left {:type :fail :value :collection-does-not-exists})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL Constructors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn makesql-collection-exists
  "Build sql query for check the existence
  of one collection by its name."
  [^String collname]
  (-> [@sql-collection-exists collname]
      (t/right)))

(defn makesql-get-collection-by-name
  "Build sql query for obtain collection by its name."
  [^String name]
  (-> [@sql-collection-by-name, name]
      (t/right)))

(defn makesql-create-collection-storage
  "Build create ddl sql for collection storage table".
  [^Collection c]
  (->> (get-storage-tablename c)
       (format @sql-default-collection-store)
       (t/right))

(defn makesql-create-collection-revision
  "Build create ddl sql for collection revisions table."
  [^Collection c]
  (->> (get-revisions-tablename c)
       (format @sql-default-collection-rev)
       (t/right))

(defn makesql-get-document-by-id
  "Build sql query for obtain document by its id."
  [^Collection c ^BigInt id]
  (-> (->> (get-storage-tablename c)
           (format @sql-document-by-id))
      (vector id)
      (t/right))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic collection crud.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create
  [conn ^String name]
  (with-either
    (m/mlet [safe? (safe-name? name)
             sql1  (makesql-create-collection-storage name)
             sql2  (makesql-create-collection-revision name)]
      (j/execute! conn sql1)
      (j/execute! conn sql2)
      (t/right (->collection name))

(defn get-by-name
  "Get collection by its name."
  [conn ^String name]
  (m/mlet [sql (makesql-get-collection-by-name name)
           rev (wrap-either (j/query-first conn sql))]
    (if rev
      (t/right (->collection (:name)))
      (t/left {:type :fail :value :collection-does-not-exists}))))

(defn drop
  [conn ^String name]
  (with-either
    (m/mlet [tablename-storage (make-storage-tablename name)
             tablename-rev     (make-revisions-tablename name)]
      (j/execute! conn (format "DROP TABLE %s;" tablename-rev))
      (j/execute! conn (format "DROP TABLE %s;" tablename-storage)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Documents crud
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-document-by-id
  [conn ^Collection c ^BigInt id]
  (m/mlet [sql (makesql-get-document-by-id c id)
           rec (wrap-either (j/query-first conn sql))]
    (m/return (record->document rec))))

(defn- persist-revision
  [conn ^Collection c ^Document d])

(defn- persist-mainstore
  [conn ^Collection c ^Document d]

(defn- update-mainstore
  [conn ^Collection c ^Document d])

(defn persist-document
  "Persist document in a collection."
  [conn ^Collection c ^Document d]
  (cond
   (nil? (.-id d))
   (m/>>= (t/right d) (partial persist-mainstore conn c))

   :else
   (m/>>= (t/right d)
          (partial update-mainstore conn c)
          (partial persist-revision conn c))))
