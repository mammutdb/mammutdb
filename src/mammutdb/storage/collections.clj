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
            [jdbc.core :as j]
            [mammutdb.core.edn :as edn]
            [mammutdb.core.errors :as e]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.errors :as serr])
  (:refer-clojure :exclude [drop])
  (:import clojure.lang.BigInt))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *collection-safe-rx* #"[\w\_\-]+")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL Readers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private sql-ops
  (delay (edn/from-resource "sql/ops.edn")))

(def ^:private sql-queries
  (delay (edn/from-resource "sql/query.edn")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol CollectionType
  (get-mainstore-tablename [_] "Get storage tablename for collection")
  (get-revisions-tablename [_] "Get storage tablename for collection"))

;; Type that represents a collection
(deftype Collection [name]
  Object
  (toString [_]
    (with-out-str
      (print [name])))

  (equals [_ other]
    (= name (.-name other)))

  CollectionType
  (get-mainstore-tablename [_]
    (str name "_storage"))
  (get-revisions-tablename [_]
    (str name "_revisions")))

(alter-meta! #'->Collection assoc :no-doc true :private true)

(defn ->collection
  "Default constructor for collection type."
  [name]
  (Collection. name))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL Constructors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn makesql-collection-exists
  "Build sql query for check the existence
  of one collection by its name."
  [^String collname]
  (-> [(:collection-exists @sql-queries) collname]
      (t/right)))

(defn makesql-get-collection-by-name
  [^String name]
  (-> [(:collection-by-name @sql-queries) name]
      (t/right)))

(defn makesql-create-collection-mainstore
  "Build create ddl sql for collection storage table"
  [c]
  (->> (get-mainstore-tablename c)
       (format (:create-default-schema-mainstore @sql-ops))
       (t/right)))

(defn makesql-create-collection-revision
  "Build create ddl sql for collection revisions table."
  [c]
  (->> (get-revisions-tablename c)
       (format (:create-default-schema-revision @sql-ops))
       (t/right)))

(defn makesql-persist-collection-in-registry
  [c]
  (-> [(:persist-collection @sql-ops) (.-name c)]
      (t/right)))

(defn makesql-delete-collection-from-registry
  [c]
  (-> [(:delete-collection @sql-ops) (.-name c)]
      (t/right)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utils
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn safe-name?
  "Parse collection name and return a safe
  string or nil."
  [name]
  (if (re-matches *collection-safe-rx* name)
    (t/right true)
    (e/error :collection-name-unsafe)))

;; (defn exists?
;;   "Check if collection with given name, are
;;   previously created."
;;   [conn name]
;;   (m/mlet [sql (makesql-collection-exists name)
;;            res (serr/wrap (j/query-first conn sql))]
;;     (if (:exists res)
;;       (m/return name)
;;       (e/error :collection-not-exists
;;                (format "Collection '%s' does not exists" name)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic collection crud.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create
  [conn ^String name]
  (serr/catch-sqlexception
   (m/mlet [safe? (safe-name? name)
            :let  [c (->collection name)]
            sql1  (makesql-create-collection-mainstore c)
            sql2  (makesql-create-collection-revision c)
            sql3  (makesql-persist-collection-in-registry c)]
     (j/execute! conn sql1)
     (j/execute! conn sql2)
     (j/execute-prepared! conn sql3)
     (m/return c))))

(defn get-by-name
  "Get collection by its name."
  [conn ^String name]
  (serr/catch-sqlexception
   (m/mlet [sql (makesql-get-collection-by-name name)
            rev (serr/wrap (j/query-first conn sql))]
     (if rev
       (m/return (->collection name))
       (e/error :collection-not-exists
                (format "Collection '%s' does not exists" name))))))

(defn drop
  [con c]
  (serr/catch-sqlexception
   (m/mlet [sql (makesql-delete-collection-from-registry c)]
     (let [tablename-storage (get-mainstore-tablename c)
           tablename-rev     (get-revisions-tablename c)]
       (j/execute! con (format "DROP TABLE %s;" tablename-rev))
       (j/execute! con (format "DROP TABLE %s;" tablename-storage))
       (j/execute-prepared! con sql)
       (t/right)))))


;; (defn drop
;;   [con c]
;;   (m/mlet [sql  (makesql-delete-collection-from-registry c)
;;            :let [tablename-storage (get-mainstore-tablename c)
;;                  tablename-rev     (get-revisions-tablename c)]

;;            _    (sconn/execute (format "DROP TABLE %s;" tablename-rev))
;;            _    (sconn/execute (format "DROP TABLE %s;" tablename-storage))
;;            _    (sconn/execute-prepared sql)]
;;     (m/return (t/right))))
