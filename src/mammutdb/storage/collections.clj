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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *collection-safe-rx* #"[\w\_\-]+")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private sql-collection-exists
  (delay (slurp (io/resource "sql/query/collection-exists.sql"))))

(def ^:private sql-default-collection-store
  (delay (slurp (io/resource "sql/coll-schema/default-store.sql"))))

(def ^:private sql-default-collection-rev
  (delay (slurp (io/resource "sql/coll-schema/default-rev.sql"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utils
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro with-exception-as-either
  [& body]
  `(try
     (do ~@body)
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
  (let [sql (deref sql-collection-exists)]
    (with-exception-as-either
      (if-let [res (j/query-first conn [sql name])]
        (t/just name)
        (t/left {:type :fail :value :collection-does-not-exists})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic collection crud.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create
  [conn ^String name]
  (m/mlet [safe? (safe-name? name)
           :let [storage-tablename   (str name "_storage")
                 revisions-tablename (str name "_revisions")]]
    (let [sql1 (format @sql-default-collection-store storage-tablename)
          sql2 (format @sql-default-collection-rev revisions-tablename)]
      (with-exception-as-either
        (j/execute! conn sql1)
        (j/execute! conn sql2)
        (j/right true)))))

(defn drop
  [conn ^String name]
  (let [storage-tablename   (str name "_storage")
        revisions-tablename (str name "_revisions")]
    (with-exception-as-either
      (j/execute! conn (format "DROP TABLE %s;" revisions-tablename))
      (j/execute! conn (format "DROP TABLE %s;" storage-tablename))
      (j/right true))))

