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

(ns mammutdb.storage.document
  (:require [cats.types :as t]
            [cats.core :as m]
            [jdbc.core :as j]
            [clj-time.core :as jt]
            [clj-time.coerce :as jc]
            [mammutdb.core.edn :as edn]
            [mammutdb.core.error :as err]
            [mammutdb.storage.collections :as scoll]
            [mammutdb.storage.json :as json]))

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

(deftype Document [id rev data createdat]
  Object
  (toString [_]
    (with-out-str
      (print [id rev])))

  (equals [_ other]
    (and (= id (.-id other))
         (= rev (.-rev other)))))

(alter-meta! #'->Document assoc :no-doc true :private true)

(defn ->document
  "Default constructor for document type."
  ([data]
     (Document. nil nil data nil))
  ([id rev data created-at]
     (Document. id rev data created-at)))

(defn record->document
  [{:keys [id revision data created_at] :as record}]
  (->> (jc/from-sql-time created_at)
       (->document id revision data)))

(defn document->record
  [doc]
  {:id (.-id doc)
   :revision (.-rev doc)
   :data (.-data doc)})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL Constructors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn makesql-get-document-by-id
  "Build sql query for obtain document by its id."
  [c id]
  (let [tablename (scoll/get-mainstore-tablename c)]
    (-> (format (:document-by-id @sql-queries) tablename)
        (vector id)
        (t/right))))

(defn makesql-persist-document-on-mainstore
  [c d]
  (let [createdat (jc/to-sql-time (.-createdat d))
        data      (json/from-native (.-data d))
        tablename (scoll/get-mainstore-tablename c)]
    (-> (format (:persist-document-on-mainstore @sql-ops) tablename)
        (vector (.-id d) data (.-rev d) createdat)
        (t/right))))

(defn makesql-persist-document-on-revisions
  [c data t]
  (let [createdat (jc/to-sql-time t)
        data      (json/from-native data)
        tablename (scoll/get-revisions-tablename c)]
    (-> (format (:persist-document-on-revisions @sql-ops) tablename)
        (vector data createdat)
        (t/right))))

(defn makesql-update-document-on-mainstore
  [c d]
  (let [createdat (jc/to-sql-time (.-createdat d))
        data      (json/from-native (.-data d))
        tablename (scoll/get-mainstore-tablename c)]
    (-> (format (:update-document-on-mainstore @sql-ops) tablename)
        (vector (.-rev d) createdat data (.-id d))
        (t/right))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Documents crud
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-by-id
  [con c id]
  (err/catch-to-either
   (m/mlet [sql (makesql-get-document-by-id c id)
            rec (err/wrap-to-either (j/query-first con sql))]
     (m/return (record->document rec)))))

(defn- persist-to-revisions
  [con c d t]
  (m/mlet [sql (makesql-persist-document-on-revisions c (.-data d) t)
           res (err/wrap-to-either
                (j/execute-prepared! con sql {:returning [:id :revision]}))]
    (let [res (first res)]
      (m/return (->document (:id res) (:revision res) (.-data d) t)))))

(defn- persist-to-mainstore
  [conn c t update? d]
  (err/catch-to-either
   (m/mlet [sql (if update?
                  (makesql-update-document-on-mainstore c d)
                  (makesql-persist-document-on-revisions c d))
            res (err/wrap-to-either
                 (j/execute-prepared! conn sql))]
     (m/return d))))

(defn persist
  "Persist document in a collection."
  [conn c d]
  (err/catch-to-either
   (let [timestamp (jt/now)
         forupdate (not (nil? (.-id d)))]
     (m/>>= (t/right d)
            (partial persist-to-revisions conn c timestamp)
            (partial persist-to-mainstore conn c timestamp forupdate)))))
