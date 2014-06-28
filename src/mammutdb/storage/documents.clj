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

(ns mammutdb.storage.documents
  (:require [cats.types :as t]
            [cats.core :as m]
            [jdbc.core :as j]
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
  [{:keys [id revision data] :as record}]
  (->document id revision data))

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
  (-> (->> (scoll/get-mainstore-tablename c)
           (format (:document-by-id @sql-queries)))
      (vector id)
      (t/right)))

(defn makesql-persist-document-on-mainstore
  [c d]
  (-> (->> (scoll/get-mainstore-tablename c)
           (format (:persist-document-on-mainstore @sql-ops)))
      (vector (.-id d) (.-data d) (.-rev d) (.-createdat d))
      (t/right)))

(defn makesql-persist-document-on-revisions
  [c data t]
  (-> (->> (scoll/get-revisions-tablename c)
           (format (:persist-document-on-revisions @sql-ops)))
      ;; TODO: convert to t to Date
      (vector (json/from-native data) t)
      (t/right)))

(defn makesql-update-document-on-mainstore
  [c d]
  (-> (->> (scoll/get-mainstore-tablename c)
           (format (:update-document-on-mainstore @sql-ops)))
      ;; TODO: convert to t to Date
      (vector (.-rev d) (.-createdat d) (json/from-native (.-data d)) (.-id d))
      (t/right)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Documents crud
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-by-id
  [conn c id]
  (err/catch-to-either
   (m/mlet [sql (makesql-get-document-by-id c id)
            rec (err/wrap-to-either (j/query-first conn sql))]
     (m/return (record->document rec)))))

(defn- persist-to-revisions
  [conn c d t]
  (m/mlet [sql (makesql-persist-document-on-revisions c (.-data d) t)
           res (err/wrap-to-either
                (j/execute-prepared! sql {:returning [:id :revision]}))]
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
            (partial persist-revision conn c timestamp)
            (partial persist-mainstore conn c timestamp forupdate)))))
