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
            [swiss.arrows :refer [-<>]]
            [mammutdb.core.error :as err]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.collection :as scoll]
            [mammutdb.storage.connection :as sconn])
  (:import mammutdb.storage.collection.JsonDocumentCollection))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Main Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: implement Droppable protocol

(deftype JsonDocument [collection id rev data createdat]
  java.lang.Object
  (toString [_]
    (with-out-str
      (print [id rev])))
  (equals [_ other]
    (and (= id (.-id other))
         (= rev (.-rev other))))

  sproto/Document
  (document->record [doc]
    {:id (.-id doc)
     :revision (.-rev doc)
     :data (.-data doc)})

  (get-collection [doc]
    (.-collection doc))

  sproto/DatabaseMember
  (get-database [doc]
    (sproto/get-database (.-collection doc))))

(defn- makesql-persist-document-on-mainstore
  [coll doc]
  (let [createdat (jc/to-sql-time (.-createdat doc))
        data      (json/from-native (.-data doc))]
    (-<> (sproto/get-mainstore-tablename coll)
         (format "INSERT INTO %s (id, data, revision, created_at)
                  VALUES (?, ?, ?, ?);" <>)
         (vector <> (.-id doc) data (.-rev doc) createdat))))

(defn- makesql-update-document-on-mainstore
  [coll doc]
  (let [createdat (jc/to-sql-time (.-createdat d))
        data      (json/from-native (.-data d))]
    (-<> (sproto/get-mainstore-tablename coll)
         (format "UPDATE %s SET revision = ?, created_at = ?,
                  data = ? WHERE id = ?;" <>)
         (vector <> (.-rev doc) createdat data (.-id doc)))))

(defn- persist-to-mainstore
  [coll doc timestamp update? conn]
  (let [createdat (jc/to-sql-time timestamp)
        data      (json/from-native (.-data doc))
        sql       (if update?
                    (makesql-update-document-on-mainstore coll doc)
                    (makesql-persist-document-on-mainstore coll doc))]
    (m/>>= (sconn/execute-prepared! conn sql)
           (fn [& args] (m/return doc)))))

(defn- persist-to-revisions
  [coll doc timestamp conn]
  (let [createdat (jc/to-sql-time timestamp)
        data      (json/from-native (.-data doc))
        sql       (-<> (sproto/get-revisions-tablename coll)
                       (format "INSERT INTO %s (data, created_at) VALUES (?, ?);" <>)
                       (vector <> (.-data doc) createdat))]
    (m/mlet [res  (sconn/execute-prepared! conn sql {:returning [:id :revision]})
             :let [res (first res)]]
      (m/return (->json-document coll
                                 (:id res)
                                 (:revision res)
                                 (.-data d)
                                 timestamp)))))

(extend-type JsonDocumentCollection
  sproto/DocumentStore
  (get-by-id [coll id conn]
    (m/mlet [rec (-<> (sproto/get-mainstore-tablename coll)
                      (format "SELECT * FROM %s WHERE id = ?;" <>)
                      (vector <> id)
                      (sconn/query-first conn <>))]
      (m/return (sproto/->record->document coll rec))))

  (persist! [coll doc conn]
    (let [timestamp (jt/now)
          forupdate (not (nil? (.-id doc)))]
      (m/>>= (t/just doc)
             #(persist-to-revisions coll % timestamp conn)
             #(persist-to-mainstore coll % timestamp forupdate conn))))

  (record->document [coll rec]
    (let [{:keys [id revision data created_at]} rec]
      (->> (jc/from-sql-time created_at)
           (JsonDocument. coll id revision data))))

  (->document [coll id rev data createdat]
    (JsonDocument. coll id rev data createdat)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Aliases
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ->document
  [& args]
  (apply sproto/->document args))

(defn persist!
  [& args]
  (apply sproto/persist! args))

(defn get-by-id!
  [& args]
  (apply sproto/get-by-id! args))
