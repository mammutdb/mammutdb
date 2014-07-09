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
            [mammutdb.storage.json :as json]
            [mammutdb.storage.protocols :as sproto]
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
      (print [id rev data createdat])))

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

;; The following code is the concrete implementation
;; of persist! for json based documents. At this momment
;; it located on this file but in future it can be moved
;; to own namespace, allowing so support multiple
;; document types.

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
  (let [createdat (jc/to-sql-time (.-createdat doc))
        data      (json/from-native (.-data doc))]
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
  [coll doc timestamp update? conn]
  (let [createdat  (jc/to-sql-time timestamp)
        data       (json/from-native (.-data doc))
        tablename  (sproto/get-revisions-tablename coll)
        sql-create (-<> tablename
                        (format "INSERT INTO %s (data, created_at) VALUES (?,?);" <>)
                        (vector <> data createdat))
        sql-update (-<> tablename
                        (format "INSERT INTO %s (id, data, created_at) VALUES (?,?,?);" <>)
                        (vector <> (.-id doc) data createdat))
        sql        (if update? sql-update sql-create)]
    (m/mlet [res  (sconn/execute-prepared! conn sql {:returning [:id :revision]})
             :let [res (first res)]]
      (m/return (sproto/->document coll
                                   (:id res)
                                   (:revision res)
                                   (.-data doc)
                                   timestamp)))))

(extend-type JsonDocumentCollection
  sproto/DocumentStore
  (persist! [coll doc conn]
    (let [timestamp (jt/now)
          forupdate (not (nil? (.-id doc)))]
      (m/>>= (t/just doc)
             #(persist-to-revisions coll % timestamp forupdate conn)
             #(persist-to-mainstore coll % timestamp forupdate conn))))

  (record->document [coll rec]
    (let [{:keys [id revision data created_at]} rec]
      (->> (jc/from-sql-time created_at)
           (JsonDocument. coll id revision data))))

  (->document [coll id rev data createdat]
    (JsonDocument. coll id rev data createdat))

  sproto/DocumentQueryable
  (get-by-id [coll id conn]
    (m/mlet [rec (-<> (sproto/get-mainstore-tablename coll)
                      (format "SELECT * FROM %s WHERE id = ?;" <>)
                      (vector <> id)
                      (sconn/query-first conn <>))]
      (m/return (sproto/record->document coll rec))))

  (get-by-rev [coll id rev conn]
    (m/mlet [rec (-<> (sproto/get-mainstore-tablename coll)
                      (format "SELECT * FROM %s WHERE id = ? AND revision = ?;" <>)
                      (vector <> id rev)
                      (sconn/query-first conn <>))]
      (m/return (sproto/record->document coll rec)))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Aliases
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ->document
  [& args]
  (apply sproto/->document args))

(defn persist!
  [& args]
  (apply sproto/persist! args))

(defn get-by-id
  [& args]
  (apply sproto/get-by-id args))
