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
  (:require [cats.monad.either :as either]
            [cats.core :as m]
            [clojure.string :as str]
            [clj-time.core :as jt]
            [clj-time.coerce :as jc]
            [jdbc.core :as j]
            [swiss.arrows :refer [-<>]]
            [mammutdb.core.errors :as e]
            [mammutdb.logging :refer [log]]
            [mammutdb.core.uuid :refer [random-uuid]]
            [mammutdb.core.revs :refer [make-new-revhash]]
            [mammutdb.storage.errors :as serr]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.protocols :as sp]
            [mammutdb.storage.collection :as scoll]
            [mammutdb.storage.connection :as sconn])
  (:import mammutdb.storage.collection.JsonDocumentCollection))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Json Document Type
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype JsonDocument [coll id revid revhash data createdat]
  java.lang.Object
  (toString [_]
    (with-out-str
      (print [id revid revhash data createdat])))

  (equals [_ other]
    (and (= id (.-id other))
         (= revid (.-revid other))
         (= revhash (.-revhash other))))

  sp/Document
  (rev [document]
    (format "%s-%s" (.-revid document)
                    (.-revhash document)))

  (document->record [_]
    {:id id
     :revid revid
     :revhash revhash
     :data data})

  (get-collection [_] coll)

  sp/DatabaseMember
  (get-database [_]
    (sp/get-database coll))

  sp/Serializable
  (to-plain-object [document]
    (merge (.-data document)
           {:_id (.-id document)
            :_rev (sp/rev document)}))

  sp/Droppable
  (drop [self conn]
    (serr/catch-sqlexception
     (let [sqlts   (jc/to-sql-time (jt/now))
           sqldata (-> (json/from-native data)
                      (either/from-either))
           revhash (make-new-revhash true revid revhash sqldata)
           sql1    (-<> (sp/get-revisions-tablename coll)
                        (format "INSERT INTO %s (id, revid, revhash, data, created_at, deleted)
                              VALUES (?, ?, ?, ?, ?, ?);" <>)
                        (vector <> id (inc revid) revhash sqldata sqlts true))
           sql2    (-<> (sp/get-mainstore-tablename coll)
                        (format "DELETE FROM %s WHERE id = ?;" <>)
                        (vector <> id))]
       (m/>>= (sconn/execute-prepared! conn sql1)
              (fn [_] (sconn/execute-prepared! conn sql2)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Persistence Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: validate id (must be length of > 0).
;; TODO: refactor this two functions in more small piceses of code.

(defn- create-json-document
  [coll doc conn]
  (let [ts      (jt/now)
        id      (or (.-id doc) (random-uuid))
        data    (.-data doc)
        sqldata (-> (json/from-native data)
                    (either/from-either))
        sqlts   (jc/to-sql-time ts)
        revid   1
        revhash (make-new-revhash false 0 "" sqldata)
        sqlfac  (fn [tablename]
                  (-<> tablename
                       (format "INSERT INTO %s (id, revid, revhash, data, created_at)
                                VALUES (?, ?, ?, ?, ?);" <>)
                       (vector <> id revid revhash sqldata sqlts)))]
    (m/mlet [_ (sconn/execute-prepared! conn (sqlfac (sp/get-mainstore-tablename coll)))
             _ (sconn/execute-prepared! conn (sqlfac (sp/get-revisions-tablename coll)))]
      (m/return (JsonDocument. coll id revid revhash data ts)))))

(defn- update-json-document
  [coll prevdoc doc conn]
  (let [ts      (jt/now)
        id      (.-id doc)
        data    (.-data doc)
        sqldata (-> (json/from-native data)
                    (either/from-either))
        sqlts   (jc/to-sql-time ts)
        revid   (inc (:revid prevdoc))
        revhash (make-new-revhash false
                                  (:revid prevdoc)
                                  (:revhash prevdoc)
                                  sqldata)
        sql1    (-<> (sp/get-revisions-tablename coll)
                     (format "INSERT INTO %s (id, revid, revhash, data, created_at)
                              VALUES (?, ?, ?, ?, ?);" <>)
                     (vector <> id revid revhash sqldata sqlts))
        sql2    (-<> (sp/get-mainstore-tablename coll)
                     (format "UPDATE %s SET data=?, revid=?, revhash=?, created_at=?
                              WHERE id = ?;" <>)
                     (vector <> sqldata revid revhash sqlts id))]
    (m/mlet [_ (sconn/execute-prepared! conn sql1)
             _ (sconn/execute-prepared! conn sql2)]
      (m/return (JsonDocument. coll id revid revhash data ts)))))

(defn persist-json-document
  [coll doc conn]
  (let [prevdoc (-<> (sp/get-mainstore-tablename coll)
                     (format "SELECT * FROM %s WHERE id = ?;" <>)
                     (vector <> (.-id doc))
                     (j/query-first conn <>))]
    (if (nil? prevdoc)
      (create-json-document coll doc conn)
      (update-json-document coll prevdoc doc conn))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Json Document parsing (from string)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- parse-rev
  [rev]
  (if (nil? rev)
    (either/right rev)
    (try
      (let [[revid revhash] (str/split rev #"-" 2)
            revid           (Long/parseLong revid)]
        (either/right [revid revhash]))
      (catch Exception exc
        (log :debug "Invalid rev format passed" exc)
        (e/error :invalid-rev-format)))))

(defn json->document
  "Json document specific coersion function
  from plain json (string) to document instance."
  ([coll data]
     (json->document coll {} data))
  ([coll opts data]
     (m/mlet [data (json/parse data)
              :let [id   (or (:_id opts) (:_id data))
                    rev  (or (:_rev opts) (:_rev data))
                    data (dissoc data :_id :_rev)]
              rev  (parse-rev rev)]
       (-> (if-let [[revid revhash] rev]
             (JsonDocument. coll id revid revhash data nil)
             (JsonDocument. coll id nil nil data nil))
           (m/return)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Json Collection Type extension.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-type JsonDocumentCollection
  sp/DocumentStore

  ;; Concerts database (postgresql) record response
  ;; into a valid document instance.
  (record->document [coll {:keys [id revid revhash data created_at]}]
    (m/mlet [data (json/to-native data)
             :let [ts (jc/from-sql-time created_at)]]
      (m/return (JsonDocument. coll id revid revhash data ts))))

  (persist-document [coll opts data conn]
    (m/mlet [doc (json->document coll opts data)]
      (persist-json-document coll doc conn)))

  sp/DocumentQueryable

  (get-document-by-id [coll id conn]
    (m/mlet [rec (-<> (sp/get-mainstore-tablename coll)
                      (format "SELECT * FROM %s WHERE id = ?;" <>)
                      (vector <> id)
                      (sconn/query-first conn <>))]
      (if (nil? rec)
        (e/error :document-does-not-exist)
        (sp/record->document coll rec))))

  (get-document-by-rev [coll id rev conn]
    (m/mlet [rev  (parse-rev rev)
             :let [[revid revhash] rev]
             rec  (-<> (sp/get-revisions-tablename coll)
                       (format "SELECT * FROM %s WHERE id = ? AND
                                revid = ? AND revhash = ?;" <>)
                       (vector <> id revid revhash)
                       (sconn/query-first conn <>))]
            (if (nil? rec)
              (e/error :document-does-not-exist)
              (sp/record->document coll rec))))

  (get-documents [coll filters conn]
    (m/mlet [res (->> (sp/get-mainstore-tablename coll)
                      (format "SELECT * FROM %s;")
                      (sconn/query conn))]
      (let [res (mapv (partial sp/record->document coll) res)]
        (if (empty? res)
          (either/right res)
          (m/sequence res))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn document?
  "Check if object v satisfies
  the main document protocol."
  [v]
  (satisfies? sp/Document v))

(defn persist-document
  "Generic function for persist document
  in the collection."
  ([coll data conn]
     (persist-document coll {} data conn))
  ([coll opts data conn]
     (sp/persist-document coll opts data conn)))

(defn get-documents
  [coll filters conn]
  (sp/get-documents coll filters conn))

(defn get-document-by-id
  [coll id conn]
  (sp/get-document-by-id coll id conn))

(defn get-document-by-rev
  [coll id rev conn]
  (sp/get-document-by-rev coll id rev conn))

(defn drop-document
  [doc conn]
  (sp/drop doc conn))

(defn drop-document-by-id
  [coll id conn]
  (m/>>= (get-document-by-id coll id conn)
         (fn [doc] (drop-document doc conn))))

(defn rev
  [doc]
  (sp/rev doc))
