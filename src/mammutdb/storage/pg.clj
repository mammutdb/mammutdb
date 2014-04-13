;; Copyright (c) 2014 Andrey Antukh <niwi@niwi.be>
;;
;; All rights reserved.
;;
;; Redistribution and use in source and binary forms, with or without
;; modification, are permitted provided that the following conditions
;; are met:
;; 1. Redistributions of source code must retain the above copyright
;;    notice, this list of conditions and the following disclaimer.
;; 2. Redistributions in binary form must reproduce the above copyright
;;    notice, this list of conditions and the following disclaimer in the
;;    documentation and/or other materials provided with the distribution.
;; 3. The name of the author may not be used to endorse or promote products
;;    derived from this software without specific prior written permission.
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

(ns mammutdb.storage.pg
  (:require [com.stuartsierra.component :as component]
            [jdbc :as j]
            [jdbc.transaction :as tx]
            [jdbc.pool.dbcp :as dbcp]
            [mammutdb.storage.protocol :as proto]
            [mammutdb.core.types.maybe :as maybe]
            [mammutdb.core.types.json :refer [json]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *collection-safe-rx* #"[\w\_\-]+")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utils
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn is-safe-collection-name?
  "Parse collection name and return a safe
string or nil."
  [name]
  (boolean (re-matches *collection-safe-rx* name)))

(defn collection-name->tablename
  "Given a type and collection name, return
a corresponding table name for these type."
  [type name]
  (case type
    :main (str name "_c")
    :revision (str name "_r")))


(defn record->object
  [record]
  (let [id  (:id record)
        rev (:revision record)]
    (-> (:data record)
        (assoc :_id id :_rev rev))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn initialize
  "Create initial tables for storage layout."
  [storage conn]
  (let [sql1 (str "CREATE TABLE mammutdb_metadata ("
                  "  type varchar(255),"
                  "  value json DEFAULT '{}'::json"
                  ");")
        sql2 (str "CREATE TABLE mammutdb_collections ("
                  "  name varchar(1024),"
                  "  metadata json DEFAULT '{}'::json,"
                  "  created_at timestamp with timezone DEFAULT now()"
                  ");")]
    (j/execute! conn sql1 sql2)))

(defn initialized?
  "Check if current storage layout is intialized."
  [conn]
  (let [sql (str "SELECT EXISTS(SELECT * "
                 "  FROM information_schema.tables "
                 "  WHERE "
                 "    table_schema = 'public' AND "
                 "    table_name = ?);")]
    (-> (j/query conn [sql "mammutdb_metadata"])
        (first))))

(defn collection-exists?
  "Check if collection with given name, are
previously created."
  [conn collection]
  (let [sql (str "SELECT EXISTS( "
                 "  SELECT * FROM mammutdb_collections "
                 "  WHERE name = ? "
                 ");")]
    (try
      (let [res (first (j/query conn [sql name]))]
        (if res
          (maybe/ok collection)
          (maybe/fail :collection-does-not-exists)))
      (catch Exception e
        (maybe/exception e)))))

(defn create-collection
  "Given a name, create and register new collection."
  [conn name]
  (let [ctmain     (collection-name->tablename :main name)
        ctrevision (collection-name->tablename :revision name)
        sql1 (str "CREATE TABLE " ctmain " ( "
                  "  id uuid DEFAULT uuid_generate_v1(), "
                  "  data json DEFAULT '{}'::json, "
                  "  revision bigint DEFAULT 1, "
                  "  created_at timestamp with timezone DEFAULT now(), "
                  "  updated_at timestamp with timestamp DEFAULT now() "
                  ");")
        sql2 (str "CREATE TABLE " ctrevision " ( "
                  "  id uuid, "
                  "  data json DEFAULT '{}'::json, "
                  "  revision bigint DEFAULT 1, "
                  "  created_at timestamp with timezone DEFAULT now() "
                  ");")
        sql3 (str "INSERT INTO mammut_collections "
                  "(name) VALUES (?)")]
    (try
      (j/execute! conn sql1 sql2)
      (j/execute-prepared! sql3 [name])
      (maybe/ok :empty)
      (catch Exception e
        (maybe/fail e)))))

(defn get-object-by-id
  [conn collname id]
  (let [sql (str "SELECT * FROM " collname "_c "
                 "WHERE id = ?;")]
    (try
      (let [res (first (j/query conn [sql id]))]
        (if res
          (maybe/ok (record->object res))
          (maybe/fail :
      (catch Exception e
        (maybe/fail e)))))

(defn- store-new-object
  [conn collection object]
  (let [object     (-> (dissoc object :_id :_rev) (json))
        ctmain     (collection-name->tablename :main collection)
        ctrevision (collection-name->tablename :revision collection)
        sql1       (str "INSERT INTO " ctmain " (data) "
                        "VALUES  (?) RETURNING id, revision;")]
    (try
      ;; We use query because insert with returning
      ;; statement works as query and return one record.
      (let [res (j/query conn [sql1 object])
            res (first res)]
        (maybe/ok (assoc object
                    :_id (:id res)
                    :_rev (:revision res))))
      (catch Exception e
        (maybe/fail e)))))

(defn- store-existing-object
  [conn collection {:keys [_id] :as newobj} {:keys [_rev] :as oldobj}]
  (let [newobj     (-> (dissoc newobj :_id :_rev) (json))
        oldobj     (-> (dissoc oldobj :_id :_rev) (json))
        ctmain     (collection-name->tablename :main collection)
        ctrevision (collection-name->tablename :revision collection)
        sql1       (str "UPDATE " ctmain " SET revision = ?, data = ?, updated_at = now() "
                        "WHERE id = ?;")
        sql2       (str "INSERT INTO " ctrevision " (id, data, revision) "
                        "VALUES (?, ?, ?);")]
    (try
      (j/execute-prepared! conn sql1 [(inc _rev) newobj _id])
      (j/execute-prepared! conn sql2 [_id oldobj _rev])
      (maybe/ok (assoc newobj :_rev (inc _rev)))
      (catch Exception e
        (maybe/fail e)))))

(defn store-object
  [conn collection {:keys [_id] :as newobj}]
  (if-let [oldobj (get-object-by-id storage conn collection _id)]
    (store-existing-object conn collection newobj oldobj)
    (store-new-object conn collection newobj)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Monadic validators
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn collection-name-valid?
  [collection]
  (if (is-safe-collection-name? collection)
    (maybe/ok collection)
    (maybe/fail :collection-invalid-name)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Protocol implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defrecord PgStorage [dbspec datasource]
  component/Lifecycle

  (start [component]
    (println "Starting storage component...")
    (let [ds (dbcp/make-datasource-spec dbspec)]
      (assoc component :datasource ds)))

  (stop [component]
    (println "Stopping storage component...")
    (.close (:datasource datasource))
    (assoc component :datasource nil))

  proto/Storage

  (initialized? [self]
    (j/with-connection [conn datasource]
      (tx/call-in-transaction #(initialized? self %))))

  (initialize [self]
    (j/with-connection [conn datasource]
      (tx/with-transaction conn
        (initializel self conn))))

  (store-object [self collection object]
    (j/with-connection [conn datasource]
      (tx/with-transaction conn
        (maybe/m-apply collection
                       (fn [c] (collection-name-valid? c))
                       (fn [c] (collection-exists? conn c))
                       (fn [c] (store-object self conn c object))))))

  (collection-exists? [self collection]
    (j/with-connection [conn datasource]
      (tx/with-transaction conn
        (maybe/m-apply collection
                       (fn [c] (collection-name-valid? c))
                       (fn [c] (collection-exists? conn c)))

  (get-object-by-id [self collection id]
    (j/with-connection [conn datasource]
      (tx/with-transaction conn
        (get-object-by-id conn collection id)))))


(defn storage
  "Storage component constructor"
  [config]
  (map->PgStorage {:dbspec (:dbspec confi)}))
