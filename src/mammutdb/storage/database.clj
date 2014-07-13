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

(ns mammutdb.storage.database
  (:require [cats.types :as t]
            [cats.core :as m]
            [jdbc.core :as j]
            [clojure.string :as str]
            [mammutdb.core.edn :as edn]
            [mammutdb.core.errors :as e]
            [mammutdb.storage.protocols :as sp]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.errors :as serr]
            [mammutdb.storage.connection :as sconn]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype Database [id name createdat]
  java.lang.Object
  (toString [_]
    (with-out-str
      (print [(str/lower-case name)])))

  (equals [_ other]
    (= name (.-name other)))

  sp/Database
  (get-database-name [db]
    (-> (.-name db)
        (str/lower-case)))

  sp/Droppable
  (drop [db con]
    (let [sql1 ["DELETE FROM mammutdb_databases WHERE name = ?;"
                (sp/get-database-name db)]]
      (serr/catch-sqlexception
       (j/execute-prepared! con sql1)
       (t/right))))

  sp/Serializable
  (to-plain-object [db]
    {:name (.-name db)
     :id (.-name db)
     :createdAt (.-createdat db)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Aliases
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ->database
  ([name]
     (Database. nil name nil))
  ([id name createdat]
     (Database. id name createdat)))

(defn record->database
  [{:keys [id name created_at]}]
  (->database id name created_at))

(defn database?
  [v]
  (instance? Database v))

(defn get-all-databases
  [conn]
  (m/mlet [:let [sql "SELECT id, name, created_at
                      FROM mammutdb_databases ORDER BY name;"]
           res  (sconn/query conn sql)]
    (t/right (mapv record->database res))))

(defn get-database-by-name
  [name conn]
  (m/>>= (->> ["SELECT id, name, created_at
                FROM mammutdb_databases
                WHERE name = ?;" name]
              (sconn/query-first conn))
         (fn [record]
           (if record
             (m/return (record->database record))
             (e/error :database-does-not-exist)))))

(defn database-exists?
  "Check if database with given name, are
  previously created."
  [^String name conn]
  (m/>>= (->> ["SELECT EXISTS(SELECT * FROM mammutdb_databases WHERE name = ?);" name]
              (sconn/query-first conn))
         (fn [record]
           (if (:exists record)
             (m/return name)
             (e/error :database-does-not-exist
                      (format "Database '%s' does not exist" name))))))

(defn create-database
  [^String name conn]
  (let [sqlexists ["SELECT EXISTS(SELECT * FROM
                    mammutdb_databases WHERE name = ?);" name]
        sqlinsert ["INSERT INTO mammutdb_databases (name) VALUES (?)" name]]
    (m/>>= (sconn/query-first conn sqlexists)
           (fn [existsresult]
             (if (:exists existsresult)
               (e/error :database-exists (format "Database '%s' is already exists." name))
               (t/right)))
           (fn [_] (sconn/execute-prepared! conn sqlinsert {:returning :all}))
           (fn [recs] (m/return (record->database (first recs)))))))

(defn drop-database
  [db con]
  (sp/drop db con))

