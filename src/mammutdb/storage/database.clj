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
            [mammutdb.storage.types :as stypes]
            [mammutdb.storage.protocols :as sproto]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.errors :as serr]
            [mammutdb.storage.connection :as sconn])
  (:import mammutdb.storage.types.Database))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *database-safe-rx* #"[\w\_\-]+")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-type Database
  sproto/Database
  (get-database-name [db]
    (-> (.-name db)
        (str/lower-case)))

  (get-collections [db con]
    (let [sql ["SELECT name, type FROM mammutdb_collections
                WHERE database = ? ORDER BY name"
               (sproto/get-database-name db)]]
      (m/mlet [results (sconn/query con sql)]
        (-> (fn [{:keys [name type]}]
              (case (keyword type)
                :document (stypes/->doc-collection db name)))
            (mapv results)
            (m/return)))))

  sproto/Droppable
  (drop! [db con]
    (let [sql1 ["DELETE FROM mammutdb_databases WHERE name = ?;"
                (sproto/get-database-name db)]]
      (serr/catch-sqlexception
       (j/execute-prepared! con sql1)
       (t/right)))))

(defn safe-name?
  "Parse collection name and return a safe
  string or nil."
  [name]
  (if (re-matches *database-safe-rx* name)
    (t/right true)
    (e/error :database-name-unsafe)))

(defn exists?
  "Check if database with given name, are
  previously created."
  [name con]
  (m/mlet [safe (safe-name? name)
           :let [sql ["SELECT EXISTS(SELECT * FROM mammutdb_databases WHERE name = ?);" name]]
           res  (sconn/query-first con sql)]
    (if (:exists res)
      (m/return (stypes/->database name))
      (e/error :database-not-exists
               (format "Database '%s' does not exists" name)))))

(defn create!
  [name con]
  (m/mlet [safe (safe-name? name)
           :let [sql ["INSERT INTO mammutdb_databases (name) VALUES (?)" name]]]
    (serr/catch-sqlexception
     (j/execute-prepared! con sql)
     (t/right (stypes/->database name)))))

(defn drop!
  [db con]
  (sproto/drop! db con))

