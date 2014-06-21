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

(ns mammutdb.storage.migrations
  "Specific migrations implementation for mammutdb."
  (:require [clojure.java.io :as io]
            [jdbc.core :as j]
            [jdbc.transaction :as tx]
            [mammutdb.config :as config]
            [mammutdb.core.edn :as edn]
            [mammutdb.storage.connection :as c]))

(declare migrations)

(defn- initialized?
  "Check if database layout is initialized or not."
  [conn]
  (try
    (j/query conn "SELECT 'public.mammutdb_migrations'::regclass")
    true
    (catch Exception e
      false)))

(defn- migration-installed?
  [conn ^String name]
  (let [sql     ["SELECT * FROM mammutdb_migrations WHERE name = ?" name]
        exists? (j/query-first conn sql)]
    (if exists? true false)))

(defn- install-migration!
  [conn ^String name]
  (let [sql "INSERT INTO mammutdb_migrations (name) VALUES (?)"]
    (j/execute-prepared! conn sql [name])))

(defn- apply-migrations!
  [conn migrations]
  (doseq [[name func] migrations]
    (when-not (migration-installed? conn name)
      (println (format "==> Installing migration: %s" name))
      (install-migration! conn name)
      (apply func [conn]))))

(defn- migrate-v1
  [conn]
  (let [sqldata (-> (edn/from-resource "sql/migrations.edn") (:v1))]
    (tx/with-transaction conn
      (j/execute! conn (:collections-create-table sqldata))
      (j/execute! conn (:metadata-create-table sqldata))
      (j/execute! conn (:users-create-table sqldata)))))

(def ^:private
  migrations [["0001" migrate-v1]])

(defn bootstrap
  "Initialize migrations system and create
  mandatory tables if them does not exists."
  []
  (with-open [conn (j/make-connection @c/datasource)]
    (when-not (initialized? conn)
      (let [sql (-> (edn/from-resource "sql/migrations.edn")
                    (:initial)
                    (:create-migrations-table))]
        (tx/with-transaction conn
          (j/execute! conn sql))))
    (tx/with-transaction conn
      (apply-migrations! conn migrations))))



