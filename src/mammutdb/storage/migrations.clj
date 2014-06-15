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
  (:require [jdbc.core :as j]
            [jdbc.transaction :as tx]
            [cats.core :as m]
            [cats.types :as t]
            [mammutdb.config :as config]
            [mammutdb.storage.connection :as c]))

(declare migrations)

(defn- initialized?
  "Check if database layout is initialized or not."
  [conn]
  (j/with-connection [conn @datasource]
    (try
      (j/query conn "SELECT 'public.mammutdb_migrations'::regclass")
      true
      (catch Exception e
        (.printStackTrace e)
        false))))

(defn- migration-installed?
  [conn ^String name]
  (let [sql     ["SELECT * FROM mammutdb_migrations WHERE name = ?" name]
        exists? (query-first conn sql)]
    (if exists? true false)))

(defn- install-migration!
  [conn ^String name]
  (let [sql "INSERT INTO mammutdb_migrations (name) VALUES (?)"]
    (tx/with-transaction conn
      (j/execute-prepared! sql [name]))))

(defn- apply-migration!
  [conn name func]
  (tx/with-transaction conn
    (apply func [conn])
    (install-migration! conn name)))

(defn bootstrap!
  "Initialize migrations system and create
  mandatory tables if them does not exists."
  []
  (j/with-connection [conn @datasource]
    (when-not (initialized? conn)
      (let [sql (slurp (io/resource "sql/schema/migrations.sql"))]
        (tx/with-transaction conn
          (j/execute! sql))))
    (try
      (tx/with-transaction conn
        (doseq [[name func] migrations]
          (when-not (migration-installed? conn name)
            (println "Installing migration:" name)
            (apply-migration! conn name func))))
      (catch Exception e
        (.printStackTrace e)))))

(defn- migration-v1
  [conn]
  (let [sql1 (slurp (io/resource "sql/schema/v1-metadata.sql"))
        sql2 (slurp (io/resource "sql/schema/v1-users.sql"))
        sql3 (slurp (io/resource "sql/schema/v1-collections.sql"))]
    (tx/with-transaction conn
      (j/execute! sql1)
      (j/execute! sql2)
      (j/execute! sql3))))

(def ^:private
  migrations [["v1" migration-v1]])

