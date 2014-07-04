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

(ns mammutdb.storage.user
  "Authentication relates storage functions."
  (:require [clojure.java.io :as io]
            [cats.core :as m]
            [cats.types :as t]
            [jdbc.core :as j]
            [buddy.hashers.bcrypt :as hasher]
            [mammutdb.core.edn :as edn]
            [mammutdb.core.errors :as e]
            [mammutdb.storage.errors :as serr]
            [mammutdb.storage.types :as stypes]
            [mammutdb.storage.protocols :as sproto]
            [mammutdb.storage.connection :as sconn])
  (:import mammutdb.storage.types.User))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-user-by-username
  [^String username conn]
  (m/mlet [:let   [sql ["SELECT id, username, password
                         FROM mammutdb_user WHERE username = ?;"
                        username]]
           result (sconn/query-first conn sql)]
    (if result
      (t/right (stypes/map->user result))
      (e/error :user-not-exists
               (format "User '%s' not exists" username)))))

(defn get-user-by-id
  [^Long id conn]
  (m/mlet [:let   [sql ["SELECT id, username, password
                         FROM mammutdb_user WHERE id = ?;" id]]
           result (sconn/query-first conn sql)]
    (if result
      (t/right (stypes/map->user result))
      (e/error :user-not-exists
               (format "User with id '%s' not exists" id)))))

(defn exists?
  [^String username conn]
  (let [sql ["SELECT EXISTS(SELECT 1 FROM mammutdb_users
                            WHERE username = ?)" username]]
    (m/mlet [res (sconn/query-first conn sql)]
      (if (:exists res)
        (m/return username)
        (e/error :user-not-exists
                 (format "User '%s' not exists" username))))))

(defn create!
  [^String username ^String password conn]
  (let [password (hasher/make-password password)
        sql      ["INSERT INTO mammutdb_users (username, password)
                   VALUES (?, ?);"
                  username,
                  password]]
    (serr/catch-sqlexception
     (let [res (j/execute-prepared! conn sql {:returning :all})]
       (t/right (stypes/map->user (first res)))))))

(extend-type User
  sproto/Droppable
  (drop! [user conn]
    (m/mlet [_    (exists? (.-username user) conn)
             :let [sql ["DELETE FROM mammutdb_users
                         WHERE id = ?;" (.-id user)]]]
      (serr/catch-sqlexception
       (j/execute-prepared! conn sql)
       (t/right)))))

(defn drop!
  [user conn]
  (sproto/drop! user conn))

