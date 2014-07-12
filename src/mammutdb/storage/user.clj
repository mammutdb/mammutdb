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
  (:require [clojure.java.io :as io]
            [cats.core :as m]
            [cats.types :as t]
            [jdbc.core :as j]
            [buddy.hashers.bcrypt :as hasher]
            [mammutdb.core.errors :as e]
            [mammutdb.storage.errors :as serr]
            [mammutdb.storage.protocols :as sproto]
            [mammutdb.storage.connection :as sconn]))

(declare user-exists?)

(deftype User [id username password token]
  Object
  (equals [_ other]
    (= id (.-id other)))

  (toString [_]
    (with-out-str
      (print [id username])))

  sproto/Droppable
  (drop [_ conn]
    (m/mlet [_    (user-exists? username conn)
             :let [sql ["DELETE FROM mammutdb_users
                         WHERE id = ?;" id]]]
      (serr/catch-sqlexception
       (j/execute-prepared! conn sql)
       (t/right)))))

(alter-meta! #'->User assoc :no-doc true :private true)

(defn user?
  [v]
  (instance? User v))

(defn ->user
  "Default user constructor."
  ([^User user token]
     (assert (user? user))
     (User. (.-id user)
            (.-username user)
            (.-password user)
            token))
  ([^Long id ^String username ^String password]
     (User. id username password nil))
  ([^Long id ^String username ^String password ^String token]
     (User. id username password token)))

(defn record->user
  [{:keys [id username password]}]
  (->user id username password))

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
      (t/right (record->user result))
      (e/error :user-does-not-exist
               (format "User '%s' not exist" username)))))

(defn get-user-by-id
  [^Long id conn]
  (m/mlet [:let   [sql ["SELECT id, username, password
                         FROM mammutdb_user WHERE id = ?;" id]]
           result (sconn/query-first conn sql)]
    (if result
      (t/right (record->user result))
      (e/error :user-does-not-exist
               (format "User with id '%s' not exist" id)))))

(defn user-exists?
  [^String username conn]
  (let [sql ["SELECT EXISTS(SELECT 1 FROM mammutdb_users
                            WHERE username = ?)" username]]
    (m/mlet [res (sconn/query-first conn sql)]
      (if (:exists res)
        (m/return username)
        (e/error :user-does-not-exist
                 (format "User '%s' not exist" username))))))

(defn create-user
  [^String username ^String password conn]
  (let [password (hasher/make-password password)
        sql      ["INSERT INTO mammutdb_users (username, password)
                   VALUES (?, ?);"
                  username,
                  password]]
    (serr/catch-sqlexception
     (let [res (j/execute-prepared! conn sql {:returning :all})]
       (t/right (record->user (first res)))))))

(defn drop-user
  [user conn]
  (sproto/drop user conn))

