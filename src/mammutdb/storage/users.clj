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

(ns mammutdb.storage.users
  "Authentication relates storage functions."
  (:require [clojure.java.io :as io]
            [cats.core :as m]
            [cats.types :as t]
            [jdbc.core :as j]
            [mammutdb.core.edn :as edn]
            [mammutdb.storage.connection :as c]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL Readers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private sql-queries
  (delay (edn/from-resource "sql/query.edn")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype User [id username password token]
  Object
  (equals [_ other]
    (= id (.-id other)))

  (toString [_]
    (with-out-str
      (print [id username]))))

(alter-meta! #'->User assoc :no-doc true :private true)
(alter-meta! #'map->User assoc :no-doc true :private true)

(defn is-user?
  [v]
  (instance? User v))

(defn ->user
  "Default user constructor."
  ([^User user token]
     (assert (is-user? user))
     (User. (.-id user)
            (.-username user)
            (.-password user)
            token))
  ([^Long id ^String username ^String password]
     (User. id username password nil))
  ([^Long id ^String username ^String password ^String token]
     (User. id username password token)))

(defn map->user
  [{:keys [id username password]}]
  (->user id username password))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-user-by-username
  [^String username]
  (j/with-connection [conn @c/datasource]
    (let [sql     (:user-by-username @sql-queries)
          result  (j/query-first conn [sql username])]
      (if result
        (j/right (map->user result))
        (j/left (format "No user found with username: %s" username))))))

(defn get-user-by-id
  [^Long id]
  (j/with-connection [conn @c/datasource]
    (let [sql     (:user-by-id @sql-queries)
          result  (j/query-first conn [sql username])]
      (if result
        (j/right (map->user result))
        (j/left (format "No user found with username: %s" username))))))
