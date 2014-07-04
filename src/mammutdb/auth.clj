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

(ns mammutdb.auth
  "Authentication functions for mammutdb."
  (:require [cats.core :as m]
            [cats.types :as t]
            [buddy.hashers.bcrypt :as hasher]
            [buddy.sign.jws :as jws]
            [mammutdb.config :as config]
            [mammutdb.core.errors :as e]
            [mammutdb.storage.user :as suser]
            [mammutdb.storage.types :as stypes]
            [mammutdb.storage.connection :as sconn]))

(def secret-key (delay (config/read-secret-key)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Private Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- check-user-password
  "Given a user record and password candidate, check
  if password matches with stored password."
  [user password]
  (let [hash (.-password user)]
    (if-let [ok (hasher/check-password password hash)]
      (t/right true)
      (e/error :wrong-authentication))))

(defn- make-access-token
  "Given a userid, return valid access token."
  [^long userid]
  (m/<$> #(jws/sign {:userid userid} %) @secret-key))

(defn- validate-access-token
  "Given a token, validates it and return user id."
  [^String token]
  (m/mlet [secretkey @secret-key]
    (let [data   (jws/unsign token secretkey)
          userid (:userid data)]
      (if userid
        (t/right userid)
        (e/error :wrong-authentication)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn authenticate-credentials
  "Given user credentials, authenticate them and return
  user record with access token."
  [^String username ^String password]
  (m/mlet [conn  (sconn/new-connection)
           user  (suser/get-user-by-username username conn)
           ok    (check-user-password user password)
           token (make-access-token (.-id user))
           _     (sconn/close-connection conn)]
    (m/return (stypes/->user user token))))

(defn authenticate-token
  "Given a token, return a user record for it or fail."
  [^String token]
  (m/mlet [conn   (sconn/new-connection)
           userid (validate-access-token token)
           user   (suser/get-user-by-id userid conn)
           _      (sconn/close-connection conn)]
    (m/return user)))
