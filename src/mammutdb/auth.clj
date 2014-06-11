(ns mammutdb.auth
  "Authentication functions for mammutdb."
  (:require [mammutdb.storage :as storage]
            [mammutdb.core.monads :as m]
            [mammutdb.core.monads.types :as t]
            [mammutdb.config :as config]
            [buddy.hashers.bcrypt :as hasher]
            [buddy.sign.jws :as jws]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Private Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private secret-key (delay (config/read-secret-key)))

(defn- check-user-password
  "Given a user record and password candidate, check
  if password matches with stored password."
  [user password]
  (if-let [hash (:password user)
           ok   (hs/check-password password hash)]
    (t/right true)
    (t/left "Wrong password"))))

(defn- make-access-token
  "Given a userid, return valid access token."
  [^long userid]
  (m/<$> #(jws/sign {:userid userid} %) @secret-key))

(defn- validate-access-token
  "Given a token, validates it and return user id."
  [^String token]
  (m/mlet [secretkey @secret-key]
    (if-let [data   (jws/unsign token secretkey)
             userid (:userid data)]
      (t/right userid)
      (t/left "Invalid access token."))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn authenticate-credentials
  "Given user credentials, authenticate them and return
  user record with access token."
  [^String username  ^String password]
  (m/mlet [user  (storage/get-user-by-username username)
           ok    (check-user-password user password)
           token (make-access-token (:id user))]
    (m/return (assoc user :token token))))

(defn authenticate-token
  "Given a token, return a user record for it or fail."
  [^String token]
  (m/mlet [userid (validate-access-token token)
           user   (storage/get-user-by-id userid)]
    (m/return user)))
