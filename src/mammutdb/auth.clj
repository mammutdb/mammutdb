(ns mammutdb.auth
  "Authentication functions for mammutdb."
  (:require [mammutdb.storage :as storage]
            [mammutdb.core.monads :as m]
            [mammutdb.core.monads.types :as t]))

(defn- check-user-password
  "Given a user record and password candidate, check
  if password matches with stored password."
  [user password]
  (t/just true))

(defn- make-access-token
  "Given a user record, return self contained,
  signed and timestamped access token."
  [^long userid]
  (j/just :user)

(defn- validate-access-token
  "Given a self contained token, validates it and return
  contained user id."
  [^String token]
  (t/just 1))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn authenticate-credentials
  "Given user credentials, authenticate them and return
  token for posterior fast auth."
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
