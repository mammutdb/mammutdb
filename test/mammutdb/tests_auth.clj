(ns mammutdb.tests-auth
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [buddy.hashers.bcrypt :as hasher]
            [buddy.sign.jws :as jws]
            [cats.core :as m]
            [cats.types :as t]
            [mammutdb.config :as config]
            [mammutdb.auth :as auth]
            [mammutdb.storage.types :as stypes]
            [mammutdb.storage.user :as user]
            [mammutdb.storage.migrations :as migrations]))


(def password (hasher/make-password "secret"))

(deftest auth
  (config/setup-config "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Authenticate credentials"
    (with-redefs [user/get-user-by-username
                  (fn [username conn]
                    (t/right (stypes/->user 1 username password)))]
      (let [user (auth/authenticate-credentials "username" "secret")]
        (is (t/right? user))
        (is (stypes/user? (.v user))))))

  (testing "Authenticate token"
    (with-redefs [user/get-user-by-id
                  (fn [id conn]
                    (t/right (stypes/->user id "username" password)))]
      (let [token (@#'auth/make-access-token 1)
            user  (auth/authenticate-token (t/from-either token))]
        (is (t/right? user))
        (is (= (.-username (t/from-either user)) "username"))))))
