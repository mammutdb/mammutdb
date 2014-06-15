(ns mammutdb.tests-auth
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [buddy.hashers.bcrypt :as hasher]
            [buddy.sign.jws :as jws]
            [mammutdb.config :as config]
            [mammutdb.auth :as auth]
            [mammutdb.storage :as storage]
            [mammutdb.storage.datatypes :as sdt]
            [cats.core :as m]
            [cats.types :as t]))

(def password (hasher/make-password "secret"))

(deftest auth
  (binding [config/*config-path* "test/testconfig.edn"]
    (testing "Authenticate credentials"
      (with-redefs [storage/get-user-by-username
                    (fn [username]
                      (t/right (sdt/user 1 username password)))]
        (let [user (auth/authenticate-credentials "username" "secret")]
          (is (t/right? user))
          (is (sdt/user? (.v user))))))

    (testing "Authenticate token"
      (with-redefs [storage/get-user-by-id
                    (fn [id]
                      (t/right (sdt/user id "username" password)))]
        (let [token (@#'auth/make-access-token 1)
              user  (auth/authenticate-token (t/from-either token))]
          (is (t/right? user))
          (is (= (:username (t/from-either user)) "username")))))))
