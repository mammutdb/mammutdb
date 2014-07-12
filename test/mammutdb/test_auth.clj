(ns mammutdb.test-auth
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [buddy.hashers.bcrypt :as hasher]
            [buddy.sign.jws :as jws]
            [cats.core :as m]
            [cats.types :as t]
            [jdbc.core :as j]
            [mammutdb.config :as config]
            [mammutdb.auth :as auth]
            [mammutdb.storage :as storage]
            [mammutdb.storage.migrations :as migrations]
            [mammutdb.storage.connection :as sconn]))


(def password (hasher/make-password "secret"))

(deftest auth
  (config/setup-config! "test/testconfig.edn")

  (testing "Authenticate credentials"
    (with-redefs [storage/get-user-by-username
                  (fn [username conn]
                    (t/right (storage/->user 1 username password)))]
      (let [user (auth/authenticate-credentials "username" "secret")]
        (is (t/right? user))
        (is (storage/user? (.v user))))))

  (testing "Authenticate token"
    (with-redefs [storage/get-user-by-id
                  (fn [id conn]
                    (t/right (storage/->user id "username" password)))]
      (let [token (@#'auth/make-access-token 1)
            user  (auth/authenticate-token (t/from-either token))]
        (is (t/right? user))
        (is (= (.-username (t/from-either user)) "username"))))))


(deftest users
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Creating/Deleting users"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr (storage/create-user "test" "test" conn)
            r  (t/from-either mr)]
        (is (t/right? mr))
        (is (storage/user? r))
        (is (.-username r) "test")
        (storage/drop-user r conn))
      (let [mr (storage/user-exists? "test" conn)
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :user-does-not-exist))))))
