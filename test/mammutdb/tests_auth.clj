(ns mammutdb.tests-auth
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [buddy.hashers.bcrypt :as hasher]
            [buddy.sign.jws :as jws]
            [cats.core :as m]
            [cats.types :as t]
            [jdbc.core :as j]
            [mammutdb.config :as config]
            [mammutdb.auth :as auth]
            [mammutdb.storage.types :as stypes]
            [mammutdb.storage.user :as suser]
            [mammutdb.storage.migrations :as migrations]
            [mammutdb.storage.connection :as sconn]))


(def password (hasher/make-password "secret"))

(deftest auth
  (config/setup-config "test/testconfig.edn")

  (testing "Authenticate credentials"
    (with-redefs [suser/get-user-by-username
                  (fn [username conn]
                    (t/right (stypes/->user 1 username password)))]
      (let [user (auth/authenticate-credentials "username" "secret")]
        (is (t/right? user))
        (is (stypes/user? (.v user))))))

  (testing "Authenticate token"
    (with-redefs [suser/get-user-by-id
                  (fn [id conn]
                    (t/right (stypes/->user id "username" password)))]
      (let [token (@#'auth/make-access-token 1)
            user  (auth/authenticate-token (t/from-either token))]
        (is (t/right? user))
        (is (= (.-username (t/from-either user)) "username"))))))


(deftest users
  (config/setup-config "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Creating/Deleting users"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr (suser/create! "test" "test" conn)
            r  (t/from-either mr)]
        (is (t/right? mr))
        (is (stypes/user? r))
        (is (.-username r) "test")
        (suser/drop! r conn))
      (let [mr (suser/exists? "test" conn)
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :user-not-exists))))))
