(ns mammutdb.test-api
  (:require [clojure.test :refer :all]
            [cats.types :as t]
            [mammutdb.api.database :as dbapi]
            [mammutdb.storage.database :as sdb]
            [mammutdb.config :as config]
            [mammutdb.storage.migrations :as migrations]))

(deftest database-api
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "get-all and create! and drop!"
    (let [mv1 (dbapi/create! "foodb1")
          mv2 (dbapi/create! "foodb2")]
      (is (t/right? mv1))
      (is (t/right? mv2)))

    (let [mv (dbapi/get-all)
          v  (t/from-either mv)]
      (is (t/right? mv))
      (is (vector? v))
      (is (= (count v) 2)))

    (let [mv1 (dbapi/get-by-name "foodb1")
          mv2 (dbapi/get-by-name "foodb2")
          v1  (t/from-either mv1)
          v2  (t/from-either mv2)]
      (is (t/right? mv1))
      (is (t/right? mv2))
      (is (sdb/database? v1))
      (is (sdb/database? v2))

      (dbapi/drop! "foodb1")
      (dbapi/drop! "foodb2")

      (let [mv (dbapi/get-all)
            v  (t/from-either mv)]
        (is (t/right? mv))
        (is (vector? v))
        (is (= (count v) 0))))))

