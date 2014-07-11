(ns mammutdb.test-api
  (:require [clojure.test :refer :all]
            [cats.types :as t]
            [mammutdb.api :as api]
            [mammutdb.api.database :as dbapi]
            [mammutdb.api.collection :as collapi]
            [mammutdb.storage.database :as sdb]
            [mammutdb.storage.collection :as scoll]
            [mammutdb.config :as config]
            [mammutdb.storage.migrations :as migrations]))

(deftest database-api
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Common crud functions"
    (let [mv1 (api/create-db "foodb1")
          mv2 (api/create-db "foodb2")]
      (is (t/right? mv1))
      (is (t/right? mv2)))

    (let [mv (api/get-all-databases)
          v  (t/from-either mv)]
      (is (t/right? mv))
      (is (vector? v))
      (is (= (count v) 2)))

    (api/drop-db "foodb1")
    (api/drop-db "foodb2")

    (let [mv (api/get-all-databases)
          v  (t/from-either mv)]
      (is (t/right? mv))
      (is (vector? v))
      (is (= (count v) 0))))

  (testing "Get by name"
    (let [mv1 (api/create-db "foodb1")
          mv2 (api/create-db "foodb2")]
      (is (t/right? mv1))
      (is (t/right? mv2)))

    (let [mv1 (api/get-db-by-name "foodb1")
          mv2 (api/get-db-by-name "foodb2")
          v1  (t/from-either mv1)
          v2  (t/from-either mv2)]
      (is (t/right? mv1))
      (is (t/right? mv2))
      (is (sdb/database? v1))
      (is (sdb/database? v2)))

    (api/drop-db "foodb1")
    (api/drop-db "foodb2")))

(deftest collection-api
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Common crud functions"
    (let [mdb (api/create-db "foodb")
          mv1 (api/create-collection "foodb" "collname1")
          mv2 (api/create-collection "foodb" "collname2")
          v1  (t/from-either mv1)
          v2  (t/from-either mv2)]
      (is (t/right? mdb))
      (is (t/right? mv1))
      (is (t/right? mv2))
      (is (scoll/collection? v1))
      (is (scoll/collection? v2)))

    (let [mr (api/get-all-collections "foodb")
          r  (t/from-either mr)]
      (is (t/right? mr))
      (is (vector? r))
      (is (= (count r) 2)))

    (api/drop-collection "foodb" "collname1")
    (api/drop-collection "foodb" "collname2")

    (let [mr (api/get-all-collections "foodb")
          r  (t/from-either mr)]
      (is (t/right? mr))
      (is (vector? r))
      (is (= (count r) 0)))))
