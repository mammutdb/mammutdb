(ns mammutdb.test-api
  (:require [clojure.test :refer :all]
            [cats.types :as t]
            [mammutdb.api :as api]
            [mammutdb.storage :as s]
            [mammutdb.storage.json :as json]
            [mammutdb.config :as config]
            [mammutdb.storage.migrations :as migrations]))

(deftest database-api
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Common crud functions"
    (let [mv1 (api/create-database "foodb1")
          mv2 (api/create-database "foodb2")]
      (is (t/right? mv1))
      (is (t/right? mv2)))

    (let [mv (api/get-all-databases)
          v  (t/from-either mv)]
      (is (t/right? mv))
      (is (vector? v))
      (is (= (count v) 2)))

    (api/drop-database "foodb1")
    (api/drop-database "foodb2")

    (let [mv (api/get-all-databases)
          v  (t/from-either mv)]
      (is (t/right? mv))
      (is (vector? v))
      (is (= (count v) 0))))

  (testing "Get by name"
    (let [mv1 (api/create-database "foodb1")
          mv2 (api/create-database "foodb2")]
      (is (t/right? mv1))
      (is (t/right? mv2)))

    (let [mv1 (api/get-database-by-name "foodb1")
          mv2 (api/get-database-by-name "foodb2")
          v1  (t/from-either mv1)
          v2  (t/from-either mv2)]
      (is (t/right? mv1))
      (is (t/right? mv2))
      (is (s/database? v1))
      (is (s/database? v2)))

    (api/drop-database "foodb1")
    (api/drop-database "foodb2")))

(deftest collection-api
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Common crud functions"
    (let [mdb (api/create-database "foodb")
          mv1 (api/create-collection "foodb" "collname1")
          mv2 (api/create-collection "foodb" "collname2")
          v1  (t/from-either mv1)
          v2  (t/from-either mv2)]
      (is (t/right? mdb))
      (is (t/right? mv1))
      (is (t/right? mv2))
      (is (s/collection? v1))
      (is (s/collection? v2)))

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
      (is (= (count r) 0)))

    (api/drop-database "foodb")))


(deftest document-api
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Common crud functions"
    (let [mdb     (api/create-database "foodb")
          mcoll   (api/create-collection "foodb" "collname1")
          docdata (-> (json/encode {:name "foo"})
                      (t/from-either))
          mdoc    (api/persist-document "foodb" "collname1" docdata)
          doc     (t/from-either mdoc)]
      (is (t/right? mdb))
      (is (t/right? mcoll))
      (is (t/right? mdoc))
      (is (s/document? doc))

      (is (= (.-revid doc) 1))
      (is (= (.-revhash doc) (str "cc56548b1eddfa72a44f9e19ee92edd7"
                                  "03f77583afef68a96b84dfa441ba4bb0")))
      (is (= (.-data doc) {:name "foo"}))
      (let [docdata (-> (s/to-plain-object doc)
                        (assoc :name "bar")
                        (json/encode)
                        (t/from-either))
            mdoc    (api/persist-document "foodb" "collname1" docdata)
            doc     (t/from-either mdoc)]

            (is (t/right? mdoc))
            (is (= (.-revid doc) 2))
            (is (= (.-revhash doc) (str "8ac3ef47802c06fe31c24670318883d4"
                                        "1bc881e8ecc01ab87309ce4d921078db")))
            (is (= (.-data doc) {:name "bar"}))))

    (api/drop-collection "foodb" "collname1")
    (api/drop-database "foodb")))

