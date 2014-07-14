(ns mammutdb.test-api
  (:require [clojure.test :refer :all]
            [cats.monad.either :as either]
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
      (is (either/right? mv1))
      (is (either/right? mv2)))

    (let [mv (api/get-all-databases)
          v  (either/from-either mv)]
      (is (either/right? mv))
      (is (vector? v))
      (is (= (count v) 2)))

    (api/drop-database "foodb1")
    (api/drop-database "foodb2")

    (let [mv (api/get-all-databases)
          v  (either/from-either mv)]
      (is (either/right? mv))
      (is (vector? v))
      (is (= (count v) 0))))

  (testing "Get by name"
    (let [mv1 (api/create-database "foodb1")
          mv2 (api/create-database "foodb2")]
      (is (either/right? mv1))
      (is (either/right? mv2)))

    (let [mv1 (api/get-database-by-name "foodb1")
          mv2 (api/get-database-by-name "foodb2")
          v1  (either/from-either mv1)
          v2  (either/from-either mv2)]
      (is (either/right? mv1))
      (is (either/right? mv2))
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
          v1  (either/from-either mv1)
          v2  (either/from-either mv2)]
      (is (either/right? mdb))
      (is (either/right? mv1))
      (is (either/right? mv2))
      (is (s/collection? v1))
      (is (s/collection? v2)))

    (let [mr (api/get-all-collections "foodb")
          r  (either/from-either mr)]
      (is (either/right? mr))
      (is (vector? r))
      (is (= (count r) 2)))

    (api/drop-collection "foodb" "collname1")
    (api/drop-collection "foodb" "collname2")

    (let [mr (api/get-all-collections "foodb")
          r  (either/from-either mr)]
      (is (either/right? mr))
      (is (vector? r))
      (is (= (count r) 0)))

    (api/drop-database "foodb")))


(deftest document-api
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Get by id"
    (let [mdb     (api/create-database "foodb")
          mcoll   (api/create-collection "foodb" "collname1")
          docdata (-> (json/encode {:name "foo"})
                      (either/from-either))
          mdoc1   (api/persist-document "foodb" "collname1" docdata)
          doc1    (either/from-either mdoc1)]
      (let [mdoc2 (api/get-document-by-id "foodb", "collname1", (.-id doc1))
            doc2  (either/from-either mdoc2)]
        (is (either/right? mdoc2))
        (is (= (.-data doc2) (.-data doc1)))))

    (api/drop-collection "foodb" "collname1")
    (api/drop-database "foodb"))

  (testing "Common crud functions"
    (let [mdb     (api/create-database "foodb")
          mcoll   (api/create-collection "foodb" "collname1")
          docdata (-> (json/encode {:name "foo"})
                      (either/from-either))
          mdoc    (api/persist-document "foodb" "collname1" docdata)
          doc     (either/from-either mdoc)]
      (is (either/right? mdb))
      (is (either/right? mcoll))
      (is (either/right? mdoc))
      (is (s/document? doc))

      (is (= (.-revid doc) 1))
      (is (= (.-revhash doc) (str "cc56548b1eddfa72a44f9e19ee92edd7"
                                  "03f77583afef68a96b84dfa441ba4bb0")))
      (is (= (.-data doc) {:name "foo"}))
      (let [docdata (-> (s/to-plain-object doc)
                        (assoc :name "bar")
                        (json/encode)
                        (either/from-either))
            mdoc    (api/persist-document "foodb" "collname1" docdata)
            doc     (either/from-either mdoc)]

            (is (either/right? mdoc))
            (is (= (.-revid doc) 2))
            (is (= (.-revhash doc) (str "8ac3ef47802c06fe31c24670318883d4"
                                        "1bc881e8ecc01ab87309ce4d921078db")))
            (is (= (.-data doc) {:name "bar"}))))

    (api/drop-collection "foodb" "collname1")
    (api/drop-database "foodb"))

  (let [mdb     (api/create-database "foodb")
        mcoll   (api/create-collection "foodb" "collname1")]

      ;; Empty document
    (testing "Persist wrong data with empty document"
      (let [mdoc (api/persist-document "foodb" "collname1" "")
            doc  (either/from-either mdoc)]
        (is (either/left? mdoc))
        (is (= (:error-code doc) :invalid-json-format))))

    (api/drop-collection "foodb" "collname1")
    (api/drop-database "foodb"))

)

