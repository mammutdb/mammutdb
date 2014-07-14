(ns mammutdb.test-http
  (:require [clojure.test :refer :all]
            [cats.monad.either :as either]
            [mammutdb.api :as api]
            [mammutdb.storage :as s]
            [mammutdb.storage.json :as json]
            [mammutdb.config :as config]
            [mammutdb.storage.migrations :as migrations]
            [mammutdb.transports.http.controllers :as ctrls]))

;; TODO: Test wrong cases.

(deftest database-api
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "List databases"
    (let [res (ctrls/databases-list {})]
      (is (= (:status res) 200))
      (is (= (count (:body res)) 0)))

    (api/create-database "sampledb")
    (let [res (ctrls/databases-list {})]
      (is (= (:status res) 200))
      (is (= (count (:body res)) 1)))

    (api/drop-database "sampledb"))

  (testing "Create database"
    (let [req {:params {:dbname "sampledb"}}
          res (ctrls/databases-create req)]
      (is (= (:status res) 201))
      (is (= (get-in res [:body :name] "sampledb"))))
    (api/drop-database "sampledb"))

  (testing "Drop database"
    (api/create-database "sampledb")
    (let [req {:params {:dbname "sampledb"}}
          res (ctrls/databases-drop req)]
      (is (= (:status res) 204))
      (is (= (:body res) "")))
    (let [res (ctrls/databases-list {})]
      (is (= (:status res) 200))
      (is (= (count (:body res)) 0))))
)


(deftest collection-api
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)
  (api/create-database "sampledb")

  (testing "List Collections"
    (let [req {:params {:dbname "sampledb"}}
          res (ctrls/collection-list req)]
      (is (= (:status res) 200))
      (is (= (count (:body res)) 0)))

    (api/create-collection "sampledb" "samplecoll")
    (let [req {:params {:dbname "sampledb"}}
          res (ctrls/collection-list req)]
      (is (= (:status res) 200))
      (is (= (count (:body res)) 1)))

    (api/drop-collection "sampledb" "samplecoll"))

  (testing "Collection create"
    (let [req {:params {:dbname "sampledb"
                        :collname "samplecoll"}}
          res (ctrls/collection-create req)]
      (is (= (:status res) 201))
      (is (= (get-in res [:body :name] "samplecoll"))))
    (let [req {:params {:dbname "sampledb"}}
          res (ctrls/collection-list req)]
      (is (= (:status res) 200))
      (is (= (count (:body res)) 1)))
    (api/drop-collection "sampledb" "samplecoll"))

  (testing "Collection delete"
    (api/create-collection "sampledb" "samplecoll")

    (let [req {:params {:dbname "sampledb"
                        :collname "samplecoll"}}
          res (ctrls/collection-drop req)]
      (is (= (:status res) 204))
      (is (= (:body res) "")))
    (let [req {:params {:dbname "sampledb"}}
          res (ctrls/collection-list req)]
      (is (= (:status res) 200))
      (is (= (count (:body res)) 0))))

  (api/drop-database "sampledb"))

(defn string-reader
  [data]
  (java.io.StringReader. data))

(deftest document-api
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)
  (api/create-database "sampledb")
  (api/create-collection "sampledb" "samplecoll")

  (testing "Create document"
    (let [req {:params {:dbname "sampledb"
                        :collname "samplecoll"}
               :body (-> (json/encode {:foo "bar"})
                         (either/from-either)
                         (string-reader))}
          res (ctrls/document-create req)]
      (is (= (:status res) 201))
      (is (= (get-in res [:body :foo] "bar")))))

  (api/drop-collection "sampledb" "samplecoll")
  (api/drop-database "sampledb"))
