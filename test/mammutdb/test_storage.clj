(ns mammutdb.test-storage
  (:require [clojure.test :refer :all]
            [cats.types :as t]
            [jdbc.core :as j]
            [clj-time.core :as jt]
            [clj-time.coerce :as jc]
            [mammutdb.storage :as storage]
            [mammutdb.storage.connection :as sconn]
            [mammutdb.storage.migrations :as migrations]
            [mammutdb.config :as config]))

(deftest databases
  ;; Setup
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Not existence of database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr (storage/database-exists? "notexistsdb" conn)
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :database-does-not-exist)))))

  (testing "Create/Delete database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr (storage/create-database "testdb" conn)
            r  (t/from-either mr)]
        (is (t/right? mr))
        (is (= r (storage/->database "testdb"))))

      (let [mr (storage/database-exists? "testdb" conn)
            r  (t/from-either mr)]
        (is (t/right? mr))
        (is r))

      (storage/drop-database (storage/->database "testdb") conn)))

  (testing "List created databases"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr1 (storage/create-database "testdb1" conn)
            mr2 (storage/create-database "testdb2" conn)
            mr3 (storage/get-all-databases conn)
            r   (t/from-either mr3)]
        (is (t/right? mr3))
        (is (vector? r))
        (is (= (count r) 2))
        (is (storage/database? (first r)))

        (storage/drop-database (t/from-either mr1) conn)
        (storage/drop-database (t/from-either mr2) conn))))

  (testing "Create duplicate database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr1 (storage/create-database "testdb" conn)
            mr2 (storage/create-database "testdb" conn)
            r   (t/from-either mr2)]
        (is (t/right? mr1))
        (is (t/left? mr2))
        (is (= (:error-code r) :database-exists)))

      (storage/drop-database (storage/->database "testdb") conn)))
)

(deftest collections
  ;; Setup
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Not existence of one collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (storage/->database "testdb")
            mr (storage/collection-exists? db "notexistent" conn)
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :collection-does-not-exist)))))

  (testing "Create/Delete collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (storage/->database "testdb")]
        (let [mr (storage/create-collection db "testcoll" :json conn)
              r  (t/from-either mr)]
          (is (t/right? mr))
          (is (= r (storage/->collection db "testcoll" :json))))
        (let [mr (storage/collection-exists? db "testcoll" conn)
              r  (t/from-either mr)]
          (is (t/right? mr))
          (is r))

        (storage/drop-collection (storage/->collection db "testcoll" :json) conn))))

  (testing "Created duplicate collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (storage/->database "testdb")]
        (let [mr1 (storage/create-collection db "testcoll" :json conn)
              mr2 (storage/create-collection db "testcoll" :json conn)
              r   (t/from-either mr2)]
          (is (t/right? mr1))
          (is (t/left? mr2))
          (is (= (:error-code r) :collection-exists))
          (is (= (-> r :error-ctx :sqlstate) :42P07)))

        (storage/drop-collection (storage/->collection db "testcoll" :json) conn))))
)

(deftest documents
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Json type document factory"
    (let [db  (storage/->database "testdb")
          cl  (storage/->collection db "testcoll" :json)
          doc (storage/->document cl 1 1 {} nil)]
      (is (instance? mammutdb.storage.document.JsonDocument doc))))

  (testing "Json document persistence"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db   (storage/->database "testdb")
            coll (t/from-either (storage/create-collection db "testcoll" :json conn))
            doc1 (storage/->document coll nil nil {:foo 1} nil)
            doc2 (t/from-either (storage/persist-document coll doc1 conn))
            doc3 (t/from-either (storage/persist-document coll doc2 conn))]
        (is (= (.-data doc1) (.-data doc2)))
        (is (= (.-data doc1) (.-data doc3)))
        (is (= (.-id doc2) (.-id doc3)))
        (is (not= (.-rev doc2) (.-rev doc3)))
        (is (<
             (jc/to-long (.-createdat doc2))
             (jc/to-long (.-createdat doc3))))

        (storage/drop-collection coll conn))))
)
