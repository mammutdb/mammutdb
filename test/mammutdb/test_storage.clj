(ns mammutdb.test-storage
  (:require [clojure.test :refer :all]
            [cats.types :as t]
            [jdbc.core :as j]
            [mammutdb.storage.collection :as scoll]
            [mammutdb.storage.database :as sdb]
            [mammutdb.storage.types :as stypes]
            [mammutdb.storage.connection :as sconn]
            [mammutdb.storage.migrations :as migrations]
            [mammutdb.config :as config]))

(deftest databases
  ;; Setup
  (config/setup-config "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Database name safety"
    (is (t/right? (sdb/safe-name? "testdbname")))
    (let [r (sdb/safe-name? "dbname@")
          i (t/from-either r)]
      (is (t/left? r))
      (is (= (:error-code i) :database-name-unsafe))))

  (testing "Not existence of database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr (sdb/exists? "notexistsdb" conn)
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :database-not-exists)))))

  (testing "Create/Delete database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr (sdb/create! "testdb" conn)
            r  (t/from-either mr)]
        (is (t/right? mr))
        (is (= r (sdb/->database "testdb"))))

      (let [mr (sdb/exists? "testdb" conn)
            r  (t/from-either mr)]
        (is (t/right? mr))
        (is r))

      (sdb/drop! (sdb/->database "testdb") conn)))

  (testing "Create duplicate database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr1 (sdb/create! "testdb" conn)
            mr2 (sdb/create! "testdb" conn)
            r   (t/from-either mr2)]
        (is (t/right? mr1))
        (is (t/left? mr2)))
      ;; TODO: test error code
      (sdb/drop! (sdb/->database "testdb") conn)))
)

(deftest collections
  ;; Setup
  (config/setup-config "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Collections name safety"
    (is (t/right? (scoll/safe-name? "testcollname")))
    (let [r (scoll/safe-name? "ddd@ddd")
          i (t/from-either r)]

      (is (t/left? r))
      (is (= (:error-code i) :collection-name-unsafe))))

  (testing "Not existence of one collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (sdb/->database "testdb")
            mr (scoll/exists? db "notexistent" conn)
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :collection-not-exists)))))

  (testing "Create/Delete collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (sdb/->database "testdb")]
        (let [mr (scoll/create! db "testcoll" :json conn)
              r  (t/from-either mr)]
          (is (t/right? mr))
          (is (= r (scoll/->collection db "testcoll" :json))))
        (let [mr (scoll/exists? db "testcoll" conn)
              r  (t/from-either mr)]
          (is (t/right? mr))
          (is r))
        (scoll/drop! (scoll/->collection db "testcoll" :json) conn))))

  (testing "Created duplicate collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (sdb/->database "testdb")]
        (let [mr1 (scoll/create! db "testcoll" :json conn)
              mr2 (scoll/create! db "testcoll" :json conn)
              r   (t/from-either mr2)]
          (is (t/right? mr1))
          (is (t/left? mr2))
          (is (= (:error-code r) :collection-exists))
          (is (= (-> r :error-ctx :sqlstate) :42P07)))
        (scoll/drop! (scoll/->collection db "testcoll" :json) conn))))
)





