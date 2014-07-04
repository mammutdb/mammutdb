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
    (with-open [con (j/make-connection @sconn/datasource)]
      (let [mr (sdb/exists? "notexistsdb" con)
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :database-not-exists)))))

  (testing "Create/Delete database"
    (with-open [con (j/make-connection @sconn/datasource)]
      (let [mr (sdb/create "testdb" con)
            r  (t/from-either mr)]
        (is (t/right? mr))
        (is (= r (stypes/->database "testdb"))))

      (let [mr (sdb/exists? "testdb" con)
            r  (t/from-either mr)]
        (is (t/right? mr))
        (is r))

      (sdb/drop! (stypes/->database "testdb") con)))

  (testing "Create duplicate database"
    (with-open [con (j/make-connection @sconn/datasource)]
      (let [mr1 (sdb/create "testdb" con)
            mr2 (sdb/create "testdb" con)
            r   (t/from-either mr2)]
        (is (t/right? mr1))
        (is (t/left? mr2)))
      ;; TODO: test error code
      (sdb/drop! (stypes/->database "testdb") con)))
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
    (with-open [con (j/make-connection @sconn/datasource)]
      (let [db (stypes/->database "testdb")
            mr (scoll/exists? db "notexistent" con)
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :collection-not-exists)))))

  (testing "Create/Delete collection"
    (with-open [con (j/make-connection @sconn/datasource)]
      (let [db (stypes/->database "testdb")]
        (let [mr (scoll/create db "testcoll" con)
              r  (t/from-either mr)]
          (is (t/right? mr))
          (is (= r (stypes/->doc-collection db "testcoll"))))
        (let [mr (scoll/exists? db "testcoll" con)
              r  (t/from-either mr)]
          (is (t/right? mr))
          (is r))
        (scoll/drop! (stypes/->doc-collection db "testcoll") con))))

  (testing "Created duplicate collection"
    (with-open [con (j/make-connection @sconn/datasource)]
      (let [db (stypes/->database "testdb")]
        (let [mr1 (scoll/create db "testcoll" con)
              mr2 (scoll/create db "testcoll" con)
              r   (t/from-either mr2)]
          (is (t/right? mr1))
          (is (t/left? mr2))
          (is (= (:error-code r) :collection-exists))
          (is (= (-> r :error-ctx :sqlstate) :42P07)))
        (scoll/drop! (stypes/->doc-collection db "testcoll") con))))
)





