(ns mammutdb.test-storage
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [cats.core :as m]
            [cats.types :as t]
            [jdbc.core :as j]
            [mammutdb.storage.collections :as scoll]
            [mammutdb.storage.connection :as sconn]
            [mammutdb.storage.migrations :as smigr]
            [mammutdb.config :as conf]
            [mammutdb.auth :as auth]
            [mammutdb.storage.users :as users]))


(deftest collections
  ;; Setup
  (conf/setup-config "test/testconfig.edn")
  (smigr/bootstrap)

  (testing "Collections name safety"
    (is (t/right? (scoll/safe-name? "testcollname")))
    (let [r (scoll/safe-name? "ddd@ddd")
          i (t/from-either r)]

      (is (t/left? r))
      (is (= (:error-code i) :collection-name-unsafe))))

  ;; (testing "Not existence of one collection"
  ;;   (with-open [c (j/make-connection @sconn/datasource)]
  ;;     (is (t/left? (scoll/exists? c "notexistent")))))

  (testing "Not existence of one collection"
    (with-open [c (j/make-connection @sconn/datasource)]
      (is (t/left? (scoll/exists? c "notexistent")))))

  (testing "Creating and existence of collection"
    (with-open [c (j/make-connection @sconn/datasource)]
      (let [coll (scoll/create c "testcoll")
            data (t/from-either coll)]
        (is (= (t/from-either coll) (scoll/->collection "testcoll"))))
      (scoll/drop c (scoll/->collection "testcoll"))))

  (testing "Created duplicate collection"
    (with-open [c (j/make-connection @sconn/datasource)]
      (let [coll (scoll/create c "testcoll")
            coll (scoll/create c "testcoll")
            data (t/from-either coll)]
        (is (= (:error-code data) :collection-exists))
        (is (= (-> data :error-ctx :sqlstate) :42P07)))
      (scoll/drop c (scoll/->collection "testcoll"))))
)





