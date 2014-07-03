(ns mammutdb.test-storage
  (:require [clojure.test :refer :all]
            [cats.types :as t]
            [jdbc.core :as j]
            [mammutdb.storage.collections :as scoll]
            [mammutdb.storage.connection :as sconn]
            [mammutdb.storage.migrations :as migrations]
            [mammutdb.config :as config]))

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

  ;; (testing "Not existence of one collection"
  ;;   (with-open [c (j/make-connection @sconn/datasource)]
  ;;     (is (t/left? (scoll/exists? c "notexistent")))))

  (testing "Get collection."
    (with-open [c (j/make-connection @sconn/datasource)]
      (let [mr (scoll/get-by-name "testcoll" c)
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :collection-not-exists)))

      (let [mr1 (scoll/create "testcoll" c)
            mr2 (scoll/get-by-name "testcoll" c)]
        (is (t/right? mr2))
        (is (= mr1 mr2)))

      (scoll/drop! (scoll/->collection "testcoll") c)))

  (testing "Creating and existence of collection"
    (with-open [c (j/make-connection @sconn/datasource)]
      (let [mr (scoll/create "testcoll" c)
            r  (t/from-either mr)]
        (is (= r (scoll/->collection "testcoll"))))
      (scoll/drop! (scoll/->collection "testcoll") c)))

  (testing "Created duplicate collection"
    (with-open [c (j/make-connection @sconn/datasource)]
      (let [mr (scoll/create "testcoll" c)
            mr (scoll/create "testcoll" c)
            r  (t/from-either mr)]
        (is (= (:error-code r) :collection-exists))
        (is (= (-> r :error-ctx :sqlstate) :42P07)))
      (scoll/drop! (scoll/->collection "testcoll") c)))
)





