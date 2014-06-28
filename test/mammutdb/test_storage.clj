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
    (is (t/left? (scoll/safe-name? "ddd@ddd"))))

  (testing "Not existence of one collection"
    (with-open [c (j/make-connection @sconn/datasource)]
      (is (t/left? (scoll/exists? c "notexistent")))))
)





