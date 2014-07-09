(ns mammutdb.test-query
  (:require [clojure.test :refer :all]
            [mammutdb.storage.query :as sq]))

(deftest querybuilder
  (testing "Make query with basic gt clause"
    (let [fieldname "name"
          querydata [:gt 2]
          result    (sq/make-filter fieldname querydata)]
      (is (= result ["(name > ?)" 2]))))

  (testing "Make query with basic lt clause"
    (let [fieldname "name"
          querydata [:lt 2]
          result    (sq/make-filter fieldname querydata)]
      (is (= result ["(name < ?)" 2]))))

  (testing "Make query using :or combinator with two subclauses"
    (let [fieldname "name"
          querydata [:or [:gt 20] [:lt 100]]
          result    (sq/make-filter fieldname querydata)]
      (is (= result ["((name > ?) OR (name < ?))" 20 100]))))

  (testing "Make query using :and combinator with two subclauses"
    (let [fieldname "name"
          querydata [:and [:gt 20] [:lt 100]]
          result    (sq/make-filter fieldname querydata)]
      (is (= result ["((name > ?) AND (name < ?))" 20 100]))))
)
