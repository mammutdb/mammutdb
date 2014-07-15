(ns mammutdb.test-query
  (:require [clojure.test :refer :all]
            [mammutdb.storage.query :as sq]))

(deftest filterbuilder
  (testing "Make query with basic gt modifier."
    (let [criteria [:gt "field" 2]
          result   (sq/build-where-clause criteria)]
      (is (= result ["data->'field' > ?" 2]))))

  (testing "Make query with basic gt modifier using special fields"
    (let [criteria [:gt "_id" "foo"]
          result   (sq/build-where-clause criteria)]
      (is (= result ["id > ?" "foo"]))))

  (testing "Make query with basic gte modifier."
    (let [criteria [:gte "field" 2]
          result   (sq/build-where-clause criteria)]
      (is (= result ["data->'field' >= ?" 2]))))

  (testing "Make query with basic lt modifier."
    (let [criteria [:lt "field" 2]
          result   (sq/build-where-clause criteria)]
      (is (= result ["data->'field' < ?" 2]))))

  (testing "Make query with basic lte modifier."
    (let [criteria [:lte "field" 2]
          result   (sq/build-where-clause criteria)]
      (is (= result ["data->'field' <= ?" 2]))))

  (testing "Make query with basic eq modifier."
    (let [criteria [:eq "field" 2]
          result   (sq/build-where-clause criteria)]
      (is (= result ["data->'field' = ?" 2]))))

  (testing "Make query with and logic operator."
    (let [criteria [:and [:gt "_field" 1] [:lt "_field" 10]]
          result   (sq/build-where-clause criteria)]
      (is (= result ["field > ? AND field < ?" 1 10]))))

  (testing "Make query with or logic operator."
    (let [criteria [:or [:gt "_field" 1] [:lt "_field" 10]]
          result   (sq/build-where-clause criteria)]
      (is (= result ["field > ? OR field < ?" 1 10]))))

  (testing "Make query with nested and & or"
    (let [criteria [:and [:or [:eq "_field1" 1] [:eq "_field1" 2]]
                         [:eq "_field2" 3]]
          result   (sq/build-where-clause criteria)]
      (is (= result ["(field1 = ? OR field1 = ?) AND field2 = ?" 1 2 3]))))

  (testing "Build basic query"
    (let [result (sq/build-query {:select [:*]
                                  :where [:eq "field" 1]
                                  :table :table
                                  :limit 10
                                  :offset 2
                                  :orderby [:field]})]
      (is (= [(str "SELECT * FROM table WHERE data->'field' = ? "
                   "ORDER BY data->'field' ASC LIMIT ? OFFSET ?") 1 10 2] result))))
)
