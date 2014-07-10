(ns mammutdb.test-uuid
   (:require [clojure.test :refer :all]
             [cats.types :as t]
             [mammutdb.core.uuid :as uuid]))

(deftest test-str->muuid
  (is (t/left? (uuid/str->muuid "__")))
  (is (t/right? (uuid/str->muuid "550e8400-e29b-41d4-a716-446655440000"))))
