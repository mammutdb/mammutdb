(ns mammutdb.test-transaction
  (:require [clojure.test :refer :all]
            [cats.monad.either :as either]
            [cats.core :as m]
            [jdbc.core :as j]
            [mammutdb.core.errors :as err]
            [mammutdb.storage.connection :as sconn]
            [mammutdb.storage.migrations :as migrations]
            [mammutdb.storage.transaction :as tx]
            [mammutdb.config :as config]))

(deftest run-in-transaction
  ;; Setup
  (config/setup-config! "test/testconfig.edn")

  (testing "It runs the function once if all goes well"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [called (atom 0)
            inc-call-count (fn [_] (m/return (swap! called inc)))
            mr  (tx/run-in-transaction conn
                                       inc-call-count)]
        (is (either/right? mr))
        (is (= @called 1)))))

  (testing "It runs as many retries as we tell it when failing"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [retries 10
            expected-calls (inc retries)
            called (atom 0)
            error-inc-call-count (fn [_]
                                   (swap! called inc)
                                   (err/error :serialization-failure
                                              nil
                                              {:error-code :serialization-failure}))
            mr  (tx/run-in-transaction conn
                                       error-inc-call-count
                                       {:retries retries})]
        (is (either/left? mr))
        (is (= @called expected-calls)))))

  (testing "It runs as many tries it has to and no more"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [retries 3
            expected-calls 2
            called (atom 0)
            error-and-success (fn [_]
                                (swap! called inc)
                                (if (=  @called 1)
                                  (do
                                    (err/error :serialization-failure
                                               nil
                                               {:error-code :serialization-failure}))
                                   (m/return nil)))
            mr  (tx/run-in-transaction conn
                                       error-and-success
                                       {:retries retries})]
        (is (either/right? mr))
        (is (= @called expected-calls)))))
)
