(ns mammutdb.tests-config
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [mammutdb.config :as config]
            [cats.core :as m]
            [cats.types :as t]))

(deftest config
  (testing "Get config file path"
    (binding [config/*config-path* "test/testconfig.edn"]
      (let [configpath (config/get-configfile-path)]
        (is (= (t/right "test/testconfig.edn") configpath)))))

  (testing "Read config file"
    (let [conf (config/read-config "test/testconfig.edn")]
      (is (t/right? conf))
      (let [v (t/from-either conf)]
        (is (:transport v))
        (is (:storage v)))))

  (testing "Read transports config"
    (binding [config/*config-path* "test/testconfig.edn"]
      (let [conf (config/read-transport-config)]
        (is (t/right? conf))
        (is (:path (t/from-either conf))))))

  (testing "Read transports config"
    (binding [config/*config-path* "test/testconfig.edn"]
      (let [conf (config/read-storage-config)]
        (is (t/right? conf))
        (is (:subname (t/from-either conf)))))))
