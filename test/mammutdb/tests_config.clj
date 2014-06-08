(ns mammutdb.tests-config
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [mammutdb.config :as cfg]
            [mammutdb.core.monads :as m]
            [mammutdb.core.monads.types :as t]))


(deftest config
  (testing "Get config file path"
    (binding [cfg/*config-path* "test/testconfig.edn"]
      (let [cfgpath (cfg/get-configfile-path)]
        (is (= (t/right "test/testconfig.edn") cfgpath)))))

  (testing "Read config file"
    (binding [cfg/*config-path* "test/testconfig.edn"]
      (let [conf (cfg/read-config)]
        (is (t/right? conf))
        (let [v (t/from-either conf)]
          (is (:transport v))
          (is (:storage v))))))

  (testing "Read transports config"
    (binding [cfg/*config-path* "test/testconfig.edn"]
      (let [conf (cfg/read-transport-config)]
        (is (t/right? conf))
        (is (:path (t/from-either conf))))))

  (testing "Read transports config"
    (binding [cfg/*config-path* "test/testconfig.edn"]
      (let [conf (cfg/read-storage-config)]
        (is (t/right? conf))
        (is (:name (t/from-either conf)))))))
