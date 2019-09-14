(ns user
  (:require
   [mount.core :as mount :refer [defstate]]
   [clj-uuid :as uuid]
   [clojure.pprint :refer [pprint]]
   [clojure.test :as test]
   [clojure.tools.namespace.repl :as r]
   [clojure.walk :refer [macroexpand-all]]
   [mammutdb.txlog :as txlog]
   [mammutdb.txlog.psql :as txpg]
   [mammutdb.util.vertx-pgsql :as pg]))

(defn- run-tests
  ([] (run-tests #"^mammutdb.tests.*"))
  ([o]
   (r/refresh)
   (cond
     (instance? java.util.regex.Pattern o)
     (test/run-all-tests o)

     (symbol? o)
     (if-let [sns (namespace o)]
       (do (require (symbol sns))
           (test/test-vars [(resolve o)]))
       (test/test-ns o)))))

(defstate pool
  :start (pg/pool "postgresql://test@localhost/test")
  :stop (.close pool))

(defstate producer
  :start (txlog/producer {::txlog/type ::txlog/psql
                          ::txpg/pool pool})
  :stop (.close producer))

(defstate consumer
  :start (txlog/consumer {::txlog/type ::txlog/psql
                          ::txpg/pool pool})
  :stop (.close consumer))

(defn start
  []
  (mount/start))

;; (comment
;;   (let [txid (uuid/v1)
;;         doc {:name "Andrey" :surname "Antukh"}]
;;   (txlog/submit! producer txid [::put doc])

;; (db/submit! [[::db/createns :persons]])

;; (db/submit! [[::db/put :persons {::db/id 1 :name "Andrei" :age 31}]
;;              [::db/put :persons {::db/id 2 :name "Vesi" :age 30}]
;;              [::db/update :persons {::db/id 1 :name "Andrey"}]])
