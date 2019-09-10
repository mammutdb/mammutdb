(ns user
  (:require
   [mount.core :as mount :refer [defstate]]
   [clojure.pprint :refer [pprint]]
   [clojure.test :as test]
   [clojure.tools.namespace.repl :as r]
   [clojure.walk :refer [macroexpand-all]]
   [mammutdb.tx :as tx]
   [mammutdb.tx.psql :as txpg]
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

(defstate transactor
  :start (tx/transactor {::tx/type ::tx/psql
                         ::txpg/pool pool})
  :stop (.close transactor))

(defstate consumer
  :start (tx/consumer transactor)
  :stop (.close consumer))

(defn start
  []
  (mount/start))
