(ns mammutdb.storage.transaction
  "Transaction abstractions."
  (:require [cats.types :as t]
            [cats.core :as m]
            [mammutdb.storage.connections :as conn]
            [mammutdb.storage.collections :as scoll]
            [mammutdb.storage.documents :as sdoc]
            [mammutdb.core.edn :as edn]
            [mammutdb.core.uuid :refer [str->muuid]]
            [mammutdb.core.error :as err]))

(defn run-in-transaction
  [con func & [{:keys [retries] :or {retries 3}}]]
  (let [con (just con)]
    (loop [retry 0]
      (try
        (m/>>= (just con) func)
        (catch java.sql.SQLException e
          (let [state (.getSQLState e)]
            (if (and (= state "40001") (< retry retries))
              (recur (inc retry))
              (t/left e)))))))
