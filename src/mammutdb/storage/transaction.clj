(ns mammutdb.storage.transaction
  "Transaction abstractions."
  (:require [cats.core :as m]
            [cats.types :as t]
            [mammutdb.core.error :as err]))

(defn run-in-transaction
  [con func & [{:keys [retries] :or {retries 3}}]]
  (let [con (t/just con)]
    (m/>>= (t/just con) func)))

;; (loop [retry 0]
;;   (try
;;     (m/>>= (t/just con) func)
;;     (catch java.sql.SQLException e
;;       (let [state (.getSQLState e)]
;;         (if (and (= state "40001") (< retry retries))
;;           (recur (inc retry))
;;           (err/error e))))))))
