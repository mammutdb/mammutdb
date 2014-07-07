(ns mammutdb.storage.transaction
  "Transaction abstractions."
  (:require [cats.core :as m]
            [cats.types :as t]
            [mammutdb.storage.connection :as sconn]))


(defn run-in-transaction
  [conn func & [{:keys [retries readonly] :or {retries 3 readonly false}}]]
  (let [conn (t/just conn)]
    (m/>>= conn func)))

(defn transaction
  ([func] (transaction {} func))
  ([options func]
     (m/mlet [conn   (sconn/new-connection)
              result (run-in-transaction conn func options)
              _      (sconn/close-connection conn)]
       (m/return result))))

;; (loop [retry 0]
;;   (try
;;     (m/>>= (t/just con) func)
;;     (catch java.sql.SQLException e
;;       (let [state (.getSQLState e)]
;;         (if (and (= state "40001") (< retry retries))
;;           (recur (inc retry))
;;           (err/error e))))))))
