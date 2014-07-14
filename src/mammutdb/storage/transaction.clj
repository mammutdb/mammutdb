(ns mammutdb.storage.transaction
  "Transaction abstractions."
  (:require [jdbc.transaction :as trans]
            [cats.core :as m]
            [cats.monad.either :as either]
            [mammutdb.logging :refer [log]]
            [mammutdb.core.errors :as err]
            [mammutdb.storage.connection :as sconn]))

(defn run-in-transaction
  [conn func & [{:keys [retries readonly] :or {retries 3 readonly false}}]]
  (loop [current-try 1]
    (log :debug (format "Running a transaction, try %s of %s" current-try retries))
    (let [tx-opts {:isolation-level :serializable
                   :read-only       readonly}
          tx-res (trans/with-transaction conn tx-opts
                   (m/>>= (either/right conn) func))]
      (if (and (either/left? tx-res)
               (= (:error-code (either/from-either tx-res))
                  :serialization-failure)
               (<= current-try retries))
        (recur (inc current-try))
        tx-res))))

(defn transaction
  ([func] (transaction {} func))
  ([options func]
     (m/mlet [conn   (sconn/new-connection)
              result (run-in-transaction conn func options)
              _      (sconn/close-connection conn)]
       (m/return result))))
