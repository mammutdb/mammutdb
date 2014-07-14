;; Copyright (c) 2014 Andrey Antukh <niwi@niwi.be>
;; All rights reserved.
;;
;; Redistribution and use in source and binary forms, with or without
;; modification, are permitted provided that the following conditions
;; are met:
;;
;; 1. Redistributions of source code must retain the above copyright
;;    notice, this list of conditions and the following disclaimer.
;; 2. Redistributions in binary form must reproduce the above copyright
;;    notice, this list of conditions and the following disclaimer in the
;;    documentation and/or other materials provided with the distribution.
;;
;; THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
;; IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
;; OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
;; IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
;; INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
;; NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
;; DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
;; THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
;; (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
;; THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
