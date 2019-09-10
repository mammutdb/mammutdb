;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) 2019 Andrey Antukh <niwi@niwi.nz>

(ns mammutdb.tx.proto)

(defprotocol Transactor
  (init! [_] "Initialize transactor")
  (submit! [_ txdata] "Submit a transaction into the transactor.")
  (consumer [_ opts] "Creates a consumer instance."))

(defprotocol TransactorConsumer
  (poll [_ opts] "Request a next batch of tx records."))
