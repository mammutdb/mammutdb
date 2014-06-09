(ns mammutdb.core.lifecycle
  "Lifecycle protocol definition")

(defprotocol Lifecycle
  (start [_] "Initialize/Start process.")
  (stop [_] "Die/Stop process."))
