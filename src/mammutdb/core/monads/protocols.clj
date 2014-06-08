(ns mammutdb.core.monads.protocols
  "Monadic types definition.")

(defprotocol Monad
  (bind [mv f] "Applies the function f to the value(s) inside mv's context."))
