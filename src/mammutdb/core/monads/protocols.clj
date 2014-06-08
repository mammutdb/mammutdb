(ns mammutdb.core.monads.protocols
  "Monadic types definition.")

(defprotocol Monad
  "Incomplete monad type definition."
  (bind [mv f] "Applies the function f to the value(s) inside mv's context."))


(defprotocol Applicative
  "Incomplete applicative type definition."
  (pure [ctx v]
    "Takes any context monadic value ctx and any value v, and puts
     the value v in the most minimal context of same type of ctx"))
