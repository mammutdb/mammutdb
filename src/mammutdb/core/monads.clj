(ns mammutdb.core.monads
  "Main entry point for monads types."
  (:require [mammutdb.core.monads.protocols :as p]
            [mammutdb.core.monads.types :as types]))


(def ^{:dynamic true :private true} *m-context*)

(defmacro with-context
  [ctx & body]
  `(binding [*m-context* ~ctx]
     ~@body))

(defn return
  "Context dependent version of pure."
  [v]
  (p/pure *m-context* v))

(defn pure
  "Takes a context type av value and any arbitrary
  value v, and return v value wrapped in a minimal
  contex of same type of av."
  [av v]
  (p/pure av v))

(defn bind
  "Given a value inside monadic context mv and any function,
  applies a function to value of mv."
  [mv f]
  (with-context mv
    (p/bind mv f)))

(defn fmap
  "Apply a function f to the value inside functor's fv
  preserving the context type."
  [f fv]
  (p/fmap fv f))

(defn fapply
  "Given function inside af's conext and value inside
  av's context, applies the function to value and return
  a result wrapped in context of same type of av context."
  [af av]
  (p/fapply af av))

(defn >>=
  "Performs a Haskell-style left-associative bind."
  ([mv f]
     (p/bind mv f))
  ([mv f & fs]
     (reduce bind mv (cons f fs))))

(defmacro mlet
  [bindings body]
  (when-not (and (vector? bindings) (even? (count bindings)))
    (throw (IllegalArgumentException. "bindings has to be a vector with even number of elements.")))
  (if (seq bindings)
    (let [sym (get bindings 0)
          monad (get bindings 1)]
      `(bind ~monad
             (fn [~sym]
               (mlet ~(subvec bindings 2) ~body))))
    body))
