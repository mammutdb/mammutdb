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
  [v]
  (p/pure *m-context* v))

(defn bind
  [mv f]
  (with-context mv
    (p/bind mv f)))

(defn >>=
  "Performs a Haskell-style left-associative bind."
  ([mv f]
     (p/bind mv f))
  ([mv f & fs]
     (reduce bind mv (cons f fs))))

(defmacro mdo
  [bindings body]
  (when-not (and (vector? bindings) (even? (count bindings)))
    (throw (IllegalArgumentException. "bindings has to be a vector with even number of elements.")))

  (if (seq bindings)
    (let [sym (get bindings 0)
          monad (get bindings 1)]
      `(bind ~monad
             (fn [~sym]
               (mdo ~(subvec bindings 2) ~body))))
    body))
