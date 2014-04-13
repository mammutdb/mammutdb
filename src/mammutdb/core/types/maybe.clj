(ns mammutdb.core.types.maybe
  "Maybe like monad implementation."
  (:require [clojure.algo.monads :refer [defmonad domonad]]
            [clojure.core.match :refer [match]]))

(defrecord Maybe [t v e])
(alter-meta! #'->Maybe assoc :no-doc true :private true)
(alter-meta! #'map->Maybe assoc :no-doc true :private true)

(defn ok [v] (->Maybe :ok v nil))
(defn fail [k] (->Maybe :fail nil k))
(defn exception [e] (->Maybe :exc nil e)

(defn- m-result-mm
  [value]
  value)

(defn- m-bind-mm
  [mv f]
  (match mv
    {:v a :e nil} (f a)
    {:v nil :e a} mv))

    ;; :else   (f mv)))

(defmonad maybe-mm
  [m-result m-result-mm
   m-bind   m-bind-mm])

(defmacro mlet
  [ops & expr]
  `(domonad maybe-mm ~ops ~@expr))

(defn m-apply
  [val & fns]
  (if (seq fns)
    (doall (reduce (fn [lastresult f] (f lastresult)) val fns))
    val))


