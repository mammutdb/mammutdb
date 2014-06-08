(ns mammutdb.core.monads.types
  "Monadic types definition."
  (:require [mammutdb.core.monads.protocols :as proto]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Either
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype Either [v type]
  Object
  (equals [self other]
    (if (instance? Either other)
      (and (= v (.v other))
           (= type (.type other)))
      false))

  (toString [self]
    (with-out-str (print [v type])))

  proto/Monad
  (bind [self f]
    (if-not (= type :left)
      (Either. (f v) :right)
      self)))

(defn left
  "Left constructor for Either type."
  [^Object v]
  (Either. v :left))

(defn right
  "Right constructor for Either type."
  [^Object v]
  (Either. v :right))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Either
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype Just [v]
  Object
  (equals [self other]
    (if (instance? Just other)
      (= v (.v other))
      false))

  (toString [self]
    (with-out-str (print [v])))

  proto/Monad
  (bind [self f]
    (Just. (f v))))

(deftype Nothing []
  Object
  (equals [self other]
    (instance? Nothing other))

  (toString [self]
    (with-out-str (print "")))

  proto/Monad
  (bind [self f]
    self))

(defn just
  [v]
  (Just. v))

(defn nothing
  []
  (Nothing.))
