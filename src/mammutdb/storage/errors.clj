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

(ns mammutdb.storage.errors
  "Storage specific error management."
  (:require [clojure.java.io :as io]
            [mammutdb.core.errors :refer [error]]
            [mammutdb.config :as config]
            [mammutdb.core.edn :as edn]
            [cats.monad.either :as either]
            [cats.core :as m]))

(def ^{:dynamic true
       :doc "PostgreSQL -> MammutDB error code translation map."}
  *pgsql-error-codes*
  {:42P07  :collection-exists
   :40001  :serialization-failure})

(defn resolve-pgsql-error
  [e]
  (let [state      (keyword (.getSQLState e))
        error-code (or (state *pgsql-error-codes*) :internal-error)
        error-ctx  {:sqlstate state
                    :message (.getMessage e)}]
    (error error-code nil error-ctx)))

(defmacro catch-sqlexception
  "Block style macro that catch sql exceptions and
  translates them to human readable messages."
  [& body]
  `(try
     (do ~@body)
     (catch java.sql.BatchUpdateException e#
       (let [e# (.getNextException e#)]
         (resolve-pgsql-error e#)))
     (catch java.sql.SQLException e#
       (resolve-pgsql-error e#))))

(defmacro wrap
  "Decorator like macro that wraps one unique expression
  in a try/catch block and return left value of either type
  if any exception is raised."
  [expression]
  `(try
     (either/right ~expression)
     (catch java.sql.BatchUpdateException e#
       (let [e# (.getNextException e#)]
         (resolve-pgsql-error e#)))
     (catch java.sql.SQLException e#
       (resolve-pgsql-error e#))))
