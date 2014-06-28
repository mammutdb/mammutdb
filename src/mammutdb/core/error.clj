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

(ns mammutdb.core.error
  "Error management functions and macros"
  (:require [clojure.java.io :as io]
            [mammutdb.config :as config]
            [mammutdb.core.edn :as edn]
            [cats.types :as t]
            [cats.core :as m]))

(declare error)

(defmacro catch-to-either
  "Block style macro that catch any exception to left value
  of either type."
  [& body]
  `(try
     (do ~@body)
     (catch Exception e#
       (error e#))))

(defmacro wrap-to-either
  "Decorator like macro that wraps one unique expression
  in a try/catch block and return left value of either type
  if any exception is raised."
  [expression]
  `(try
     (t/right ~expression)
     (catch Exception e#
       (error e#))))

(defn exception?
  [e]
  (instance? Throwable e))

(defn error
  [code e]
  (t/left (cond
           (exception? e)
           {:type :exception :value e :code code}

           (string? e)
           {:type :string :value e :code code}

           (keyword? e)
           {:type :keyword :value e :code code})))

