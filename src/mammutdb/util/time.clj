;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) 2019 Andrey Antukh <niwi@niwi.nz>

(ns mammutdb.util.time
  (:require [cognitect.transit :as t])
  (:import java.time.Instant))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Serialization Layer conversions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(declare from-string)

(def ^:private write-handler
  (t/write-handler
   (constantly "m")
   (fn [v] (str (.toEpochMilli v)))))

(def ^:private read-handler
  (t/read-handler
   (fn [v] (-> (Long/parseLong v)
               (Instant/ofEpochMilli)))))

(def +read-handlers+
  {"m" read-handler})

(def +write-handlers+
  {Instant write-handler})

(defmethod print-method Instant
  [mv ^java.io.Writer writer]
  (.write writer (str "#instant \"" (.toString mv) "\"")))

(defmethod print-dup Instant [o w]
  (print-method o w))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helpers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn from-string
  [s]
  {:pre [(string? s)]}
  (Instant/parse s))

(defn now
  []
  (Instant/now))
