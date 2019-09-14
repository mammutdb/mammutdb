;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) 2019 Andrey Antukh <niwi@niwi.nz>

(ns mammutdb.txlog
  "A transaction log public api."
  (:require
   [promesa.core :as p]
   [clojure.spec.alpha :as s]
   [mammutdb.txlog.psql :as psql]
   [mammutdb.txlog.proto :as pt]
   [mammutdb.util.transit :as t]))

;; --- Producer Constructor

(defmulti producer ::type)

(defmethod producer ::psql
  [opts]
  (psql/producer opts))

(defmethod producer :default
  [opts]
  (throw (ex-info "Invalid producer type" {:type (::type opts)})))

;; --- Consumer Constructor

(defmulti consumer ::type)

(defmethod consumer ::psql
  [opts]
  (psql/consumer opts))

(defmethod consumer :default
  [opts]
  (throw (ex-info "Invalid consumer type" {:type (::type opts)})))


;; --- Transactor API

(s/def ::id any?)
(s/def ::submit-data (s/map-of keyword? any?))
(s/def ::producer #(satisfies? pt/Producer %))
(s/def ::consumer #(satisfies? pt/Consumer %))
(s/def ::offset int?)
(s/def ::batch (s/and int? pos?))
(s/def ::poll-opts
  (s/keys :opt-un [::offset ::batch]))

(defn submit!
  [producer data]
  (s/assert ::producer producer)
  (s/assert ::submit-data data)
  (let [^bytes data (t/encode txop {:type :json-verbose})]
    (pt/submit! producer data)))

(defn poll
  [consumer opts]
  (s/assert ::consumer consumer)
  (s/assert ::poll-opts opts)
  (->> (pt/poll consumer opts)
       (p/map (fn [rows]
                (mapv (fn [[offset data created-at]]
                        {:offset offset
                         :time created-at
                         :data (t/decode data)})
                      rows)))))


