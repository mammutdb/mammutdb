;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) 2019 Andrey Antukh <niwi@niwi.nz>

(ns mammutdb.tx
  (:require
   [promesa.core :as p]
   [clojure.spec.alpha :as s]
   [mammutdb.tx.psql :as psql]
   [mammutdb.tx.proto :as pt]
   [mammutdb.util.transit :as t]))

;; --- Transactor Creation

(defmulti transactor ::type)

(defmethod transactor ::psql
  [opts]
  (psql/transactor opts))

(defmethod transactor :default
  [opts]
  (throw (ex-info "Invalid transactor type" {:type (::type opts)})))

;; --- Transactor API

(s/def ::id any?)
(s/def ::doc (s/and (s/map-of keyword? any?)
                    (s/keys :req-un [::id])))
(s/def ::txop
  (s/or :put (s/cat :op #{::put} :doc ::doc)
        :update (s/cat :op #{::update} :doc ::doc)))

(defn submit!
  [tx txop]
  (s/assert ::txop txop)
  (let [^bytes data (t/encode txop {:type :json-verbose})]
    (pt/submit! tx data)))

(defn consumer
  [transactor]
  (pt/consumer transactor {}))

(defn poll
  [consumer opts]
  (->> (pt/poll consumer opts)
       (p/map (fn [rows]
                (mapv (fn [[offset data created-at]]
                        {:offset offset
                         :data (t/decode (.getBytes data "UTF-8"))
                         :tx-time (.toInstant created-at)})
                      rows)))))


