;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) 2019 Andrey Antukh <niwi@niwi.nz>

(ns mammutdb.txlog.psql
  "A postgresql backed transaction log implementation."
  (:require
   [clojure.spec.alpha :as s]
   [promesa.core :as p]
   [mammutdb.util.vertx-pgsql :as pg]
   [mammutdb.txlog.proto :as pt]))

(declare impl-submit)
(declare impl-poll)
(declare ->Producer)
(declare ->Consumer)

;; --- Public API

(s/def ::pool pg/pool?)
(s/def ::pool-opts map?)
(s/def ::uri string?)
(s/def ::schema string?)

(s/def ::producer-params
  (s/keys :opt [::pool ::pool-opts ::uri ::schema]))

(s/def ::consumer-params ::producer-params)

(defn producer
  [{:keys [::pool ::pool-opts ::uri] :as opts}]
  (s/assert ::producer-params opts)
  (cond
    (pg/pool? pool)
    (let [tx (->Producer pool opts false)]
      (pt/init! tx)
      tx)

    (string? uri)
    (let [pool (pg/pool uri pool-opts)
          tx   (->Producer pool opts true)]
      (pt/init! tx)
      tx)

    :else
    (throw (ex-info "producer: invalid arguments" {:opts opts}))))

(defn consumer
  [{:keys [::pool ::pool-opts ::uri] :as opts}]
  (s/assert ::consumer-params opts)
  (cond
    (pg/pool? pool)
    (->Consumer pool opts false)

    (string? uri)
    (let [pool (pg/pool uri pool-opts)]
      (->Consumer pool opts true))

    :else
    (throw (ex-info "consumer: invalid arguments" {:opts opts}))))

;; --- Impl

(deftype Consumer [pool opts close-pool?]
  pt/Consumer
  (poll [_ opts]
    (impl-poll pool opts))

  java.io.Closeable
  (close [_]
    (when close-pool?
      (.close pool))))

(deftype Producer [pool opts close-pool?]
  pt/Producer
  (init! [_]
    (let [schema (::schema opts "public")
          ops  [(str"create schema if not exists " schema)
                (str "create table if not exists " schema ".txlog ("
                     "  id bigserial PRIMARY KEY,"
                     "  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,"
                     "  data jsonb"
                     ")")]]
      @(pg/atomic pool
         (p/run! #(pg/query pool %) ops))))

  (submit! [_ txdata]
    (p/do! (impl-submit pool txdata)))

  java.io.Closeable
  (close [_]
    (when close-pool?
      (.close pool))))

(defn- to-bytes
  [v]
  (cond
    (bytes? v) v
    (string? v) (.getBytes v "UTF-8")
    :else (throw (ex-info "to-bytes: unexpected data" {:v v}))))

(defn- impl-submit
  [pool ^bytes txdata]
  (let [sql "insert into public.txlog (data) values ($1::jsonb) returning id"
        sdata (String. txdata "UTF-8")]
    (p/map first (pg/query-one pool [sql sdata]))))

(defn- impl-poll
  [pool {:keys [offset batch]
         :or {offset 0 batch 10}
         :as opts}]
  (let [sql "select id, data, created_at from txlog where id >= $1 order by id limit $2"]
    (->> (pg/query pool [sql offset batch])
         (p/map (fn [rows]
                  (map (fn [[offset data created-at]]
                         [offset
                          (to-bytes data)
                          (.toInstant created-at)])
                       rows))))))

