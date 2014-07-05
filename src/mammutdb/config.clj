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

(ns mammutdb.config
  "Main configuration interface."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.reader.edn :as edn]
            [com.stuartsierra.component :as component]
            [mammutdb.core.util :refer [exit]]
            [mammutdb.logging :refer [log]]
            [schema.core :as s]
            [cats.core :as m]
            [cats.types :as t]))

;; Dynamic var for configuration file path.
;; It just serves for testing purposes only.
(def ^:dynamic *config* (atom {}))

(def ^:dynamic *schema*
  {:transport {:path [s/Str]
               :options {s/Any s/Any}}
   :secret-key s/Str
   :storage {:host s/Str
             :port s/Int
             :dbname s/Str}})

(defn validate-config
  [config]
  (try
    (s/validate *schema* config)
    (catch Exception e
      (log :error (.getMessage e))
      (exit 1))))

(defn read-config
  "Read config from file and return it."
  [path]
  (try
    (edn/read-string (slurp path))
    (catch Exception e
      (log :error (format "Config file %s not found" (.toString e)))
      (exit 1))))

(defn setup-config!
  [path]
  (cond
   (string? path)
   (let [f (io/as-file path)]
     (when-not (.exists f)
       (log :error (format "File '%s' not found" path))
       (exit 1))
     (let [cfg (read-config path)]
       (validate-config cfg)
       (reset! *config* cfg)
       cfg))

   (map? path)
   (do
     (validate-config path)
     (reset! *config* path)
     path)))

(defrecord Configuration [path cfg]
  component/Lifecycle
  (start [component]
    (log :info "Starting config module.")
    (let [cfg (setup-config! path)]
      (assoc component :cfg cfg)))

  (stop [_]
    (log :info "Stoping config module.")))

(defn configuration
  [path]
  (map->Configuration {:path path}))

;; (defn- keywordize [s]
;;   (-> (str/lower-case s)
;;       (str/replace "_" "-")
;;       (keyword)))

;; (defn- read-system-properties
;;   "Read system properties and return map."
;;   []
;;   (->> (System/getProperties)
;;        (map (fn [[k v]] [(keywordize k) v]))
;;        (into {})
;;        (t/just)))

;; (defn get-configfile-path
;;   "Get configuration file path."
;;   []
;;   (m/mlet [props (read-system-properties)]
;;     (if-let [path (:mammutdb.cfg props @*config-path*)]
;;       (t/right path)
;;       (t/left "Configuration file not specified."))))

;; (defn read-transport-config
;;   "Read transport section from config"
;;   []
;;   (m/mlet [cfgpath (get-configfile-path)
;;            cfg     (read-config cfgpath)]
;;     (if (:transport cfg)
;;       (m/return (:transport cfg))
;;       (t/left "No transport configuration found con config file."))))

;; (defn read-storage-config
;;   "Read storage section from config"
;;   []
;;   (m/mlet [cfgpath (get-configfile-path)
;;            cfg     (read-config cfgpath)]
;;     (if (:storage cfg)
;;       (m/return (:storage cfg))
;;       (t/left "No storage configuration found con config file."))))

;; (defn read-secret-key
;;   []
;;   (m/mlet [cfgpath (get-configfile-path)
;;            cfg     (read-config cfgpath)]
;;     (if (:secret-key cfg)
;;       (m/return (:secret-key cfg))
;;       (t/left "No secretkey configured."))))
