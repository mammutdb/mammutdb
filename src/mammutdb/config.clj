(ns mammutdb.config
  "Main configuration interface."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.reader.edn :as edn]
            [mammutdb.core.monads :as m]
            [mammutdb.core.monads.types :as t]))

;; Dynamic var for configuration file path.
;; It just serves for testing purposes only.
(def ^:dynamic *config-path* nil)

(defn- keywordize [s]
  (-> (str/lower-case s)
      (str/replace "_" "-")
      (keyword)))

(defn- read-system-properties
  "Read system properties and return map."
  []
  (->> (System/getProperties)
       (map (fn [[k v]] [(keywordize k) v]))
       (into {})
       (t/just)))

(defn get-configfile-path
  "Get configuration file path."
  []
  (m/mlet [props (read-system-properties)]
    (if-let [path (:mammutdb.cfg props *config-path*)]
      (t/right path)
      (t/left "Configuration file not specified."))))

(defn- read-config-impl
  "Read config from file and return it."
  [path]
  (if (.exists (io/as-file path))
    (t/right (edn/read-string (slurp path)))
    (t/left (format "Config file %s not found" path))))

(def read-config (memoize read-config-impl))

(defn read-transport-config
  "Read transport section from config"
  []
  (m/mlet [cfgpath (get-configfile-path)
           cfg     (read-config cfgpath)]
    (if (:transport cfg)
      (m/return (:transport cfg))
      (t/left "No transport configuration found con config file."))))

(defn read-storage-config
  "Read storage section from config"
  []
  (m/mlet [cfgpath (get-configfile-path)
           cfg     (read-config cfgpath)]
    (if (:storage cfg)
      (m/return (:storage cfg))
      (t/left "No storage configuration found con config file."))))

(defn read-secret-key
  []
  (m/mlet [cfgpath (get-configfile-path)
           cfg     (read-secret-key cfgpath)]
    (if (:secret-key cfg)
      (m/return (:secret-key cfg))
      (t/left "No secretkey configured."))))
