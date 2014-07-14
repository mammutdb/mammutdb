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

(ns mammutdb.cli
  "Main entry point for command line interface of mammutdb."
  (:require [mammutdb.core.util :refer [exit]]
            [mammutdb.core.barrier :as barrier]
            [mammutdb.storage.migrations :refer [migrations]]
            [mammutdb.transports :refer [transport]]
            [mammutdb.config :refer [configuration]]
            [com.stuartsierra.component :as component]
            [cats.core :as m]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class))

(defn system
  "System constructor."
  [confpath]
  (component/system-map
   :configuration (configuration confpath)
   :migrations    (component/using (migrations) [:configuration])
   :transport     (component/using (transport) [:configuration])))

(def ^:private options
   [["-v" nil "Verbosity level"
     :id :verbosity
     :default 0
     :assoc-fn (fn [m k _] (update-in m [k] inc))]
    ["-c" "--config CONFIGFILE"
     :id :config]
    ["-h" "--help"]])

(defn usage
  [summary]
  (str/join \newline ["Options:" summary]))

(defn initialize
  [confpath]
  (let [lock (barrier/count-down-latch 1)]
    (System/setProperty "org.eclipse.jetty.LEVEL" "INFO")
    (component/start (system confpath))
    (barrier/wait lock)))

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args options)]
    ;; TODO: configure logging depending on verbosity
    ;; level selected from command line.
    (cond
     (:help options)
     (do
       (usage summary)
       (exit 0))

     (:config options)
     (let [confpath (:config options)]
       (initialize confpath)
       (exit 0))

     :else
     (do
       (usage summary)
       (exit 0)))))
