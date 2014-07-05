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

(ns mammutdb.transports
  "Main interface to transport initialization."
  (:require [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [cats.core :as m]
            [cats.types :as t]
            [mammutdb.logging :refer [log]]
            [mammutdb.core.util :refer [exit]]))

(defn resolve-fn-by-name
  "Given a name, dynamicaly load function."
  [^String path]
  (let [loadpath (first path)
        callable (second path)
        nsname   (str/replace loadpath #"/" ".")]
    (try
      (load loadpath)
      (ns-resolve (symbol nsname) (symbol callable))
      (catch Exception e
        (.printStackTrace e)
        (log :error (format "Canot load transport '%s'" loadpath))
        (exit 1)))))

(defn load-transport
  [path options]
  (let [func (resolve-fn-by-name path)]
    (func options)))

(defrecord Transport [configuration transport]
  component/Lifecycle
  (start [component]
    (log :info "Starting transport")
    (let [path      (get-in configuration [:cfg :transport :path])
          options   (get-in configuration [:cfg :transport :options])
          transport (load-transport path options)]
      (assoc component :transport (component/start transport))))

  (stop [_]
    (component/stop transport)))

(defn transport
  "Transport constructor."
  []
  (map->Transport {}))
