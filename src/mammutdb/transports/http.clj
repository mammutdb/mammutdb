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

(ns mammutdb.transports.http
  (:require [compojure.handler :refer [api]]
            [ring.middleware.json :refer [wrap-json-response wrap-json-body]]
            [ring.adapter.jetty9 :refer [run-jetty]]
            [com.stuartsierra.component :as component]
            [mammutdb.logging :refer [log]]
            [mammutdb.core.barrier :as barrier]
            [mammutdb.transports.http.routes :refer [main-routes]]))

;; TODO: reimplement jetty logger and make it logging into
;; mammutdb logger.
;; Links:
;; - http://stackoverflow.com/questions/2120370/jetty-how-to-disable-logging
;; - http://download.eclipse.org/jetty/stable-9/apidocs/org/eclipse/jetty/util/log/Logger.html

(defrecord HttpTransport [options stopfn]
  component/Lifecycle
  (start [component]
    (log :info "Starting transport: http")
    (let [app    (-> main-routes
                     (wrap-json-body {:keywords? true :bigdecimals? true})
                     (wrap-json-response {:pretty true})
                     (api))
          opts   (assoc options
                   :daemon? true
                   :join? false)
          stopfn (run-jetty app opts)]
      (assoc component
        :app app
        :stopfn stopfn)))

  (stop [_]
    (log :info "Stoping transport: http")
    (.stop stopfn)))

(defn transport
  [options]
  (map->HttpTransport {:options options}))
