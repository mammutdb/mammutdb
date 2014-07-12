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

(ns mammutdb.storage.connection
  (:require [jdbc.core :as j]
            [jdbc.pool.dbcp :as pool]
            [cats.core :as m]
            [cats.types :as t]
            [mammutdb.config :as config]
            [mammutdb.storage.errors :as serr]))

(def ^:dynamic
  datasource (delay (let [conf        @config/*config*
                          storageconf (:storage conf)
                          dbspec {:subprotocol "postgresql"
                                  :subname (format "//%s:%s/%s"
                                                   (:host storageconf)
                                                   (:port storageconf)
                                                   (:dbname storageconf))
                                  :user (:user storageconf)
                                  :password (:password storageconf)}]
                      (println (str "Connecting... " (:subname dbspec)))
                      (pool/make-datasource-spec dbspec))))

(defn new-connection
  "Monadic function for create new connection."
  []
  (serr/catch-sqlexception
   (t/right (j/make-connection @datasource))))

(defn close-connection
  "Monadic close connection function."
  [con]
  (serr/catch-sqlexception
   (.close con)
   (t/right true)))

(defn query
  [con sql]
  (serr/wrap (j/query con sql)))

(defn query-first
  [con sql]
  (serr/wrap (j/query-first con sql)))

(defn execute-prepared!
  ([conn sql]
     (serr/wrap (j/execute-prepared! conn sql)))
  ([conn sql opts]
     (serr/wrap (j/execute-prepared! conn sql opts))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; State Monad Jdbc operatiosn (not used at this momment)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; (defn execute
;;   [sql]
;;   (-> (fn [con]
;;         (j/execute! con sql)
;;         (t/pair nil con))
;;       (t/state-t)))

;; (defn execute-prepared
;;   [sql]
;;   (-> (fn [con]
;;         (j/execute-prepared! con sql)
;;         (t/pair nil con))
;;       (t/state-t)))

;; (defn query
;;   [sql]
;;   (-> (fn [con]
;;         (let [res (j/query con sql)]
;;           (t/pair res con)))
;;       (t/state-t)))

;; (defn query-first
;;   [sql]
;;   (-> (fn [con]
;;         (let [r (m/eval-state (query sql) con)]
;;           (t/pair r con)))
;;       (t/state-t)))
