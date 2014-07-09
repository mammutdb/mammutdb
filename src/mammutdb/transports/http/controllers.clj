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

(ns mammutdb.transports.http.controllers
  (:require [cats.core :as m]
            [cats.types :as t]
            [mammutdb.api.database :as dbapi]
            [mammutdb.transports.http.conversions :as conv]
            [mammutdb.transports.http.protocols :refer [to-plain-object]]
            [mammutdb.transports.http.response :refer :all]))

(defn home-ctrl
  [request]
  (ok {:server "MammutDB 1.0-SNAPSHOT"}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Error Handling
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn left-as-response
  [result]
  (let [value (t/from-either result)
        response {:code (:error-code value)
                  :msg (:error-msg value)
                  :ctx (:error-ctx value)}]
    (case (:http-code value)
      :400 (bad-request response)
      :404 (not-found response)
      (bad-request (assoc response :type :unexpected)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Databases Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn databases-list
  [request]
  (let [mresult (dbapi/get-all)
        result  (t/from-either mresult)]
    (cond
     (t/right? mresult)
     (ok (mapv to-plain-object result))

     (t/left? mresult)
     (left-as-response mresult))))

(defn databases-create
  [{:keys [params] :as req}]
  (let [dbname (:dbname params)
        result (dbapi/create! dbname)]
    (cond
     (t/right? result)
     (created (to-plain-object (t/from-either result)))

     (t/left? result)
     (left-as-response result))))

(defn databases-drop
  [{:keys [params] :as req}]
  (let [dbname (:dbname params)
        result (dbapi/drop! dbname)]
    (cond
     (t/right? result)
     (no-content)

     (t/left? result)
     (left-as-response result))))
