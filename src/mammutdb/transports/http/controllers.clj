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
            [cats.monad.maybe :as maybe]
            [cats.monad.either :as either]
            [mammutdb.api :as api]
            [mammutdb.storage :as s]
            [mammutdb.transports.http.response :refer :all]))

(defn home-ctrl
  [request]
  (ok {:server "MammutDB 1.0-SNAPSHOT"
       :apiurls ["/databases"
                 "/databases/:dbname"
                 "/databases/:dbname/:collname"]}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Error Handling
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn left-as-response
  [result]
  (let [value (either/from-either result)
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
  [req]
  (let [mresult (api/get-all-databases)
        result  (either/from-either mresult)]
    (cond
     (either/right? mresult)
     (ok (mapv s/to-plain-object result))

     (either/left? mresult)
     (left-as-response mresult))))

(defn databases-create
  [{:keys [params] :as req}]
  (let [dbname (:dbname params)
        result (api/create-database dbname)]
    (cond
     (either/right? result)
     (created (s/to-plain-object (either/from-either result)))

     (either/left? result)
     (left-as-response result))))

(defn databases-drop
  [{:keys [params] :as req}]
  (let [dbname (:dbname params)
        result (api/drop-database dbname)]
    (cond
     (either/right? result)
     (no-content)

     (either/left? result)
     (left-as-response result))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Collection Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn collection-list
  [{:keys [params] :as req}]
  (let [dbname (:dbname params)
        result (api/get-all-collections dbname)]
    (cond
     (either/right? result)
     (ok (mapv s/to-plain-object (either/from-either result)))

     (either/left? result)
     (left-as-response result))))

(defn collection-create
  [{:keys [params] :as req}]
  (let [dbname   (:dbname params)
        collname (:collname params)
        result   (api/create-collection dbname collname)]
    (cond
     (either/right? result)
     (created (s/to-plain-object (either/from-either result)))

     (either/left? result)
     (left-as-response result))))

(defn collection-drop
  [{:keys [params] :as req}]
  (let [dbname   (:dbname params)
        collname (:collname params)
        result (api/drop-collection dbname collname)]
    (cond
     (either/right? result)
     (no-content)

     (either/left? result)
     (left-as-response result))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Documents Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; (defn document-list
;;   [{:keys [params] :as req}]
;;   (let [dbname (:dbname params)
;;         result (api/get-documents dbname)]
;;     (cond
;;      (either/right? result)
;;      (ok (mapv s/to-plain-object (either/from-either result)))

;;      (either/left? result)
;;      (left-as-response result))))

;; (defn document-detail
;;   [{:keys [params] :as req}]
;;   (let [dbname   (:dbname params)
;;         collname (:collname params)
;;         docid    (:docid params)
;;         result   (api/get-document-by-id dbname collname docid)]
;;     (cond
;;      (either/right? result)
;;      (ok (s/to-plain-object (either/from-either result)))

;;      (either/left? result)
;;      (left-as-response result))))

(defn document-create
  [{:keys [params] :as req}]
  (let [dbname   (:dbname params)
        collname (:collname params)
        body     (slurp (:body req))
        result   (api/persist-document dbname collname body)]
    (cond
     (either/right? result)
     (created (s/to-plain-object (either/from-either result)))

     (either/left? result)
     (left-as-response result))))

;; (defn document-drop
;;   [{:keys [params] :as req}]
;;   (let [dbname   (:dbname params)
;;         collname (:collname params)
;;         result (api/drop-collection dbname collname)]
;;     (cond
;;      (either/right? result)
;;      (no-content)
;;
;;      (either/left? result)
;;      (left-as-response result))))

