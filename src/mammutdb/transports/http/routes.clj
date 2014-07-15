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

(ns mammutdb.transports.http.routes
  (:require [compojure.core :refer :all]
            [compojure.handler :refer [api]]
            [mammutdb.transports.http.controllers :as ctrl]))

(defroutes main-routes
  ;; (ANY "/" [] ctrl/home-ctrl)

  (GET "/" [] ctrl/databases-list)
  (PUT "/:dbname" [] ctrl/databases-create)
  (DELETE "/databases/:dbname" [] ctrl/databases-drop)

  (GET "/:dbname" [] ctrl/collection-list)
  (PUT "/:dbname/:collname" [] ctrl/collection-create)
  (DELETE "/:dbname/:collname" [] ctrl/collection-drop)

  (GET "/:dbname/:collname" [] ctrl/document-list)
  (POST "/:dbname/:collname" [] ctrl/document-create)
  (GET "/:dbname/:collname/:docid" [] ctrl/document-detail)
  (DELETE "/:dbname/:collname/:docid" [] ctrl/document-drop)

  ;; TODO
  ;; (GET "/:dbname/:collname/:docid/revs" [] ctrl/document-revs-list)
  (GET "/:dbname/:collname/:docid/revs/:rev" [] ctrl/document-revs-detail)

)
