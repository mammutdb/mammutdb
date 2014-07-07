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

(ns mammutdb.api.documents
  (:require [cats.types :as t]
            [cats.core :as m]))



;;             [mammutdb.storage.connection :as conn]
;;             [mammutdb.storage.collections :as scoll]
;;             [mammutdb.storage.documents :as sdoc]
;;             [mammutdb.storage.transaction :as stx]
;;             [mammutdb.core.edn :as edn]
;;             [mammutdb.core.uuid :refer [str->muuid]]
;;             [mammutdb.core.error :as err]))

;; (defn get-by-id
;;   "Get document by id."
;;   [^String collection ^String id]
;;   (let [txfn (fn [con]
;;                (m/mlet [id (str->muuid id)
;;                         c  (scoll/get-by-name con collection)
;;                         d  (sdoc/get-by-id con c id)]
;;                  (m/return d)))]
;;     (m/mlet [con (conn/new-connection)
;;              res (stx/run-in-transaction con txfn)
;;              _   (conn/close-connection con)]
;;       (m/return res))))

;; (defn persist
;;   "Persist document."
;;   [^String collection document options]
;;   {:pre [(map? document)]}
;;   (m/mlet [con (conn/new-connection)
;;            c   (scoll/get-by-name con collection)
