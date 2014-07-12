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

(ns mammutdb.api.database
  (:require [cats.types :as t]
            [cats.core :as m]
            [mammutdb.core.errors :as e]
            [mammutdb.core.safe :refer [check-database-name-safety]]
            [mammutdb.storage :as storage]
            [mammutdb.storage.transaction :as stx]))

(defn get-all-databases
  []
  (->> (fn [conn] (storage/get-all-databases conn))
       (stx/transaction {:readonly true})))

(defn get-database-by-name
  [name]
  (->> (fn [conn]
         (m/>>= (check-database-name-safety name)
                (fn [name] (storage/get-database-by-name name conn))))
       (stx/transaction {:readonly true})))

(defn create-database
  [name]
  (->> (fn [conn]
         (m/>>= (check-database-name-safety name)
                (fn [name] (storage/create-database name conn))))
       (stx/transaction {:readonly false})))

(defn drop-database
  [name]
  (->> (fn [conn]
         (m/>>= (check-database-name-safety name)
                (fn [name] (storage/get-database-by-name name conn))
                (fn [db] (storage/drop-database db conn))))
       (stx/transaction {:readonly false})))
