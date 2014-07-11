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
            [mammutdb.storage.connection :as sconn]
            [mammutdb.storage.transaction :as stx]
            [mammutdb.storage.database :as sdb]))

(defn get-all-databases
  []
  (->> (fn [conn] (sdb/get-all conn))
       (stx/transaction {:readonly true})))

(defn get-db-by-name
  [name]
  (->> (fn [conn]
         (m/>>= (check-database-name-safety name)
                (fn [name] (sdb/get-by-name name conn))))
       (stx/transaction {:readonly true})))

(defn create-db
  [name]
  (->> (fn [conn]
         (m/>>= (check-database-name-safety name)
                (fn [name] (sdb/create! name conn))))
       (stx/transaction {:readonly false})))

(defn drop-db
  [name]
  (->> (fn [conn]
         (m/>>= (check-database-name-safety name)
                (fn [name] (sdb/get-by-name name conn))
                (fn [db] (sdb/drop! db conn))))
       (stx/transaction {:readonly false})))
