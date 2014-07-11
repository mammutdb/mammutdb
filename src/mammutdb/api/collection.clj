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

(ns mammutdb.api.collection
  (:require [cats.types :as t]
            [cats.core :as m]
            [mammutdb.core.errors :as e]
            [mammutdb.storage.connection :as sconn]
            [mammutdb.storage.transaction :as stx]
            [mammutdb.storage.database :as sdb]
            [mammutdb.storage.collection :as scoll]))

(defn get-all-collections
  [^String dbname]
  (->> (fn [conn]
         (m/>>= (sdb/get-by-name dbname conn)
                (fn [db] (scoll/get-all db conn))))
       (stx/transaction {:readonly true})))

(defn get-collection-by-name
  [^String dbname ^String name]
  (->> (fn [conn]
         (m/>>= (sdb/get-by-name dbname conn)
                (fn [db] (scoll/get-by-name db name conn))))
       (stx/transaction {:readonly true})))

(defn create-collection
  [^String dbname ^String name]
  (->> (fn [conn]
         (m/>>= (sdb/get-by-name dbname conn)
                (fn [db] (scoll/create! db name :json conn))))
       (stx/transaction {:readonly false})))

(defn drop-collection
  [^String dbname ^String name]
  (->> (fn [conn]
         (m/>>= (sdb/get-by-name dbname conn)
                (fn [db] (scoll/get-by-name db name conn))
                (fn [coll] (scoll/drop! coll conn))))
       (stx/transaction {:readonly false})))
