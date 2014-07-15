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
  (:require [cats.core :as m]
            [cats.monad.either :as either]
            [mammutdb.core.errors :as e]

            [mammutdb.core.safe :refer [check-collection-name-safety
                                        check-database-name-safety]]
            [mammutdb.storage.json :as json]
            [mammutdb.storage :as s]))

(defn- check-document-validity
  "Check if incoming documen json format is
  valid and return the data untouched."
  [data]
  (m/>>= (either/right data)
         (fn [data]
           (if (empty? data)
             (e/error :invalid-json-format)
             (m/return data)))
         (fn [data] (json/decode data))
         (fn [_] (m/return data))))

(defn persist-document
  ([^String db ^String coll data]
     (persist-document db coll data {}))
  ([^String db ^String coll data opts]
     (->> (fn [conn]
            (m/mlet [data   (check-document-validity data)
                     dbname (check-database-name-safety db)
                     name   (check-collection-name-safety coll)
                     db     (s/get-database-by-name dbname conn)
                     coll   (s/get-collection-by-name db coll conn)]
              (s/persist-document coll opts data conn)))
          (s/transaction {:readonly false}))))

(defn get-documents
  [^String db ^String coll]
  (->> (fn [conn]
         (m/mlet [dbname (check-database-name-safety db)
                  name   (check-collection-name-safety coll)
                  db     (s/get-database-by-name dbname conn)
                  coll   (s/get-collection-by-name db coll conn)]
           (s/get-documents coll {} conn)))
       (s/transaction {:readonly true})))

(defn get-document-by-id
  ([^String db ^String coll ^String id]
     (get-document-by-id db coll id {}))
  ([^String db ^String coll ^String id options]
     (->> (fn [conn]
            (m/mlet [dbname (check-database-name-safety db)
                     name   (check-collection-name-safety coll)
                     db     (s/get-database-by-name dbname conn)
                     coll   (s/get-collection-by-name db name conn)]
              (s/get-document-by-id coll id conn)))
          (s/transaction {:readonly true}))))

;; (defn get-document-by-rev
;;   ([^String db ^String coll ^String id]
;;      (get-document-by-id db coll id {}))
;;   ([^String db ^String coll ^String id options]
;;      (->> (fn [conn]
;;             (m/mlet [dbname (check-database-name-safety db)
;;                      name   (check-collection-name-safety coll)
;;                      db     (s/get-database-by-name dbname conn)
;;                      coll   (s/get-collection-by-name db name conn)]
;;               (s/get-document-by-id coll id conn)))
;;           (s/transaction {:readonly true}))))

(defn drop-document-by-id
  [^String db ^String coll ^String id]
  (->> (fn [conn]
       (m/mlet [dbname (check-database-name-safety db)
                name   (check-collection-name-safety coll)
                db     (s/get-database-by-name dbname conn)
                coll   (s/get-collection-by-name db name conn)]
               (s/drop-document-by-id coll id conn)))
     (s/transaction {:readonly true})))
