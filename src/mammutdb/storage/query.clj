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

(ns mammutdb.storage.query
  (:require [clojure.string :as str]
            [clojure.core.match :refer [match]]
            [schema.core :as s]))

(declare build-where-clause)
(declare build-select-clause)
(declare build-ordering-clause)
(declare parse-fieldname)

(def ^:dynamic *nested-scope* 0)
(def ^:private query-schema
  {:select [s/Keyword]
   :table s/Keyword
   (s/optional-key :where) [s/Any]
   (s/optional-key :limit) s/Int
   (s/optional-key :offset) s/Int
   (s/optional-key :orderby) [s/Keyword]})

(defn- build-query-reducer
  [opts [sql params] key]
  (case key
    :select
    (let [select (:select opts)]
      [(str sql "SELECT " (build-select-clause select)) params])

    :where
    (let [where       (:where opts)
          where       (build-where-clause where)
          whereclause (first where)
          whereparams (rest where)]
      [(str sql " WHERE " whereclause)
       (apply conj params whereparams)])

    :table
    (let [table (name (:table opts))]
      [(str sql " FROM " table) params])

    :orderby
    (let [orderby (:orderby opts)]
      [(str sql " ORDER BY " (build-ordering-clause orderby)) params])

    :limit
    (let [limit (:limit opts)]
      [(str sql " LIMIT ?")
       (conj params limit)])

    :offset
    (let [offset (:offset opts)]
      [(str sql " OFFSET ?")
       (conj params offset)])))

(defn build-query
  [opts]
  {:pre [(s/validate query-schema opts)]}
  (let [orderedopts  [:select :table :where :orderby :limit :offset]
        [sql params] (reduce (partial build-query-reducer opts) ["" []] orderedopts)]
    (apply vector sql params)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Select clause
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn build-select-clause
  [fields]
  (->> (map name fields)
       (interpose ", ")
       (str/join "")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Order By Clause
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn build-ordering-clause
  [fields]
  (->> fields
       (map name)
       (map (fn [field]
              (if (= (subs field 0 1) "-")
                (str (parse-fieldname (subs field 1)) " DESC")
                (str (parse-fieldname field) " ASC"))))
       (str/join ", ")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Where clause
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: validate fieldname string for avoid sql injections

(defn- parse-fieldname
  [fieldname]

  (cond
   (= fieldname "_rev")
   "(revid::text || '-' || revhash)"

   (= (subs fieldname 0 1) "_")
   (subs fieldname 1)

   :else
   (format "data->'%s'" fieldname)))

(defn- build-combined-clause
  [op criterias]

  (let [op      (case op :and "AND" :or "OR")
        filters (mapv build-where-clause criterias)
        clauses (->> (map first filters)
                     ;; (map (partial format "(%s)"))
                     (interpose op)
                     (str/join " "))
        params  (mapcat rest filters)]
    (if (> *nested-scope* 1)
      (apply vector (format "(%s)" clauses) params)
      (apply vector (format "%s" clauses) params))))

(defn- build-simple-clause
  "Build sql where clause for filtering
  by one field with one or combined criteria."
  [[op field value :as criteria]]
  (case op
    :eq  [(format "%s = ?" (parse-fieldname field)) value]
    :gt  [(format "%s > ?" (parse-fieldname field)) value]
    :lt  [(format "%s < ?" (parse-fieldname field)) value]
    :gte [(format "%s >= ?" (parse-fieldname field)) value]
    :lte [(format "%s <= ?" (parse-fieldname field)) value]))

(defn build-where-clause
  [[op & rest :as criteria]]
  (binding [*nested-scope* (inc *nested-scope*)]
    (case op
      :eq  (build-simple-clause criteria)
      :lt  (build-simple-clause criteria)
      :gt  (build-simple-clause criteria)
      :lte (build-simple-clause criteria)
      :gte (build-simple-clause criteria)
      :and (build-combined-clause :and rest)
      :or  (build-combined-clause :or rest))))
