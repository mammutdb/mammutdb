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
  (:require [clojure.string :as str]))


;; (sdoc/filter-by-date [:or [:gt 2] [:lt 3]])
;; (sdoc/filter-by-date )

(declare make-filter)
(declare make-combinator-filter)

(defn make-combinator-filter
  [fieldname combstr optrest]
  (let [filters (mapv (partial make-filter fieldname) optrest)
        clauses (->> (mapv first filters)
                     (interpose combstr)
                     (str/join " "))
        params  (mapv second filters)]
    (apply vector (format "(%s)" clauses) params)))


(defn make-filter
  [fieldname filterdata]
  {:pre [(vector? filterdata)
         (> (count filterdata) 1)
         (keyword? (first filterdata))]}
  (let [[opt & optrest] filterdata]
    (case opt
      :gt  [(format "(%s > ?)" fieldname)
            (first optrest)]
      :lt  [(format "(%s < ?)" fieldname)
            (first optrest)]
      :gte [(format "(%s >= ?)" fieldname)
            (first optrest)]
      :lte [(format "(%s <= ?)" fieldname)
            (first optrest)]
      :or  (make-combinator-filter fieldname "OR" optrest)
      :and  (make-combinator-filter fieldname "AND" optrest))))
