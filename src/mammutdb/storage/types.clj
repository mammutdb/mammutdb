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

(ns mammutdb.storage.types
  (:require [clojure.string :as str]
            [mammutdb.storage.protocols :as sproto]))

(deftype Database [name]
  java.lang.Object
  (toString [_]
    (with-out-str
      (print [(str/lower-case name)])))

  (equals [_ other]
    (= name (.-name other))))

(deftype DocumentCollection [database name]
  java.lang.Object
  (toString [_]
    (with-out-str
      (print [(sproto/get-database-name database) name])))

  (equals [_ other]
    (and (= name (.-name other))
         (= database (.database other)))))

;; (deftype JsonDocument [id rev data createdat]
;;   java.lang.Object
;;   (toString [_]
;;     (with-out-str
;;       (print [id rev])))

;;   (equals [_ other]
;;     (and (= id (.-id other))
;;          (= rev (.-rev other)))))

(alter-meta! #'->Database assoc :no-doc true :private true)
(alter-meta! #'->DocumentCollection assoc :no-doc true :private true)
;; (alter-meta! #'->JsonDocument assoc :no-doc true :private true)


(defn ->database
  "Default constructor for database instance."
  [^String name]
  (Database. name))

(defn ->doc-collection
  "Default constructor for document based collection."
  [^Database db ^String name]
  (DocumentCollection. db name))

