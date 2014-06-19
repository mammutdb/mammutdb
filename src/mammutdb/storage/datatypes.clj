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

(ns mammutdb.storage.datatypes
  "Record definition")

(defrecord User [id username password])
(alter-meta! #'->User assoc :no-doc true :private true)
(alter-meta! #'map->User assoc :no-doc true :private true)

(defn user
  [id username password]
  (User. id username password))

(defn map->user
  [{:keys [id username password]}]
  (user id username password))

(defn user?
  [v]
  (instance? User v))

;; Type that represents a collection
(deftype Collection [name options]
  Object
  (toString [_]
    (with-out-str
      (print [name])))

  (equals [_ other]
    (= name (.-name other))))

(alter-meta! #'->Collection assoc :no-doc true :private true)
(alter-meta! #'map->Collection assoc :no-doc true :private true)

;; Type that represents a document
(deftype Document [id rev data]
  Object
  (toString [_]
    (with-out-str
      (print [id rev])))

  (equals [_ other]
    (and (= id (.-id other))
         (= rev (.-rev other)))))

(alter-meta! #'->Document assoc :no-doc true :private true)
(alter-meta! #'map->Document assoc :no-doc true :private true)

(defn collection
  "Default constructor for collection type."
  [name options]
  (Collection. name options)

(defn document
  "Default constructor for document type."
  [id rev data]
  (Document. id rev data))
