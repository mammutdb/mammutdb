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

(ns mammutdb.storage.protocols)

(defprotocol Database
  "Any database representation type
  should implement this protocol."
  (get-database-name [_] "Get database name")
  (get-collections [_ conn] "Get collections."))

(defprotocol DatabaseMember
  (get-database [_] "Get associated database with current instance."))

(defprotocol Collection
  "Any collection representation type
  should implement this protocol."
  (get-collection-name [_] "Get collection name")
  (get-mainstore-tablename [_] "Get main storage tablename for collection")
  (get-revisions-tablename [_] "Get rev storage tablename for collection"))

(defprotocol CollectionStore
  (collection-exists-by-name? [_ name conn] "Check if collection exists.")
  (get-collection-by-name [_ name conn] "Get collection.")
  (->collection [_ name type] "Collection instance constructor.")
  (create-collection! [_ name type conn] "Create collection."))

(defprotocol Document
  "Any document representation type
  should implement this protocol."
  (document->record [doc] "Convert document to record")
  (get-collection [doc] "Get associated collection"))

(defprotocol DocumentStore
  (record->document [_ rec] "Buld document from record")
  (->document [_ id rev data createdat] "Build document from data.")
  (get-by-id [_ id conn] "Get document by id")
  (persist! [_ doc conn] "Persist document"))

(defprotocol Droppable
  "Any thing that can be droppable
  should implement this protocol."
  (drop! [_ conn] "Drop element"))


