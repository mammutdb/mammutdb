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

(ns mammutdb.storage
  (:require [potemkin.namespaces :refer [import-vars]]
            [mammutdb.storage.protocols :as protocols]
            [mammutdb.storage.user :as user]
            [mammutdb.storage.document :as document]
            [mammutdb.storage.transaction :as transaction]
            [mammutdb.storage.connection :as connection]
            [mammutdb.storage.database :as database]
            [mammutdb.storage.collection :as collection]))

(import-vars
 [protocols

  to-plain-object]

 [document

  document?
  json->document
  persist-document]
  ;; get-document-by-id
  ;; get-document-by-rev]

 [collection

  ->collection
  record->collection

  collection?
  collection-exists?
  get-all-collections
  get-collection-by-name
  create-collection
  drop-collection]

 [database

  ->database
  record->database

  database?
  database-exists?

  get-all-databases
  get-database-by-name
  create-database
  drop-database]

 [connection

  new-connection
  close-connection
  query
  query-first
  execute-prepared!]

 [transaction

  run-in-transaction
  transaction]

 [user

  ->user
  record->user

  user?
  user-exists?

  get-user-by-username
  get-user-by-id

  user-exists?
  create-user
  drop-user])
