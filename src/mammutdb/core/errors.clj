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

(ns mammutdb.core.errors
  "Generic mammutdb error codes definition."
  (:require [cats.monad.either :as either]))

;; TODO: remove http code from errors. Is responsibility
;; of transport protocol define them for each error type.

(def ^:dynamic
  *mammutdb-error-codes*
  {:collection-exists
   {:msg "Collection already exists"
    :http-code :400}

   :collection-does-not-exist
   {:msg "Collection does not exist"
    :http-code :404}

   :database-does-not-exist
   {:msg "Database does not exist"
    :http-code :404}

   :database-exists
   {:msg "Database already exists"
    :http-code :400}

   :database-name-unsafe
   {:msg "Database name is unsafe"
    :http-code :400}

   :user-does-not-exist
   {:msg "User does not exist"
    :http-code :404}

   :wrong-authentication
   {:msg "Wrong credentials"
    :http-code :400}

   :internal-error
   {:msg "Internal error"
    :http-code :500}

   :unexpected
   {:msg "Unexpected error"
    :http-code :500}

   :invalid-json-format
   {:msg "Invalid json format."
    :http-code :400}

   :collection-name-unsafe
   {:msg "Collection name is unsafe"
    :http-code :400}

   :invalid-rev-format
   {:msg "rev field has invalid format"
    :http-code :400}

   :serialization-failure
   {:msg "Transaction serialization failed"
    :http-code 500}})

(defn error
  [code & [msg ctx]]
  (if-let [errdata (code *mammutdb-error-codes*)]
    (either/left {:error-code code
                  :error-ctx ctx
                  :http-code (:http-code errdata)
                  :error-msg (or msg (:msg errdata))})
    (either/left {:error-code :unexpected
                  :error-ctx ctx
                  :error-msg (format "Error code '%s' not defined" code)})))
