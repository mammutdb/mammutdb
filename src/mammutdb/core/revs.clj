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

(ns mammutdb.core.revs
  (:require [cats.types :as t]
            [taoensso.nippy :as nippy]
            [buddy.core.codecs :refer [->byte-array bytes->hex]]
            [buddy.core.hash :as hash]
            [mammutdb.core.errors :as e]))

;; TODO: This is a quick and dirty implementation. It needs a refactor.
;; Considerations:
;; - Use platform agnostic binary serialization or platform specific?
;; - revhash should be ireversible or reversible?
;; - that hash algorithm should be used? sha3? sha2? md5/sha1? other?

(defn make-new-revhash
  "Create new revision."
  [^Boolean deleted ^Long oldrevid ^String oldrevhash body]
  (let [data (nippy/freeze {:deleted deleted
                            :revid oldrevid
                            :revhash (->byte-array oldrevhash)
                            :body (->byte-array body)})]
    (-> (hash/sha3-256 data)
        (bytes->hex))))
