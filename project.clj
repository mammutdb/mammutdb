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

(defproject mammutdb "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "BSD (2 Clause)"
            :url "http://opensource.org/licenses/BSD-2-Clause"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.reader "0.8.4"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.namespace "0.2.4"]
                 [org.clojure/core.match "0.2.1"]
                 [cheshire "5.3.1"]
                 [buddy "0.2.0b1"]
                 [cats "0.1.0-SNAPSHOT" :exclusions [org.clojure/clojure]]
                 [potemkin "0.3.4"]
                 [prismatic/schema "0.2.4"]
                 [com.stuartsierra/component "0.2.1"]

                 ;; [org.apache.logging.log4j/log4j-core "2.0-rc1"]
                 ;; [org.apache.logging.log4j/log4j-api "2.0-rc1"]

                 ;; Storage
                 [clojure.jdbc "0.2.1"]
                 [clojure.jdbc/clojure.jdbc-dbcp "0.2.0"]
                 [postgresql "9.3-1101.jdbc41"]
                 [stch-library/sql "0.1.1"]

                 ;; Http Api interface
                 [compojure "1.1.8"]

                 [metosin/ring-http-response "0.4.0"]
                 [info.sunng/ring-jetty9-adapter "0.6.0" :exclusions [ring/ring-core]]
                 [ring/ring-ssl "0.2.1" :exclusions [ring/ring-core]]
                 [ring/ring-json "0.3.1" :exclusions [ring/ring-core]]
                 [ring/ring-core "1.2.2" :exclusions [javax.servlet/servlet-api]]
                 [ring/ring-servlet "1.2.2" :exclusions [javax.servlet/servlet-api]]]
  ;; :aot :all
  ;; :main ^:skip-aot mammutdb.cli
  :target-path "target/%s"
  :injections [(require '[cats.types :as t])
               (require '[cats.core :as m])])
