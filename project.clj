;; Copyright (c) 2014 Andrey Antukh <niwi@niwi.be>
;;
;; All rights reserved.
;;
;; Redistribution and use in source and binary forms, with or without
;; modification, are permitted provided that the following conditions
;; are met:
;; 1. Redistributions of source code must retain the above copyright
;;    notice, this list of conditions and the following disclaimer.
;; 2. Redistributions in binary form must reproduce the above copyright
;;    notice, this list of conditions and the following disclaimer in the
;;    documentation and/or other materials provided with the distribution.
;; 3. The name of the author may not be used to endorse or promote products
;;    derived from this software without specific prior written permission.
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
  :license {:name "BSD"
            :url "http://opensource.org/licenses/BSD-3-Clause"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.stuartsierra/component "0.2.1"]
                 [org.clojure/tools.reader "0.8.4"]
                 [jarohen/nomad "0.6.3" :exclusions [org.clojure/tools.reader]]
                 [compojure "1.1.6"]
                 [info.sunng/ring-jetty9-adapter "0.6.0" :exclusions [ring/ring-core]]
                 [org.clojure/tools.namespace "0.2.4"]
                 [ring/ring-core "1.2.2" :exclusions [javax.servlet/servlet-api]]
                 [ring/ring-servlet "1.2.2" :exclusions [javax.servlet/servlet-api]]
                 [org.clojure/algo.monads "0.1.5"]
                 [org.clojure/core.match "0.2.1"]
                 [cheshire "5.3.1"]
                 [postgresql "9.3-1101.jdbc41"]
                 [buddy "0.1.1"]
                 [be.niwi/clj.jdbc "0.1.1"]
                 [be.niwi/clj.jdbc-dbcp "0.1.1"]]
  :main ^:skip-aot mammutdb.cli
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
