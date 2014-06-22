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

(ns mammutdb.cli
  "Main entry point for command line interface of mammutdb."
  (:require [mammutdb.config :as conf]
            [mammutdb.core.util :refer [exit]]
            [mammutdb.core.barrier :as barrier]
            [mammutdb.storage.migrations :as migrations]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class))

(def ^:private options
   [["-v" nil "Verbosity level"
     :id :verbosity
     :default 0
     :assoc-fn (fn [m k _] (update-in m [k] inc))]
    ["-c" "--config CONFIGFILE"
     :id :config]
    ["-h" "--help"]])

(defn usage
  [summary]
  (str/join \newline ["Options:" summary]))

(defn init
  [path]
  (let [cdl (barrier/count-down-latch 1)
        ret (m/mlet [_ (conf/setup-config path)
                     _ (migrations/bootstrap)]
              (barrier/await cdl)
              (t/right nil))]
    (if (t/right? ret)
      (exit 0, nil)
      (exit 1, (str (t/from-either ret))))))

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args options)]
    ;; TODO: configure logging depending on verbosity
    ;; level selected from command line.
    (cond
     (:help options)    (exit 0 (usage summary))
     (:config options)  (exit 0 (init (:config options)))
     :else              (exit 1 (usage summary)))))
