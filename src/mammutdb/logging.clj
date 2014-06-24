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

(ns mammutdb.logging
  "Logging implementation for mammutdb."
  (:require [cats.types :as t]
            [clojure.string :as str])
  (:import java.util.logging.Level
           java.util.logging.Logger
           java.util.logging.ConsoleHandler
           java.util.logging.LogRecord
           java.util.logging.Formatter
           java.util.logging.FileHandler
           java.util.logging.SimpleFormatter))


(def ^{:dynamic true
       :doc "Logging write queue"}
  *logger-writer* (agent 0N :error-mode :continue))

(defn- configured?
  [^Logger logger]
  (let [handlers (.getHandlers logger)]
    (= (count handlers) 0)))

(defn- configure-logger!
  [logger]
  (let [formatter (proxy [Formatter] []
                    (format [^LogRecord rec]
                      (str/join " " [(.getLevel rec)
                                     (.getMessage rec)])))
        handler   (doto (ConsoleHandler.)
                    (.setLevel Level/ALL)
                    (.setFormatter formatter))]
    (doto logger
      (.setUseParentHandlers false)
      (.setLevel Level/ALL)
      (.addHandler handler))))

(def ^{:dynamic :true
       :doc "Lazzy logger constructor"}
  *logger*
  (delay (let [logger (Logger/getLogger "mammutdb")]
           (configure-logger! logger)
           logger)))


(defn log
  [level message & params]
  (send-off *logger-writer*
            (fn [v & args]
              (let [message (apply format message params)]
                (case level
                  :info (.info @*logger* message)
                  :error (.log @*logger* Level/SEVERE message)
                  :debug (.fine @*logger* message)
                  :warn (.warning @*logger* message)))

              (inc v)))
  (t/right nil))
