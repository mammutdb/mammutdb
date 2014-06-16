(ns mammutdb.cli
  "Main entry point for command line interface
  of mammutdb."
  (:require [mammutdb.config :as conf]
            [mammutdb.storage.migrations :as migrations]
            [cats.types :as t]
            [cats.core :as m])
  (:gen-class))

(defn -main
  [configpath]
  (alter-var-root #'conf/*config-path* (fn [_] configpath))
  (migrations/bootstrap))
