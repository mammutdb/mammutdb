(ns mammutdb.core.edn
  "Edn reader helpers"
  (:require [clojure.tools.reader.edn :as edn]
            [clojure.java.io :as io]))

(defn from-resource
  "Read edn file from resource."
  [^String path]
  (-> (io/resource path)
      (slurp)
      (edn/read-string)))

