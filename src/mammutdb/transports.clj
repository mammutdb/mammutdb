(ns mammutdb.transports
  "Main interface to transport initialization."
  (:require [clojure.string :as str]
            [mammutdb.core.lifecycle :as lifecycle]
            [mammutdb.core.monads :as m]
            [mammutdb.core.monads.types :as t]))

(defn resolve-fn-by-name
  "Given a name, dynamicaly load function."
  [^String path]
  (let [[nsname fnname] (str/split path #"/")]
    (try
      (load nsname)
      (t/right (ns-resolve (symbol nsname) (symbol fnname)))
      (catch Exception e
        (t/left (str e))))))

(def load-transport
  [conf]
  (if-let [path (:path conf)]
    (m/<*> (resolve-fn-by-name path) conf)
    (t/left "Transport path not defined on configuration")))

(defn initialize-transport
  [conf]
  (m/mlet [transport (load-transport conf)]
    (m/return transport)))
