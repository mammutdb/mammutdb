(ns mammutdb.system
  (:require [com.stuartsierra.component :as component]
            [mammutdb.storage :refer [new-storage]]))

(defn mammutdb-system
  []
  (component/system-map
   :storage (new-storage "postgresql://localhost:5432/test")))
