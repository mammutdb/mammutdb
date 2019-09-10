(ns mammutdb.tests.helpers
  (:refer-clojure :exclude [await]))

;; (defn database-reset
;;   [next]
;;   (with-open [conn (db/connection)]
;;     (let [sql (str "SELECT table_name "
;;                    "  FROM information_schema.tables "
;;                    " WHERE table_schema = 'public' "
;;                    "   AND table_name != 'migrations';")
;;           result (->> (sc/fetch conn sql)
;;                       (map :table_name))]
;;       (sc/execute conn (str "TRUNCATE "
;;                             (apply str (interpose ", " result))
;;                             " CASCADE;"))))
;;   (try
;;     (next)
;;     (finally
;;       (st/clear! uxbox.media/media-storage)
;;       (st/clear! uxbox.media/assets-storage))))

(defmacro await
  [expr]
  `(try
     (deref ~expr)
     (catch Exception e#
       (.getCause e#))))
