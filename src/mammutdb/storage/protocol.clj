(ns mammutdb.storage.protocol
  "Main interface to storage.")

(defprotocol Storage
  (initialized? [self] "Check if storage is initialized")
  (initialize [self] "Initialize storage.")

  (store-object [self collection object] "Store object.")
  ;; (drop-object [self collection object] "Drop object.")

  (get-object-by-id [self collection id] "Get object by its internal id.")
  ;; (get-object-by-field [self colection field value] "Get object by field.")

  ;; (get-object-revisions [self collection] "Get object revisions.")

  (collection-exists? [self collection] "Check if collection exists."))
