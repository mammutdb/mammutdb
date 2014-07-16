(ns mammutdb.test-storage
  (:require [clojure.test :refer :all]
            [cats.monad.either :as either]
            [cats.core :as m]
            [jdbc.core :as j]
            [clj-time.core :as jt]
            [clj-time.coerce :as jc]
            [mammutdb.storage :as s]
            [mammutdb.storage.json :as json]
            [mammutdb.storage.connection :as sconn]
            [mammutdb.storage.migrations :as migrations]
            [mammutdb.config :as config]))

(deftest databases
  ;; Setup
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Not existence of database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr (s/database-exists? "notexistsdb" conn)
            r  (either/from-either mr)]
        (is (either/left? mr))
        (is (= (:error-code r) :database-does-not-exist)))))

  (testing "Create/Delete database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr (s/create-database "testdb" conn)
            r  (either/from-either mr)]
        (is (either/right? mr))
        (is (= r (s/->database "testdb"))))

      (let [mr (s/database-exists? "testdb" conn)
            r  (either/from-either mr)]
        (is (either/right? mr))
        (is r))

      (s/drop-database (s/->database "testdb") conn)))

  (testing "List created databases"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr1 (s/create-database "testdb1" conn)
            mr2 (s/create-database "testdb2" conn)
            mr3 (s/get-all-databases conn)
            r   (either/from-either mr3)]
        (is (either/right? mr3))
        (is (vector? r))
        (is (= (count r) 2))
        (is (s/database? (first r)))

        (s/drop-database (either/from-either mr1) conn)
        (s/drop-database (either/from-either mr2) conn))))

  (testing "Create duplicate database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr1 (s/create-database "testdb" conn)
            mr2 (s/create-database "testdb" conn)
            r   (either/from-either mr2)]
        (is (either/right? mr1))
        (is (either/left? mr2))
        (is (= (:error-code r) :database-exists)))

      (s/drop-database (s/->database "testdb") conn)))
)

(deftest collections
  ;; Setup
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (testing "Not existence of one collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (s/->database "testdb")
            mr (s/collection-exists? db "notexistent" conn)
            r  (either/from-either mr)]
        (is (either/left? mr))
        (is (= (:error-code r) :collection-does-not-exist)))))

  (testing "Create/Delete collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (s/->database "testdb")]
        (let [mr (s/create-collection db "testcoll" :json conn)
              r  (either/from-either mr)]
          (is (either/right? mr))
          (is (= r (s/->collection db "testcoll" :json))))
        (let [mr (s/collection-exists? db "testcoll" conn)
              r  (either/from-either mr)]
          (is (either/right? mr))
          (is r))

        (s/drop-collection (s/->collection db "testcoll" :json) conn))))

  (testing "Created duplicate collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (s/->database "testdb")]
        (let [mr1 (s/create-collection db "testcoll" :json conn)
              mr2 (s/create-collection db "testcoll" :json conn)
              r   (either/from-either mr2)]
          (is (either/right? mr1))
          (is (either/left? mr2))
          (is (= (:error-code r) :collection-exists))
          (is (= (-> r :error-ctx :sqlstate) :42P07)))

        (s/drop-collection (s/->collection db "testcoll" :json) conn))))
)

(deftest documents
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (with-open [conn (j/make-connection @sconn/datasource)]
    (let [db   (s/->database "testdb")
          coll (either/from-either (s/create-collection db "testcoll" :json conn))]

      (testing "Simple json to document parsing"
        (let [mdoc (m/>>= (json/encode {:name "foo"})
                          (partial s/json->document coll))
              doc  (either/from-either mdoc)]
          (is (s/document? doc))
          (is (nil? (.-id doc)))
          (is (nil? (.-revid doc)))
          (is (nil? (.-revhash doc)))))

      (testing "Simple json to document parsing with opts"
        (let [mdoc (m/>>= (json/encode {:name "foo"})
                          (partial s/json->document coll {:_id 1}))
              doc  (either/from-either mdoc)]
          (is (s/document? doc))
          (is (= (.-id doc) 1))
          (is (nil? (.-revid doc)))
          (is (nil? (.-revhash doc)))))

      (testing "Simple json to document parsing with opts and preference check"
        (let [mdoc (m/>>= (json/encode {:name "foo" :_id 2})
                          (partial s/json->document coll {:_id 1}))
              doc  (either/from-either mdoc)]
          (is (s/document? doc))
          (is (= (.-id doc) 1))
          (is (nil? (.-revid doc)))
          (is (nil? (.-revhash doc)))))

      (s/drop-collection coll conn)))

  (testing "Document storage api revs semantics through doc"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db   (s/->database "testdb")
            coll (either/from-either (s/create-collection db "testcoll" :json conn))]

        (let [docdata (-> (json/encode {:name "foo"})
                          (either/from-either))
              mdoc    (s/persist-document coll docdata conn)
              doc     (either/from-either mdoc)]
          (is (either/right? mdoc))
          (is (= (.-revid doc) 1))
          (is (= (.-revhash doc) (str "cc56548b1eddfa72a44f9e19ee92edd7"
                                      "03f77583afef68a96b84dfa441ba4bb0")))
          (is (= (.-data doc) {:name "foo"}))

          (let [docdata (-> (s/to-plain-object doc)
                            (assoc :name "bar")
                            (json/encode)
                            (either/from-either))
                mdoc    (s/persist-document coll docdata conn)
                doc     (either/from-either mdoc)]

            (is (either/right? mdoc))
            (is (= (.-revid doc) 2))
            (is (= (.-revhash doc) (str "8ac3ef47802c06fe31c24670318883d4"
                                        "1bc881e8ecc01ab87309ce4d921078db")))
            (is (= (.-data doc) {:name "bar"}))))

        (s/drop-collection coll conn))))

  (testing "Document storage api revs semantics through opts"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db   (s/->database "testdb")
            coll (either/from-either (s/create-collection db "testcoll" :json conn))]

        (let [docdata (-> (json/encode {:name "foo"})
                          (either/from-either))
              mdoc    (s/persist-document coll {:id "fookey"} docdata conn)
              doc     (either/from-either mdoc)]
          (is (either/right? mdoc))
          (is (= (.-revid doc) 1))
          (is (= (.-revhash doc) (str "cc56548b1eddfa72a44f9e19ee92edd7"
                                      "03f77583afef68a96b84dfa441ba4bb0")))
          (is (= (.-data doc) {:name "foo"}))

          (let [docdata (-> (s/to-plain-object doc)
                            (assoc :name "bar")
                            (json/encode)
                            (either/from-either))
                mdoc    (s/persist-document coll docdata conn)
                doc     (either/from-either mdoc)]

            (is (either/right? mdoc))
            (is (= (.-revid doc) 2))
            (is (= (.-revhash doc) (str "8ac3ef47802c06fe31c24670318883d4"
                                        "1bc881e8ecc01ab87309ce4d921078db")))
            (is (= (.-data doc) {:name "bar"}))))

        (s/drop-collection coll conn))))

  (testing "The JSON documents can be dropped"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db   (s/->database "testdb")
            coll (either/from-either (s/create-collection db "testcoll" :json conn))]

        (let [docdata (-> (json/encode {:name "foo"})
                          (either/from-either))
              mdoc    (s/persist-document coll docdata conn)
              doc     (either/from-either mdoc)]
          (is (either/right? mdoc))
          (is (= (.-revid doc) 1))

          (either/right? (s/get-document-by-id coll (.-id doc) conn))
          (is (= doc
                 (either/from-either (s/get-document-by-id coll (.-id doc) conn))))

          (is (either/right? (s/drop-document doc conn)))
          (is (either/left? (s/get-document-by-id coll (.-id doc) conn))))

        (s/drop-collection coll conn)
        (s/drop-database db conn))))

  (testing "The JSON documents can be dropped given their id"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db   (s/->database "testdb")
            coll (either/from-either (s/create-collection db "testcoll" :json conn))]

        (let [docdata (-> (json/encode {:name "foo"})
                          (either/from-either))
              mdoc    (s/persist-document coll docdata conn)
              doc     (either/from-either mdoc)]
          (is (either/right? mdoc))
          (is (= (.-revid doc) 1))

          (either/right? (s/get-document-by-id coll (.-id doc) conn))
          (is (= doc
                 (either/from-either (s/get-document-by-id coll (.-id doc) conn))))

          (is (either/right? (s/drop-document-by-id coll (.-id doc) conn)))
          (is (either/left? (s/get-document-by-id coll (.-id doc) conn))))

        (s/drop-collection coll conn)
        (s/drop-database db conn))))

  (testing "The JSON documents can be queried in any point in time"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db   (s/->database "testdb")
            coll (either/from-either (s/create-collection db "testcoll" :json conn))]

        (let [docdata  (-> (json/encode {:name "foo"})
                          (either/from-either))
              mdoc     (s/persist-document coll docdata conn)
              doc      (either/from-either mdoc)
              docdata2 (-> (s/to-plain-object doc)
                          (assoc :name "bar")
                          (json/encode)
                          (either/from-either))
              mdoc2    (s/persist-document coll docdata2 conn)
              doc2     (either/from-either mdoc2)]

          (is (either/right? mdoc))
          (is (= (.-revid doc) 1))
          (is (either/right? mdoc2))
          (is (= (.-revid doc2) 2))

          (let [qmdoc  (s/get-document-by-rev coll (.-id doc) (s/rev doc) conn)
                qmdoc2 (s/get-document-by-rev coll (.-id doc2) (s/rev doc2) conn)]
            (is (either/right? qmdoc))
            (is (either/right? qmdoc2))
            (is (= doc (either/from-either qmdoc)))
            (is (= doc2 (either/from-either qmdoc2))))

        (s/drop-collection coll conn)
        (s/drop-database db conn)))))

  (testing "Document storage api revs semantics through doc"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db   (s/->database "testdb")
            coll (either/from-either (s/create-collection db "testcoll" :json conn))]

        (let [docdata (-> (json/encode {:name "foo"})
                           (either/from-either))
              mdoc    (s/persist-document coll docdata conn)
              doc     (either/from-either mdoc)
              mrevs   (s/get-revisions-of coll (.-id doc) conn)
              revs    (either/from-either mrevs)]
          (is (either/right? mrevs))
          (is (= (count revs) 1))

          (let [docdata (-> (s/to-plain-object doc)
                            (assoc :name "bar")
                            (json/encode)
                            (either/from-either))
                mdoc    (s/persist-document coll docdata conn)
                doc     (either/from-either mdoc)
                mrevs   (s/get-revisions-of coll (.-id doc) conn)
                revs    (either/from-either mrevs)]
            (is (either/right? mrevs))
            (is (= (count revs) 2))
            (is (= (first revs)
                   doc))))

        (s/drop-collection coll conn)))))
