(ns mammutdb.test-storage
  (:require [clojure.test :refer :all]
            [cats.types :as t]
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
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :database-does-not-exist)))))

  (testing "Create/Delete database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr (s/create-database "testdb" conn)
            r  (t/from-either mr)]
        (is (t/right? mr))
        (is (= r (s/->database "testdb"))))

      (let [mr (s/database-exists? "testdb" conn)
            r  (t/from-either mr)]
        (is (t/right? mr))
        (is r))

      (s/drop-database (s/->database "testdb") conn)))

  (testing "List created databases"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr1 (s/create-database "testdb1" conn)
            mr2 (s/create-database "testdb2" conn)
            mr3 (s/get-all-databases conn)
            r   (t/from-either mr3)]
        (is (t/right? mr3))
        (is (vector? r))
        (is (= (count r) 2))
        (is (s/database? (first r)))

        (s/drop-database (t/from-either mr1) conn)
        (s/drop-database (t/from-either mr2) conn))))

  (testing "Create duplicate database"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [mr1 (s/create-database "testdb" conn)
            mr2 (s/create-database "testdb" conn)
            r   (t/from-either mr2)]
        (is (t/right? mr1))
        (is (t/left? mr2))
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
            r  (t/from-either mr)]
        (is (t/left? mr))
        (is (= (:error-code r) :collection-does-not-exist)))))

  (testing "Create/Delete collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (s/->database "testdb")]
        (let [mr (s/create-collection db "testcoll" :json conn)
              r  (t/from-either mr)]
          (is (t/right? mr))
          (is (= r (s/->collection db "testcoll" :json))))
        (let [mr (s/collection-exists? db "testcoll" conn)
              r  (t/from-either mr)]
          (is (t/right? mr))
          (is r))

        (s/drop-collection (s/->collection db "testcoll" :json) conn))))

  (testing "Created duplicate collection"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db (s/->database "testdb")]
        (let [mr1 (s/create-collection db "testcoll" :json conn)
              mr2 (s/create-collection db "testcoll" :json conn)
              r   (t/from-either mr2)]
          (is (t/right? mr1))
          (is (t/left? mr2))
          (is (= (:error-code r) :collection-exists))
          (is (= (-> r :error-ctx :sqlstate) :42P07)))

        (s/drop-collection (s/->collection db "testcoll" :json) conn))))
)

(deftest documents
  (config/setup-config! "test/testconfig.edn")
  (migrations/bootstrap)

  (with-open [conn (j/make-connection @sconn/datasource)]
    (let [db   (s/->database "testdb")
          coll (t/from-either (s/create-collection db "testcoll" :json conn))]

      (testing "Simple json to document parsing"
        (let [mdoc (m/>>= (json/encode {:name "foo"})
                          (partial s/json->document coll))
              doc  (t/from-either mdoc)]
          (is (s/document? doc))
          (is (nil? (.-id doc)))
          (is (nil? (.-revid doc)))
          (is (nil? (.-revhash doc)))))

      (testing "Simple json to document parsing with opts"
        (let [mdoc (m/>>= (json/encode {:name "foo"})
                          (partial s/json->document coll {:_id 1}))
              doc  (t/from-either mdoc)]
          (is (s/document? doc))
          (is (= (.-id doc) 1))
          (is (nil? (.-revid doc)))
          (is (nil? (.-revhash doc)))))

      (testing "Simple json to document parsing with opts and preference check"
        (let [mdoc (m/>>= (json/encode {:name "foo" :_id 2})
                          (partial s/json->document coll {:_id 1}))
              doc  (t/from-either mdoc)]
          (is (s/document? doc))
          (is (= (.-id doc) 1))
          (is (nil? (.-revid doc)))
          (is (nil? (.-revhash doc)))))

      (s/drop-collection coll conn)))

  (testing "Document storage api revs semantics through doc"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db   (s/->database "testdb")
            coll (t/from-either (s/create-collection db "testcoll" :json conn))]

        (let [docdata (-> (json/encode {:name "foo"})
                          (t/from-either))
              mdoc    (s/persist-document coll docdata conn)
              doc     (t/from-either mdoc)]
          (is (t/right? mdoc))
          (is (= (.-revid doc) 1))
          (is (= (.-revhash doc) (str "cc56548b1eddfa72a44f9e19ee92edd7"
                                      "03f77583afef68a96b84dfa441ba4bb0")))
          (is (= (.-data doc) {:name "foo"}))

          (let [docdata (-> (s/to-plain-object doc)
                            (assoc :name "bar")
                            (json/encode)
                            (t/from-either))
                mdoc    (s/persist-document coll docdata conn)
                doc     (t/from-either mdoc)]

            (is (t/right? mdoc))
            (is (= (.-revid doc) 2))
            (is (= (.-revhash doc) (str "8ac3ef47802c06fe31c24670318883d4"
                                        "1bc881e8ecc01ab87309ce4d921078db")))
            (is (= (.-data doc) {:name "bar"}))))

        (s/drop-collection coll conn))))

  (testing "Document storage api revs semantics through opts"
    (with-open [conn (j/make-connection @sconn/datasource)]
      (let [db   (s/->database "testdb")
            coll (t/from-either (s/create-collection db "testcoll" :json conn))]

        (let [docdata (-> (json/encode {:name "foo"})
                          (t/from-either))
              mdoc    (s/persist-document coll {:id "fookey"} docdata conn)
              doc     (t/from-either mdoc)]
          (is (t/right? mdoc))
          (is (= (.-revid doc) 1))
          (is (= (.-revhash doc) (str "cc56548b1eddfa72a44f9e19ee92edd7"
                                      "03f77583afef68a96b84dfa441ba4bb0")))
          (is (= (.-data doc) {:name "foo"}))

          (let [docdata (-> (s/to-plain-object doc)
                            (assoc :name "bar")
                            (json/encode)
                            (t/from-either))
                mdoc    (s/persist-document coll docdata conn)
                doc     (t/from-either mdoc)]

            (is (t/right? mdoc))
            (is (= (.-revid doc) 2))
            (is (= (.-revhash doc) (str "8ac3ef47802c06fe31c24670318883d4"
                                        "1bc881e8ecc01ab87309ce4d921078db")))
            (is (= (.-data doc) {:name "bar"}))))

        (s/drop-collection coll conn))))
)
