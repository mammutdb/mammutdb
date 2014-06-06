# mammutdb

Fully transactional and immutable opensource database.


## Project status

Currently on design phases.


## Ideas

- Revision system/Transaction log (CouchDB)
- MVCC (PostgreSQL and CouchDB)
- Immutable database (related to transaction log) (Datomic)
- Garbage collector (like postgresql vacuum or couchdb gc) (PostgreSQL and CouchDB)
- Client interface as HTTP (CouchDB)
- Collection/document concept (MongoDB)
- Strong transactions and safe persistence (PostgreSQL)
- Optimistic concurrency control (CouchDB)
- Composable configuration for collection types
- JSON based (CouchDB)
- Pluggable schema definition
- Pluggable client interfaces


## Goals

### Main goals

- Should be written with Clojure.
- Should be written in purely functional way.
- No reinvent: PostgreSQL should be used for storage.

### First phase goals (main)

- Pluggable client interface.
- Initially full stateless HTTP restful client interface.
- Full support for transactions.
- Revision system "a la CouchDB".
- Optimistic concurrency control.
- MongoDB like collections/document concept (obviously without mongodb limits).
- Basic query/filtering support (by id and any json key).
- Basic indexes support for specific document keys.

### Second phase goals

- Pluggable schema definition.
- Advanced query language support.
- Support for joins between collections.
- Support store binary blobs.
- Support full text search indexes and searches.
- Different types of collections: without transactions, without revisions
  (for write high performance collections), without occ.
- Official clojure client library.
- Official haskell client library.
- Official python3 client library.

