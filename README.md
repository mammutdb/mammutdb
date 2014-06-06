# mammutdb

Fully transactional and immutable opensource database.


## Project status

Currently on design phases.

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

- Advanced query language support.
- Support for joins between collections.
- Support store binary blobs.
- Support full text search indexes and searches.
- Different types of collections: without transactions, without revisions
  (for write high performance collections), without occ.
- Official clojure client library.
- Official haskell client library.
- Official python3 client library.

