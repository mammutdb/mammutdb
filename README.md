# mammutdb

[![Travis Badge](https://img.shields.io/travis/mammutdb/mammutdb.svg?style=flat)](https://travis-ci.org/mammutdb/mammutdb "Travis Badge")

Fully transactional and immutable opensource database.

## Project status

Currently on design phases.

## Features

- Strong ACID transactionality.
- Immutable data.
- Transaction log and full history navigability.
- Lightweight schema support for data validation.
- Database/Collection/Document logical storage separation
- PostgreSQL as storage engine.
- Different types of documents/collections: json, binary blob, edn?
- Optional OCC (optimistic concurrency control).
- Distributed cache.

NOTE: this is a full features list, to know the state of each item,
see the changelog or below development phases state.

## Development phases

### Zero phase (meta) ###

- [x] Monadic storage api.
- [x] Pluggable transport interface.
- [x] Configuration management.
- [x] Storage layout migrations.
- [x] Http Api layout and dynamic loading through configuration

### First phase ###

- [ ] Basic crud for Documents/Collections/Databases
- [ ] Full transactionality support on high level database api.
- [ ] Transaction log.
- [ ] Navigability through transaction-log/revisions of one document.
- [ ] Optionally optimistic concurrency control for collections.
- [ ] Basic query/filtering support for Collections and Databases.
- [ ] Optional shema support for compatible collections.

### Second phase ###

- [ ] Basic indexes support.
- [ ] Rich collection filtering.
- [ ] Clojure driver/bindings.
- [ ] Python3 driver/bindings.

### Third phase ###

- [ ] Rich query language.
- [ ] Joins between collections.
- [ ] Binary collection/document type.
- [ ] Cache objects.
- [ ] Distributed cache.
- [ ] Async events (new revision, new transaction)

### Future ideas ###

- WebSockets/ServerSentEvents api for events notifications
- WebHooks
- Support full text search indexes and searches.
- Fast collections.
- Haskell driver/bindings.
- Golang driver/bindings.

## Instalation notes
You should configure your database connection with the file: testconfig.edn

Current implementation requires "UUID OSSP" extension. Should be enabled with:

```sql
CREATE EXTENSION "uuid-ossp";
```

## Run standalone process

```bash
lein with-profile uberjar run -c tests/tesconfig.edn
```
