# SQLite storage engine for CouchDB

## Synopsis

A pluggable storage engine for CouchDB based on SQLite. Created as a "dive in" project for a better understanding of the internals of PSE in CouchDB rather than a real production ready engine.

## Try

_TBD: Build a docker image_

## Setup

_TBD: Write about changing Couch's rebar config and adding a section to "couchdb_engines"_

## Dev setup

Clone this repo. Run `make compile`. It'll clone CouchDB master, build it (given all the deps installed), symlink deps and then build the libs.

## Testing

### EUnit

Run `make test`, it'll setup CouchDB testing env, generate engine's tests and run them.

### Manual

_TBD: Write basic CRUD steps for `curl` with `engine` attribute_

## License

[Apache License 2.0](https://raw.githubusercontent.com/eiri/couch_sqlite_engine/master/LICENSE "Apache License 2.0")
