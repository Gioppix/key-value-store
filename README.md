# Simple Key Value Persisted Store

## Features

- Insert a key 🔥
- Remove a key 🚀
- Read a key 😳

## Implementation

- Append-only log
- SSTables + In-memory Index + Bloom Filter
- Periodic SSTables compaction and merging
    - Includes Bloom filter rebuilding as they're per sstable
- Tombstone handling
- Multi-thread safety (UNIX only, uses `pwrite`)
- Deferred file deletion

## TODO

- [ ] Improve compaction logic
    - [ ] Add an option to slow down writes when it can't keep up
- [ ] Find faster deserializer
- [ ] Support loading existing database
- [ ] Add tests (!)
- [ ] Fix spaghetti code
- [ ] Add support for string values (easy)
- [ ] Add support for string keys (maybe harder)
