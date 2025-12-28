# Simple Key Value Persisted Store

## Features

- Insert a key
- Remove a key
- Read a key

## Implementation

- Append-only log
- SSTables + Index + Bloom Filter
- Periodic SSTables compaction and merging
    - Since bloom filters are per-sstable this also refreshes them
- Tombstone handling
- Multi-thread safety (UNIX only, uses `pwrite`)
- Deferred file deletion

## TODO

- [ ] Improve compaction logic
- [ ] Find faster deserializer
- [ ] Support loading existing database
- [ ] Add tests and fix spaghetti code
