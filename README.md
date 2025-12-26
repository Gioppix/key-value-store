# Simple Key Value Persisted Store

## Features

- Insert a key
- Remove a key
- Read a key

## Implementation

- Append-only log
- SSTables + Index
- Periodic SSTables compaction and merging
- Tombstone handling
- Multi-thread safety (UNIX only, uses `pwrite`)
- Deferred file deletion

## TODO

- [ ] Expose multi-threading
- [ ] Add Bloom Filter (avoid scanning all SSTables for non-existent keys)
- [ ] Manage Bloom filter reconstruction
- [ ] Improve compaction logic
- [ ] Find faster deserializer
- [ ] Support loading existing database
- [ ] Add tests and fix spaghetti code
- [ ] Consider `RwLock`
