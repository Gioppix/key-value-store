use postcard::{take_from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{FILE_SIZE_BYTES, errors::Error};

use super::{Key, Value};
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::Path,
    sync::Mutex,
};

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct KVMemoryRepr {
    key: Key,
    /// Holds the value (or the tombstone)
    value: Option<Value>,
    /// Used to distinguish from empty bytes. Should **ALWAYS** be true
    valid: bool,
}

impl KVMemoryRepr {
    pub fn new(key: Key, value: Option<Value>) -> Self {
        Self {
            key,
            value,
            valid: true,
        }
    }

    pub fn size(&self) -> u64 {
        postcard::experimental::serialized_size(&self).expect("serializable") as u64
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn value(&self) -> &Option<Value> {
        &self.value
    }
}

impl PartialOrd for KVMemoryRepr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KVMemoryRepr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

pub enum FindResult {
    Found(Value),
    Tombstone,
    None,
}

pub fn create_file(path: &Path, file_size_bytes: u64) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.set_len(file_size_bytes)?;

    Ok(file)
}

pub fn write_data_at_offset(file: &File, data: &KVMemoryRepr, offset: u64) -> Result<(), Error> {
    let serialized: Vec<u8> = to_allocvec(data)?;
    file.write_at(serialized.as_slice(), offset)?;

    Ok(())
}

/// Checks if the current file can
pub fn acquire_log_slot(requested_size: u64, mutex: &Mutex<u64>) -> Option<u64> {
    let mut offset = mutex.lock().expect("lock poisoned");
    let current_write_offset = *offset;

    let remaining_space = FILE_SIZE_BYTES - current_write_offset;

    if requested_size > remaining_space {
        None
    } else {
        *offset += requested_size;
        Some(current_write_offset)
    }
}

/// Deserializes KV entries from a byte slice.
///
/// Ignores the eventual empty part (all zeros).
pub fn deserialize_entries_from_bytes(buffer: &[u8]) -> Result<Vec<KVMemoryRepr>, Error> {
    let mut kv_entries = vec![];
    let mut remaining_slice = buffer;

    while !remaining_slice.is_empty() {
        match take_from_bytes::<KVMemoryRepr>(remaining_slice) {
            Ok((entry, unused)) => {
                // Check if this is an actual struct or just empty space
                if entry.valid {
                    kv_entries.push(entry);
                }

                remaining_slice = unused;
            }
            Err(e) => {
                if remaining_slice.iter().all(|b| *b == 0) {
                    // The file had an empty part, ignore it
                    break;
                } else {
                    return Err(Error::Serialization(e));
                }
            }
        }
    }

    Ok(kv_entries)
}

pub fn read_file(file: &File, file_size: u64) -> Result<Vec<u8>, Error> {
    let mut buffer = vec![0u8; file_size as usize];
    file.read_exact_at(&mut buffer, 0)?;

    Ok(buffer)
}

pub fn write_file(file: &File, buffer: &[u8], max_size: u64) -> Result<(), Error> {
    if buffer.len() > max_size as usize {
        return Err(Error::FileDirectoryCreation);
    }

    file.write_all_at(buffer, 0)?;

    Ok(())
}

/// Reads all entries in a file.
///
/// Ignores the eventual empty part.
pub fn deserialize_file(file: &File, file_size: u64) -> Result<Vec<KVMemoryRepr>, Error> {
    let contents = read_file(file, file_size)?;

    deserialize_entries_from_bytes(&contents)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_empty() {
        let buf = vec![0u8; 100];
        let result = deserialize_entries_from_bytes(&buf).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_deserialize_single_entry() {
        let entry = KVMemoryRepr::new(1, Some(99999));
        let serialized = to_allocvec(&entry).unwrap();
        let result = deserialize_entries_from_bytes(&serialized).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(*result[0].key(), 1);
    }

    #[test]
    fn test_deserialize_multiple_entries() {
        let mut buf = Vec::new();
        buf.extend(to_allocvec(&KVMemoryRepr::new(1, Some(2))).unwrap());
        buf.extend(to_allocvec(&KVMemoryRepr::new(3, Some(4))).unwrap());
        buf.extend(vec![0u8; 32]);

        let result = deserialize_entries_from_bytes(&buf).unwrap();
        assert_eq!(result.len(), 2);
    }
}
