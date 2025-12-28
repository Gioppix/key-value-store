use bitcode::{Decode, Encode};

use crate::{Key, Value, errors::Error};

// 16mb
const STRUCT_LEN_BYTES: usize = 3;

#[derive(PartialEq, Eq, Encode, Decode)]
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

#[derive(Debug)]
pub enum SerializationError {
    SerializingStruct(bitcode::Error),
    BufferTooSmall,
    InvalidLength,
    DecodeFailed(bitcode::Error),
}

impl From<bitcode::Error> for SerializationError {
    fn from(error: bitcode::Error) -> Self {
        SerializationError::SerializingStruct(error)
    }
}

pub fn serialize(data: &KVMemoryRepr) -> Result<Vec<u8>, Error> {
    let encoded_struct = bitcode::encode(data);

    let mut result = Vec::with_capacity(STRUCT_LEN_BYTES + encoded_struct.len());
    result.resize(STRUCT_LEN_BYTES, 0);

    serialize_length(
        encoded_struct.len() as u64,
        &mut result[0..STRUCT_LEN_BYTES],
    )?;
    result.extend_from_slice(&encoded_struct);

    Ok(result)
}

/// Deserializes KV entries from a byte slice.
///
/// Ignores the eventual empty part (all zeros).
pub fn deserialize_entries_from_bytes(
    buffer: &[u8],
    file: &'static str,
) -> Result<Vec<KVMemoryRepr>, Error> {
    let mut kv_entries = vec![];
    let mut remaining_slice = buffer;

    while !remaining_slice.is_empty() {
        let p = deserialize(remaining_slice);

        if let Err(_) = &p
            && remaining_slice.iter().any(|b| *b != 0)
        {
            eprintln!(
                "Error deserializing {file}, first 30 bytes: {:?}",
                &remaining_slice[..remaining_slice.len().min(30)]
            );
        }

        match p {
            Ok((entry, unused)) => {
                // Check if this is an actual struct or just empty space
                // TODO: this could be a corrupted write
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
                    return Err(e);
                }
            }
        }
    }

    Ok(kv_entries)
}

pub fn deserialize(bytes: &[u8]) -> Result<(KVMemoryRepr, &[u8]), Error> {
    if bytes.len() < STRUCT_LEN_BYTES {
        return Err(Error::Serialization(SerializationError::BufferTooSmall));
    }

    let length_bytes: [u8; STRUCT_LEN_BYTES] = bytes[0..STRUCT_LEN_BYTES]
        .try_into()
        .map_err(|_| Error::Serialization(SerializationError::BufferTooSmall))?;

    let struct_len = deserialize_length(&length_bytes) as usize;

    if struct_len == 0 {
        return Err(Error::Serialization(SerializationError::BufferTooSmall));
    }

    if bytes.len() < STRUCT_LEN_BYTES + struct_len {
        return Err(Error::Serialization(SerializationError::BufferTooSmall));
    }

    let struct_bytes = &bytes[STRUCT_LEN_BYTES..STRUCT_LEN_BYTES + struct_len];

    let entry: KVMemoryRepr = bitcode::decode(struct_bytes).map_err(|e| {
        eprintln!(
            "Decode error: len={}, bytes={:?}",
            struct_bytes.len(),
            struct_bytes
        );
        Error::Serialization(SerializationError::DecodeFailed(e))
    })?;

    let remaining = &bytes[STRUCT_LEN_BYTES + struct_len..];

    Ok((entry, remaining))
}

fn serialize_length(length: u64, bytes: &mut [u8]) -> Result<(), Error> {
    let max_length = (1 << (STRUCT_LEN_BYTES * 8)) - 1;

    if length > max_length {
        return Err(Error::TooBig);
    }

    // No clue what this wants me to do
    #[allow(clippy::needless_range_loop)]
    for i in 0..STRUCT_LEN_BYTES {
        bytes[i] = (length >> (i * 8)) as u8;
    }

    Ok(())
}

fn deserialize_length(bytes: &[u8; STRUCT_LEN_BYTES]) -> u64 {
    let mut length = 0u64;

    // This is cleaner than `bytes.enumerate()`
    #[allow(clippy::needless_range_loop)]
    for i in 0..STRUCT_LEN_BYTES {
        length |= (bytes[i] as u64) << (i * 8);
    }

    length
}
