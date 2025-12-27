pub mod compactor;

use crate::FILE_SIZE_BYTES;
use crate::cleanup::CleanableFile;
use crate::functions::{FindResult, KVMemoryRepr};
use crate::{Key, errors::Error, functions};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::{fs::File, path::Path};

const TABLE_TO_INDEX_RATIO: u64 = 128;

/// A SSTable with in-memory index
pub struct SSTable {
    id: u64,
    /// Sorted list of (Key, offset) values
    index: Index,
    /// File containing sorted entries
    file: File,
    file_path: PathBuf,
    /// File size in bytes
    file_size: u64,
}

impl SSTable {
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }
}

impl CleanableFile for SSTable {
    fn path(&self) -> PathBuf {
        self.file_path().to_owned()
    }
}

type Index = Vec<(Key, u64)>;

fn log_content_to_index_and_data(log_file_content: &[u8]) -> Result<(Index, Vec<u8>), Error> {
    let mut log_file_entries = functions::deserialize_entries_from_bytes(log_file_content)?;

    // This is a STABLE sort (important)
    log_file_entries.sort();

    // Entries will be deduplicated and sorted
    let mut entries: Vec<KVMemoryRepr> = Vec::new();

    for entry in log_file_entries.into_iter() {
        if let Some(last) = entries.last_mut()
            && last.key() == entry.key()
        {
            *last = entry;
            continue;
        }

        entries.push(entry);
    }

    entries_to_index_and_data(&entries)
}

fn entries_to_index_and_data(entries: &[KVMemoryRepr]) -> Result<(Index, Vec<u8>), Error> {
    let index_size = (FILE_SIZE_BYTES / TABLE_TO_INDEX_RATIO).max(1);
    let index_interval = entries.len() / index_size as usize;
    let mut index = Vec::new();
    let mut sstable_data = Vec::new();
    let mut total_offset = 0u64;

    for (i, entry) in entries.iter().enumerate() {
        let serialized = serialize_entry(entry)?;
        let entry_size = serialized.len() as u64;

        if index_interval > 0 && i % index_interval == 0 {
            index.push((*entry.key(), total_offset));
        }

        sstable_data.extend_from_slice(&serialized);
        total_offset += entry_size;
    }

    Ok((index, sstable_data))
}

fn serialize_entry(entry: &KVMemoryRepr) -> Result<Vec<u8>, Error> {
    postcard::to_allocvec::<KVMemoryRepr>(entry).map_err(Into::into)
}

fn create_sstable_file(
    id: u64,
    sstables_dir: &Path,
    sstable_data: &[u8],
) -> Result<(File, PathBuf, u64), Error> {
    let sstable_file_size = sstable_data.len() as u64;
    let sstable_path = sstables_dir.join(format!("{id}"));
    let sstable_file = functions::create_file(&sstable_path, sstable_file_size)?;
    functions::write_file(&sstable_file, sstable_data, sstable_file_size)?;

    Ok((sstable_file, sstable_path, sstable_file_size))
}

pub fn log_file_to_sstable(sstables_dir: &Path, log_file: &File) -> Result<SSTable, Error> {
    let log_file_content = functions::read_file(log_file, FILE_SIZE_BYTES)?;
    let (index, sstable_data) = log_content_to_index_and_data(&log_file_content)?;

    let id: u64 = rand::random();
    let (sstable_file, sstable_path, sstable_file_size) =
        create_sstable_file(id, sstables_dir, &sstable_data)?;

    Ok(SSTable {
        id,
        index,
        file: sstable_file,
        file_path: sstable_path,
        file_size: sstable_file_size,
    })
}

pub fn find_in_sstable(key: &Key, sstable: &SSTable) -> Result<FindResult, Error> {
    let (range_start, range_end) = index_to_range(key, &sstable.index);
    let range_end = range_end.unwrap_or(sstable.file_size);

    let size = range_end - range_start;
    let mut buffer = vec![0u8; size as usize];
    sstable.file.read_exact_at(&mut buffer, range_start)?;

    let entries = functions::deserialize_entries_from_bytes(&buffer)?;
    // TODO: test just a linear search as with small arrays it exploits cache locality or pipelining or whatever
    let maybe_entry_index = entries.binary_search_by_key(key, |t| *t.key()).ok();

    // it's important to distinguish between finding none and not finding anything
    let result = match maybe_entry_index {
        Some(i) => match *entries[i].value() {
            Some(value) => FindResult::Found(value),
            None => FindResult::Tombstone,
        },
        None => FindResult::None,
    };

    Ok(result)
}

fn index_to_range(key: &Key, index: &Index) -> (u64, Option<u64>) {
    let mut start_offset = 0;
    let mut end_offset = None;

    // Binary search to find the appropriate range in the index
    let pos = index.binary_search_by_key(&key, |(k, _)| k);

    match pos {
        Ok(idx) => {
            // Exact match found
            start_offset = index[idx].1;
            end_offset = index.get(idx + 1).map(|(_, offset)| *offset);
        }
        Err(idx) => {
            // Key would be inserted at idx
            if idx > 0 {
                start_offset = index[idx - 1].1;
            }
            if idx < index.len() {
                end_offset = Some(index[idx].1);
            }
        }
    }

    (start_offset, end_offset)
}
