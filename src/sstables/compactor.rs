use crate::{
    cleanup::background_file_delete,
    errors::Error,
    functions::{self},
    serialization::{self, KVMemoryRepr},
    sstables::{self, SSTable, entries_to_index_and_data},
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::AtomicBool},
    thread::spawn,
};

const MIN_TABLES_IN_MERGE: usize = 4;
const MAX_TABLES_IN_MERGE: usize = 30;

pub struct CompactorManager {
    sstables_dir: PathBuf,
    /// Tables are sorted newest first (index 0 is the most recent table)
    sstables: Arc<Mutex<Vec<Arc<SSTable>>>>,
    currently_compacting: Arc<AtomicBool>,
}

impl CompactorManager {
    pub fn new(sstables_dir: PathBuf, sstables: Arc<Mutex<Vec<Arc<SSTable>>>>) -> Self {
        Self {
            sstables_dir,
            sstables,
            currently_compacting: Default::default(),
        }
    }

    pub fn signal_sstable_inserted(&self) {
        let sstables_dir = self.sstables_dir.clone();
        let sstables = self.sstables.clone();
        let compacting = self.currently_compacting.clone();

        if compacting.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return; // Already compacting
        }

        spawn(move || {
            if let Err(e) = handle_compaction_check_rec(&sstables_dir, &sstables) {
                log::error!("Compaction check failed: {:?}", e)
            }
            compacting.store(false, std::sync::atomic::Ordering::SeqCst);
        });
    }
}

fn handle_compaction_check_rec(
    sstables_dir: &Path,
    sstables: &Mutex<Vec<Arc<SSTable>>>,
) -> Result<(), Error> {
    loop {
        let merged = handle_compaction_check(sstables_dir, sstables)?;
        if !merged {
            break;
        }
    }
    Ok(())
}

/// `sstables` must be sorted newest to oldest
///
/// Return whether a merge actually happened
fn handle_compaction_check(
    sstables_dir: &Path,
    sstables: &Mutex<Vec<Arc<SSTable>>>,
) -> Result<bool, Error> {
    let current_state = { sstables.lock().expect("sstables lock poisoned").clone() };

    let to_merge = find_sstables_to_merge(&current_state);

    for (start, end) in &to_merge {
        let sizes: Vec<u64> = current_state[*start..*end]
            .iter()
            .map(|t| t.file_size)
            .collect();
        log::trace!("Merging group [{}, {}): sizes = {:?}", start, end, sizes);
    }

    // Spawn a thread for each merge operation
    let handles: Vec<_> = to_merge
        .iter()
        .map(|(start, end)| {
            let sstables_dir = sstables_dir.to_path_buf();
            let tables_to_merge: Vec<Arc<SSTable>> = current_state[*start..*end].to_vec();

            // Save tombstones if this range includes the end
            let save_tombstones = *end != current_state.len();

            spawn(move || {
                merge_sstables(&sstables_dir, tables_to_merge.as_slice(), save_tombstones)
            })
        })
        .collect();

    // Join all threads and collect results
    let merged_sstables: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("merge thread panicked"))
        .collect::<Result<_, _>>()?;

    // Update the sstables list with all merged results
    for (i, new_sstable) in merged_sstables.into_iter().enumerate() {
        let new_sstable = Arc::new(new_sstable);
        let (start, end) = to_merge[i];

        let old_tables = current_state[start..end].to_vec();
        let old_ids: Vec<_> = old_tables.iter().map(|t| t.id).collect();

        {
            let mut locked_sstables = sstables.lock().expect("sstables lock poisoned");

            // Check that all ids from the range still exist consecutively
            let mut found_start = None;
            for i in 0..locked_sstables.len() {
                if i + old_ids.len() <= locked_sstables.len() {
                    let consecutive_match =
                        (0..old_ids.len()).all(|j| locked_sstables[i + j].id == old_ids[j]);

                    if consecutive_match {
                        found_start = Some(i);
                        break;
                    }
                }
            }

            if found_start.is_none() {
                // TODO handle
                panic!();
            }

            // Safe to merge and overwrite: remove the old sstables and insert the new one.
            // SSTables inserted during compaction are kept
            let new_state: Vec<Arc<SSTable>> = locked_sstables
                .iter()
                .flat_map(|sstable| {
                    if old_ids.contains(&sstable.id) {
                        if sstable.id == old_ids[0] {
                            Some(new_sstable.clone())
                        } else {
                            None
                        }
                    } else {
                        Some(sstable.clone())
                    }
                })
                .collect();

            *locked_sstables = new_state;
        }

        for old_table in old_tables {
            background_file_delete(old_table);
        }
    }

    Ok(!to_merge.is_empty())
}

/// Tables are expected newer first
fn merge_sstables(
    sstables_dir: &Path,
    tables: &[Arc<SSTable>],
    save_tombstones: bool,
) -> Result<SSTable, Error> {
    let mut contents = Vec::with_capacity(tables.len());
    for table in tables {
        let file_contents = functions::read_file(&table.file, table.file_size)?;
        let entries = serialization::deserialize_entries_from_bytes(&file_contents, "sstable")?;
        contents.push(entries);
    }

    let merged = merge_sstable_contents(contents, save_tombstones);

    let (index, data, bloom_filter) = entries_to_index_and_data(&merged)?;

    let id: u64 = rand::random();
    let (file, path, size) = sstables::create_sstable_file(id, sstables_dir, &data)?;

    let sstable = SSTable {
        id,
        index,
        file,
        file_path: path,
        file_size: size,
        bloom_filter,
    };

    Ok(sstable)
}

/// `lists` are expected newest first;
/// each list must be sorted by key
fn merge_sstable_contents(
    lists: Vec<Vec<KVMemoryRepr>>,
    save_tombstones: bool,
) -> Vec<KVMemoryRepr> {
    let mut result = Vec::new();

    // Convert each Vec into an iterator with an index
    let mut iters: Vec<_> = lists
        .into_iter()
        .map(|v| v.into_iter().peekable())
        .collect();

    loop {
        // First pass: find the minimum key among all current elements
        let mut min_key = None;

        for it in iters.iter_mut() {
            if let Some(kv) = it.peek() {
                let key = kv.key();
                match min_key {
                    None => {
                        min_key = Some(*key);
                    }
                    Some(current_min) if *key < current_min => {
                        min_key = Some(*key);
                    }
                    _ => {}
                }
            }
        }

        // If no minimum key found, we're done
        let min_key = match min_key {
            None => break,
            Some(key) => key,
        };

        // Second pass: process all iterators with the minimum key
        let mut value_to_save = None;
        for it in iters.iter_mut() {
            if let Some(kv) = it.peek()
                && kv.key() == &min_key
            {
                // Safety: we just peek'd
                let kv = it.next().unwrap();

                // Save the first (newest) value we encounter
                if value_to_save.is_none() {
                    value_to_save = Some(kv);
                }
            }
        }

        // Save the value if appropriate
        if let Some(kv) = value_to_save
            && (save_tombstones || kv.value().is_some())
        {
            result.push(kv);
        }
    }

    result
}

/// Returns list of indexes of tables to merge in the form `[start, end)`
///
/// The ranges are non-overlapping.
fn find_sstables_to_merge(sstables: &[Arc<SSTable>]) -> Vec<(usize, usize)> {
    let mut result = Vec::new();

    let mut i = sstables.len();

    while i > 0 {
        // Start a new group from position i-1
        let group_end = i;
        let current_bucket = get_bucket(sstables[i - 1].file_size);
        let mut group_start = i - 1;

        // Scan backwards while in the same bucket and under MAX_TABLES_IN_MERGE
        while group_start > 0 && (group_end - group_start) < MAX_TABLES_IN_MERGE {
            let prev_idx = group_start - 1;
            let prev_bucket = get_bucket(sstables[prev_idx].file_size);

            if prev_bucket == current_bucket {
                group_start = prev_idx;
            } else {
                break;
            }
        }

        // Check if this group meets the minimum size requirement
        let group_size = group_end - group_start;
        if group_size >= MIN_TABLES_IN_MERGE {
            result.push((group_start, group_end));
        }

        // Move to the next position
        i = group_start;
    }

    result
}

// Helper function to determine bucket for a table size
fn get_bucket(size: u64) -> u32 {
    if size == 0 {
        return 0;
    }
    // Bucket boundaries: 0-10MB, 10-100MB, 100-1000MB, etc.
    // 10MB = 10_000_000 bytes
    let size_in_mb = size / 1_000_000;
    if size_in_mb < 10 {
        0
    } else if size_in_mb < 100 {
        1
    } else if size_in_mb < 1000 {
        2
    } else if size_in_mb < 10000 {
        3
    } else {
        4
    }
}
