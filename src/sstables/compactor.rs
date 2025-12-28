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

const MERGE_THRESHOLD: f64 = 4.0;

pub struct CompactorManager {
    sstables_dir: PathBuf,
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
fn handle_compaction_check(
    sstables_dir: &Path,
    sstables: &Mutex<Vec<Arc<SSTable>>>,
) -> Result<bool, Error> {
    let current_state = { sstables.lock().expect("sstables lock poisoned").clone() };

    let to_merge = find_sstables_to_merge(&current_state);

    if let Some((index, (id_1, id_2))) = to_merge {
        // We can only safely delete tombstones if we're merging the last 2 entries
        let save_tombstones = index != current_state.len() - 2;

        let new_sstable = merge_sstables(
            sstables_dir,
            &current_state[index],
            &current_state[index + 1],
            save_tombstones,
        )?;

        let new_sstable = Arc::new(new_sstable);

        {
            let mut locked_sstables = sstables.lock().expect("sstables lock poisoned");

            // Check that both ids exist consecutively by comparing ids directly
            let ids_present_and_consecutive = locked_sstables
                .windows(2)
                .any(|window| window[0].id == id_1 && window[1].id == id_2);

            if !ids_present_and_consecutive {
                // handle
                panic!();
            }

            // Safe to merge and overwrite: remove the two sstables and insert the new one.
            // SSTables inserted during compaction are kept
            let new_state: Vec<Arc<SSTable>> = locked_sstables
                .iter()
                .flat_map(|sstable| {
                    if sstable.id == id_1 {
                        Some(new_sstable.clone())
                    } else if sstable.id == id_2 {
                        None
                    } else {
                        Some(sstable.clone())
                    }
                })
                .collect();

            *locked_sstables = new_state;
        }

        background_file_delete(current_state[index].clone());
        background_file_delete(current_state[index + 1].clone());
    }

    Ok(to_merge.is_some())
}

fn merge_sstables(
    sstables_dir: &Path,
    t1: &SSTable,
    t2: &SSTable,
    save_tombstones: bool,
) -> Result<SSTable, Error> {
    let contents_1 = {
        let contents = functions::read_file(&t1.file, t1.file_size)?;
        serialization::deserialize_entries_from_bytes(&contents, "sstable")?
    };
    let contents_2 = {
        let contents = functions::read_file(&t2.file, t2.file_size)?;
        serialization::deserialize_entries_from_bytes(&contents, "sstable")?
    };

    let merged = merge_sstable_contents(&contents_1, &contents_2, save_tombstones);

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

/// `t1` and `t2` must be sorted.
/// `t1` is expected to be the newer table
fn merge_sstable_contents(
    t1: &[KVMemoryRepr],
    t2: &[KVMemoryRepr],
    save_tombstones: bool,
) -> Vec<KVMemoryRepr> {
    let mut result = Vec::new();
    let mut i = 0;
    let mut j = 0;

    while i < t1.len() && j < t2.len() {
        match t1[i].cmp(&t2[j]) {
            std::cmp::Ordering::Less => {
                if save_tombstones || t1[i].value().is_some() {
                    result.push(KVMemoryRepr::new(*t1[i].key(), *t1[i].value()));
                }
                i += 1;
            }
            std::cmp::Ordering::Equal => {
                // Conflict: use t1 (newer table)
                if save_tombstones || t1[i].value().is_some() {
                    result.push(KVMemoryRepr::new(*t1[i].key(), *t1[i].value()));
                }
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Greater => {
                if save_tombstones || t2[j].value().is_some() {
                    result.push(KVMemoryRepr::new(*t2[j].key(), *t2[j].value()));
                }
                j += 1;
            }
        }
    }

    // Add remaining elements from t1
    while i < t1.len() {
        if save_tombstones || t1[i].value().is_some() {
            result.push(KVMemoryRepr::new(*t1[i].key(), *t1[i].value()));
        }
        i += 1;
    }

    // Add remaining elements from t2
    while j < t2.len() {
        if save_tombstones || t2[j].value().is_some() {
            result.push(KVMemoryRepr::new(*t2[j].key(), *t2[j].value()));
        }
        j += 1;
    }

    result
}

/// Returns an index to merge with its successor
fn find_sstables_to_merge(sstables: &[Arc<SSTable>]) -> Option<(usize, (u64, u64))> {
    sstables
        .windows(2)
        .enumerate()
        .filter(|(_, pair)| {
            let s1 = pair[0].file_size;
            let s2 = pair[1].file_size;

            if s1 == 0 || s2 == 0 {
                return false;
            }

            // Calculate ratio (Large / Small) to check if they are similar enough.
            // If the ratio is huge (e.g. 100MB vs 1KB), it's not worth the I/O cost yet.
            let (min, max) = if s1 < s2 { (s1, s2) } else { (s2, s1) };
            (max as f64 / min as f64) <= MERGE_THRESHOLD
        })
        // From the valid pairs, pick the one that is cheapest to merge (smallest total size)
        .min_by_key(|(_, pair)| pair[0].file_size + pair[1].file_size)
        .map(|(i, pair)| (i, (pair[0].id, pair[1].id)))
}
