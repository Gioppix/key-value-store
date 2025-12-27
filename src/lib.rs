mod append_log;
mod cleanup;
mod errors;
mod files;
mod functions;
mod sstables;

use functions::KVMemoryRepr;
use sstables::compactor::CompactorManager;
use std::fs::{self};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::errors::Error;
use crate::files::FileWithPath;
use crate::functions::FindResult;
use crate::sstables::SSTable;

/// 16kb page size on Mac M
const FILE_SIZE_BYTES: u64 = 1024 * 16;

pub struct KVStorage {
    // Key lock
    /// File and the current write offset
    append_log: Mutex<(Arc<FileWithPath>, Mutex<u64>, Arc<Mutex<Vec<KVMemoryRepr>>>)>,
    /// Sorted list (newer at the beginning) of SSTables
    sstables: Arc<Mutex<Vec<Arc<SSTable>>>>,
    base_dir: PathBuf,
    sstables_dir: PathBuf,
    compaction_manager: CompactorManager,
}

type Key = u64;
type Value = u64;

impl KVStorage {
    /// Creates a new KV database
    pub fn new(location: &str) -> Result<Self, Error> {
        let path = Path::new(location);
        if !path.is_dir() {
            return Err(Error::InvalidDbLocation);
        }

        let db_dir = path.join("db");
        fs::create_dir(&db_dir).map_err(|_| Error::FileDirectoryCreation)?;
        let sstables_dir = db_dir.join("sstables");
        fs::create_dir(&sstables_dir).map_err(|_| Error::FileDirectoryCreation)?;

        let file = append_log::create_append_log(&db_dir)?;

        let sstables: Arc<Mutex<_>> = Default::default();

        Ok(Self {
            append_log: Mutex::new((Arc::new(file), Mutex::new(0), Default::default())),
            sstables: sstables.clone(),
            base_dir: db_dir,
            sstables_dir: sstables_dir.clone(),
            compaction_manager: CompactorManager::new(sstables_dir, sstables),
        })
    }

    pub fn write(&self, key: Key, value: Option<Value>) -> Result<(), Error> {
        let data = KVMemoryRepr::new(key, value);

        // Clone the Arc since a slot on that file was acquired
        let (slot, log_file, in_memory) = loop {
            let log_slot = {
                let log_data = self.append_log.lock().expect("poisoned append_log_lock");
                functions::acquire_log_slot(data.size(), &log_data.1)
                    .map(|s| (s, log_data.0.clone(), log_data.2.clone()))
            };

            match log_slot {
                Some(slot) => break slot,
                None => {
                    // Create new append file
                    let old_log_file = {
                        let mut append_log = self.append_log.lock().expect("poisoned append_log");

                        let file = append_log::create_append_log(&self.base_dir)?;

                        let (old_file, ..) = std::mem::replace(
                            &mut *append_log,
                            (Arc::new(file), Default::default(), Default::default()),
                        );

                        old_file
                    };

                    let sstable =
                        sstables::log_file_to_sstable(&self.sstables_dir, &old_log_file.file)?;
                    let sstable = Arc::new(sstable);
                    cleanup::background_file_delete(old_log_file);

                    self.sstables
                        .lock()
                        .expect("poisoned sstables lock")
                        .insert(0, sstable);

                    self.compaction_manager.signal_sstable_inserted();
                }
            }
        };

        functions::write_data_at_offset(&log_file.file, &data, slot)?;

        in_memory
            .lock()
            .expect("poisoned in_memory_log lock")
            .push(data);

        Ok(())
    }

    pub fn read(&self, key: &Key) -> Result<Option<Value>, Error> {
        let append_log_lock = self.append_log.lock().expect("poisoned in_memory_log lock");

        // Search from the end to get the most recent value for the key
        for entry in append_log_lock
            .2
            .lock()
            .expect("poisoned in_memory")
            .iter()
            .rev()
        {
            if entry.key() == key {
                return Ok(*entry.value());
            }
        }

        // Clone the current state (not the sstables themselves)
        // Since their content is effectively immutable this operation is safe (the only possible change is compaction/merge)
        let current_sstables_state = &self
            .sstables
            .lock()
            .expect("sstables lock poisoned")
            .clone();

        // Start scanning SSTables in order
        for sstable in current_sstables_state {
            let res = sstables::find_in_sstable(key, sstable)?;

            match res {
                FindResult::Found(value) => return Ok(Some(value)),
                FindResult::Tombstone => return Ok(None),
                FindResult::None => {}
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_everything() {
        let location = format!("./test-dbs/{}", rand::random::<u64>());
        fs::create_dir_all(&location).unwrap();

        let kv = KVStorage::new(&location).unwrap();
        kv.write(1, Some(10)).unwrap();
        assert_eq!(kv.read(&1).unwrap(), Some(10));
        kv.write(1, Some(20)).unwrap();
        assert_eq!(kv.read(&1).unwrap(), Some(20));
        kv.write(2, None).unwrap();
        assert_eq!(kv.read(&2).unwrap(), None);
        assert_eq!(kv.read(&99).unwrap(), None);
    }
}
