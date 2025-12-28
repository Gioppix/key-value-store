use crate::{
    FILE_SIZE_BYTES, Key, Value, cleanup,
    errors::Error,
    files::FileWithPath,
    functions::{self, FindResult},
    serialization::{self, KVMemoryRepr},
    sstables::{self, SSTable, compactor::CompactorManager},
};
use std::{
    mem,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock, RwLockReadGuard},
};

/// The file's offset is added to prevent this vector having the wrong order
type InMemoryAppendLog = Vec<(u64, KVMemoryRepr)>;

/// Represents the log file, the current available write location and the in-memory copy
type InnerState = (FileWithPath, Mutex<u64>, RwLock<InMemoryAppendLog>);

pub struct AppendLog {
    state: RwLock<InnerState>,
    file_rotation_lock: Mutex<()>,
    db_dir: PathBuf,
}

impl AppendLog {
    pub fn new(db_dir: &Path) -> Result<Self, Error> {
        let file = create_append_log_file(db_dir)?;

        Ok(Self {
            state: RwLock::new((file, Mutex::new(0), Default::default())),
            file_rotation_lock: Default::default(),
            db_dir: db_dir.to_owned(),
        })
    }

    /// This will search for `key` in the append log
    pub fn find_key(&self, key: &Key) -> FindResult {
        let state_lock = self.state.read().expect("poisoned state lock");

        // Search from the end to get the most recent value for the key
        for entry in state_lock
            .2
            .read()
            .expect("poisoned in_memory")
            .iter()
            .rev()
        {
            if entry.1.key() == key {
                return match entry.1.value() {
                    Some(v) => FindResult::Found(*v),
                    None => FindResult::Tombstone,
                };
            }
        }

        FindResult::None
    }

    /// This will write a `key` in the append log, creating new files as needed
    pub fn write_key(
        &self,
        key: Key,
        value: Option<Value>,
        sstables_dir: &Path,
        sstables: &Mutex<Vec<Arc<SSTable>>>,
        compaction_manager: &CompactorManager,
    ) -> Result<(), Error> {
        let data = KVMemoryRepr::new(key, value);

        let serialized_data = serialization::serialize(&data)?;
        let serialized_data_len = serialized_data.len() as u64;

        // Clone the Arc since a slot on that file was acquired
        let (slot, read_lock) = loop {
            let log_slot = self.try_acquire_slot(serialized_data_len);

            match log_slot {
                Some(slot) => break slot,
                None => {
                    // Create new append file
                    {
                        let rotation_lock_guard =
                            self.file_rotation_lock.lock().expect("poisoned lock");

                        // During wait for rotation lock another worker might have created a new file
                        if let Some(slot) = self.try_acquire_slot(serialized_data_len) {
                            break slot;
                        }

                        let file = create_append_log_file(&self.db_dir)?;

                        // Up until here, reads work (writes will wait for rotation lock).
                        // It's important that after this point there's no ongoing writes on the file
                        let mut append_log = self.state.write().expect("poisoned append_log");

                        let (old_log_file, ..) = mem::replace(
                            &mut *append_log,
                            (file, Default::default(), Default::default()),
                        );

                        let sstable =
                            sstables::log_file_to_sstable(sstables_dir, &old_log_file.file)?;
                        let sstable = Arc::new(sstable);

                        sstables
                            .lock()
                            .expect("poisoned sstables lock")
                            .insert(0, sstable);

                        drop(rotation_lock_guard);
                        // It's important that append log lock is dropped after this point.
                        // The in-memory logs are cleared and new reads must go through sstables, hence the write must happen.
                        drop(append_log);

                        // Avoid making other threads wait on this
                        cleanup::remove_file_logged(&old_log_file.path);
                    };

                    compaction_manager.signal_sstable_inserted();
                }
            }
        };

        functions::write_data_at_offset(&read_lock.0.file, &serialized_data, slot)?;

        let mut in_memory_log_guard = read_lock.2.write().expect("poisoned in_memory_log lock");

        in_memory_log_guard.push((slot, data));
        // Insertion sort since it's almost sorted
        functions::insertion_sort_by_key(&mut in_memory_log_guard, |k| k.0);

        Ok(())
    }

    /// Returns, if possible, the read lock to the state and the reserved slot
    fn try_acquire_slot(&self, size: u64) -> Option<(u64, RwLockReadGuard<'_, InnerState>)> {
        let state_lock = self.state.read().expect("poisoned append_log_lock");
        AppendLog::calculate_log_slot(size, &state_lock.1).map(|location| (location, state_lock))
    }

    /// Checks if the current file can fit a new entry and, if so, updates the current write pointer
    fn calculate_log_slot(requested_size: u64, offset: &Mutex<u64>) -> Option<u64> {
        let mut offset_guard = offset.lock().expect("lock poisoned");
        let current_write_offset = *offset_guard;

        let remaining_space = FILE_SIZE_BYTES - current_write_offset;

        if requested_size > remaining_space {
            None
        } else {
            *offset_guard += requested_size;
            Some(current_write_offset)
        }
    }
}

fn create_append_log_file(base_dir: &Path) -> Result<FileWithPath, Error> {
    let random_suffix = rand::random::<u64>();
    let log_name = format!("log_{}", random_suffix);
    let log_path = base_dir.join(log_name);

    let file = functions::create_file(&log_path, FILE_SIZE_BYTES)?;

    Ok(FileWithPath {
        file,
        path: log_path,
    })
}
