use crate::{
    FILE_SIZE_BYTES, Key, Value, cleanup,
    errors::Error,
    files::FileWithPath,
    functions::{self, FindResult, KVMemoryRepr},
    sstables::{self, SSTable, compactor::CompactorManager},
};
use std::{
    mem,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub struct AppendLog {
    state: Mutex<(Arc<FileWithPath>, Mutex<u64>, Arc<Mutex<Vec<KVMemoryRepr>>>)>,
    file_rotation_lock: Mutex<()>,
    db_dir: PathBuf,
}

impl AppendLog {
    pub fn new(db_dir: &Path) -> Result<Self, Error> {
        let file = create_append_log_file(db_dir)?;

        Ok(Self {
            state: Mutex::new((Arc::new(file), Mutex::new(0), Default::default())),
            file_rotation_lock: Default::default(),
            db_dir: db_dir.to_owned(),
        })
    }

    /// This will search for `key` in the append log
    pub fn find_key(&self, key: &Key) -> FindResult {
        let state_lock = self.state.lock().expect("poisoned state lock");

        // Search from the end to get the most recent value for the key
        for entry in state_lock
            .2
            .lock()
            .expect("poisoned in_memory")
            .iter()
            .rev()
        {
            if entry.key() == key {
                return match entry.value() {
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

        // Clone the Arc since a slot on that file was acquired
        let (slot, log_file, in_memory) = loop {
            let log_slot = self.try_acquire_slot(data.size());

            match log_slot {
                Some(slot) => break slot,
                None => {
                    // Create new append file
                    {
                        let rotation_lock_guard =
                            self.file_rotation_lock.lock().expect("poisoned lock");

                        // During wait for rotation lock another worker might have created a new file
                        if let Some(slot) = self.try_acquire_slot(data.size()) {
                            break slot;
                        }

                        let file = create_append_log_file(&self.db_dir)?;

                        // Up until here, reads work (writes will wait for rotation lock)
                        let mut append_log = self.state.lock().expect("poisoned append_log");

                        let (old_log_file, ..) = mem::replace(
                            &mut *append_log,
                            (Arc::new(file), Default::default(), Default::default()),
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
                        cleanup::background_file_delete(old_log_file);
                    };

                    compaction_manager.signal_sstable_inserted();
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

    fn try_acquire_slot(
        &self,
        size: u64,
    ) -> Option<(u64, Arc<FileWithPath>, Arc<Mutex<Vec<KVMemoryRepr>>>)> {
        let state_lock = self.state.lock().expect("poisoned append_log_lock");
        functions::acquire_log_slot(size, &state_lock.1)
            .map(|s| (s, state_lock.0.clone(), state_lock.2.clone()))
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
