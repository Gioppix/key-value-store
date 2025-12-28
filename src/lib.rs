mod append_log;
mod cleanup;
mod errors;
mod files;
mod functions;
mod serialization;
mod sstables;

use crate::append_log::AppendLog;
use crate::errors::Error;
use crate::functions::FindResult;
use crate::sstables::SSTable;
use sstables::compactor::CompactorManager;
use std::fs::{self};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// 16kb page size on Mac M
const FILE_SIZE_BYTES: u64 = 1024 * 16;

pub struct KVStorage {
    // Key lock
    /// File and the current write offset
    append_log: AppendLog,
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

        let sstables: Arc<Mutex<_>> = Default::default();

        let append_log = AppendLog::new(&db_dir)?;

        Ok(Self {
            // append_log: Mutex::new((Arc::new(file), Mutex::new(0), Default::default())),
            append_log,
            sstables: sstables.clone(),
            base_dir: db_dir,
            sstables_dir: sstables_dir.clone(),
            compaction_manager: CompactorManager::new(sstables_dir, sstables),
        })
    }

    pub fn write(&self, key: Key, value: Option<Value>) -> Result<(), Error> {
        self.append_log.write_key(
            key,
            value,
            &self.sstables_dir,
            &self.sstables,
            &self.compaction_manager,
        )
    }

    pub fn read(&self, key: &Key) -> Result<Option<Value>, Error> {
        let append_log_result = self.append_log.find_key(key);

        match append_log_result {
            FindResult::Found(value) => return Ok(Some(value)),
            FindResult::Tombstone => return Ok(None),
            FindResult::None => {}
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
            let res = sstable.find(key)?;

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
