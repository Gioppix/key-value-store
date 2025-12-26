use crate::sstables::SSTable;
use std::{
    fs::{File, remove_file},
    path::PathBuf,
    sync::Arc,
    thread::{sleep, spawn},
    time::Duration,
};

const MAX_RETRIES: u64 = 10;
const RETRY_INTERVAL_MS: u64 = 10;

trait Cleanable {
    fn clean(self);
    fn path(&self) -> PathBuf;
}

impl Cleanable for (File, PathBuf) {
    fn clean(self) {
        let path = self.1.to_owned();
        match remove_file(self.1) {
            Ok(_) => {}
            Err(e) => {
                log::error!("failed to remove file {:?}: {:?}", path, e);
            }
        }
    }

    fn path(&self) -> PathBuf {
        self.1.clone()
    }
}

impl Cleanable for SSTable {
    fn clean(self) {
        let path = self.file_path().to_owned();
        match self.cleanup() {
            Ok(_) => {}
            Err(e) => {
                log::error!("failed to remove file {path:?}: {e:?}");
            }
        }
    }

    fn path(&self) -> PathBuf {
        self.file_path().to_owned()
    }
}

pub fn background_file_delete<T: Cleanable + Sync + Send + 'static>(mut file: Arc<T>) {
    let path = file.path();
    spawn(move || {
        for _ in 0..MAX_RETRIES {
            sleep(Duration::from_millis(RETRY_INTERVAL_MS));

            file = match Arc::try_unwrap(file) {
                Ok(f) => {
                    f.clean();
                    return;
                }
                Err(e) => e,
            };
        }
        log::error!("failed to remove log file {:?}: max retires reached", path,);
    });
}
