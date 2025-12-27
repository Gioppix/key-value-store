use std::{
    fs::remove_file,
    path::{Path, PathBuf},
    sync::Arc,
    thread::{sleep, spawn},
    time::Duration,
};

// 10ms to 10 seconds
const MAX_RETRIES: u32 = 10;
const FIRST_DELAY_INTERVAL_MS: u32 = 10;

pub trait CleanableFile {
    fn path(&self) -> PathBuf;
}

fn remove_file_logged(path: &Path) {
    match remove_file(path) {
        Ok(_) => {}
        Err(e) => {
            log::error!("failed to remove file {:?}: {:?}", path, e);
        }
    }
}

/// Removes a file after it's not longer used. Uses exponential backoff.
///
/// This function relies on the fact that all other copies of the `Arc` are dropped after being used.
pub fn background_file_delete<T: CleanableFile + Sync + Send + 'static>(mut file: Arc<T>) {
    let path = file.path();
    spawn(move || {
        for retry in 0..MAX_RETRIES {
            let delay = FIRST_DELAY_INTERVAL_MS * 2_u32.pow(retry);
            sleep(Duration::from_millis(delay as u64));

            file = match Arc::try_unwrap(file) {
                Ok(_) => {
                    remove_file_logged(&path);
                    log::trace!("File {path:?} cleaned at retry {retry}");
                    return;
                }
                Err(arc) => arc,
            };
        }
        log::error!("failed to remove log file {:?}: max retires reached", path,);
    });
}
