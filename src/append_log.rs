use std::{
    fs::File,
    path::{Path, PathBuf},
};

use crate::{FILE_SIZE_BYTES, errors::Error, functions};

pub fn create_append_log(base_dir: &Path) -> Result<(File, PathBuf), Error> {
    let random_suffix = rand::random::<u64>();
    let log_name = format!("log_{}", random_suffix);
    let log_path = base_dir.join(log_name);

    let file = functions::create_file(&log_path, FILE_SIZE_BYTES)?;

    Ok((file, log_path))
}
