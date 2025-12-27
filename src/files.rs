use std::{fs::File, path::PathBuf};

use crate::cleanup::CleanableFile;

pub struct FileWithPath {
    pub file: File,
    pub path: PathBuf,
}

impl CleanableFile for FileWithPath {
    fn path(&self) -> PathBuf {
        self.path.clone()
    }
}
