use super::Value;
use crate::{FILE_SIZE_BYTES, errors::Error};
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::Path,
    sync::Mutex,
};

pub enum FindResult {
    Found(Value),
    Tombstone,
    None,
}

pub fn create_file(path: &Path, file_size_bytes: u64) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.set_len(file_size_bytes)?;

    Ok(file)
}

pub fn write_data_at_offset(file: &File, data: &[u8], offset: u64) -> Result<(), Error> {
    file.write_at(data, offset)?;

    Ok(())
}

pub fn read_file(file: &File, file_size: u64) -> Result<Vec<u8>, Error> {
    let mut buffer = vec![0u8; file_size as usize];
    file.read_exact_at(&mut buffer, 0)?;

    Ok(buffer)
}

pub fn write_file(file: &File, buffer: &[u8], max_size: u64) -> Result<(), Error> {
    if buffer.len() > max_size as usize {
        return Err(Error::FileDirectoryCreation);
    }

    file.write_all_at(buffer, 0)?;

    Ok(())
}

/// Sorts a slice in-place using insertion sort in ascending order, ordering by a key function.
///
/// This function performs an insertion sort on the slice `to_sort`, where the sort order
/// is determined by the value returned by the `key` function for each element.
pub fn insertion_sort_by_key<T, F: Fn(&T) -> u64>(to_sort: &mut [T], key: F) {
    for i in 1..to_sort.len() {
        let mut j = i;
        while j > 0 && key(&to_sort[j - 1]) > key(&to_sort[j]) {
            to_sort.swap(j - 1, j);
            j -= 1;
        }
    }
}
