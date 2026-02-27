//! This module implements a cached file system allowing for results to be stored in-memory rather
//! rather being queried from the file system again.

use std::fs;
use std::io::{Error, Result};
use std::path::{Path, PathBuf};

use std::sync::LazyLock;

use moka::sync::Cache;

pub fn read(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    static READ_CACHE: LazyLock<Cache<PathBuf, Vec<u8>>> = LazyLock::new(|| Cache::new(10_000));

    let path = path.as_ref().canonicalize()?;
    match READ_CACHE.get(path.as_path()) {
        Some(content) => Ok(content),
        None => {
            let content = fs::read(path.as_path())?;
            READ_CACHE.insert(path, content.clone());
            Ok(content)
        }
    }
}

pub fn read_to_string(path: impl AsRef<Path>) -> Result<String> {
    let content = read(path)?;
    String::from_utf8(content).map_err(|_| {
        Error::new(
            std::io::ErrorKind::InvalidData,
            "The contents of the file are not valid UTF8",
        )
    })
}

pub fn read_dir(path: impl AsRef<Path>) -> Result<Box<dyn Iterator<Item = Result<PathBuf>>>> {
    static READ_DIR_CACHE: LazyLock<Cache<PathBuf, Vec<PathBuf>>> = LazyLock::new(|| Cache::new(10_000));

    let path = path.as_ref().canonicalize()?;
    match READ_DIR_CACHE.get(path.as_path()) {
        Some(entries) => Ok(Box::new(entries.into_iter().map(Ok)) as Box<_>),
        None => {
            let entries: Vec<PathBuf> = fs::read_dir(path.as_path())?
                .flat_map(|maybe_entry| maybe_entry.map(|entry| entry.path()))
                .collect();
            READ_DIR_CACHE.insert(path, entries.clone());
            Ok(Box::new(entries.into_iter().map(Ok)) as Box<_>)
        }
    }
}
