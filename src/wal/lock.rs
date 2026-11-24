use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, ErrorKind},
    path::{Path, PathBuf},
};

pub const LOCK_FILE_NAME: &str = "ember.lock";

#[derive(Debug)]
pub struct PersistenceLock {
    _file: File,
}

impl PersistenceLock {
    pub fn acquire(directory: &Path) -> io::Result<Self> {
        fs::create_dir_all(directory)?;

        let path = directory.join(LOCK_FILE_NAME);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(fs::TryLockError::WouldBlock) => Err(io::Error::new(
                ErrorKind::WouldBlock,
                format!(
                    "persistence directory already locked {}",
                    directory.display()
                ),
            )),
            Err(TryLockError::Error(error)) => Err(error),
        }
    }
}

fn persistence_directory(path: &Path) -> io::Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    fs::create_dir_all(parent)?;

    fs::canonicalize(parent)
}

pub fn acquire_persistence_locks(
    snapshot_path: &Path,
    wal_path: &Path,
) -> io::Result<Vec<PersistenceLock>> {
    let snapshot_directory = persistence_directory(snapshot_path)?;
    let wal_directory = persistence_directory(wal_path)?;

    let snapshot_name = snapshot_path
        .file_name()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "invalid snapshot path"))?;

    let wal_name = wal_path
        .file_name()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "invalid WAL path"))?;

    if snapshot_name == LOCK_FILE_NAME || wal_name == LOCK_FILE_NAME {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "persistence path uses reserved lock filename",
        ));
    }

    if snapshot_directory.join(snapshot_name) == wal_directory.join(wal_name) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "snapshot and WAL paths must be different",
        ));
    }

    let mut directories = vec![snapshot_directory, wal_directory];

    directories.sort();
    directories.dedup();

    directories
        .iter()
        .map(|directory| PersistenceLock::acquire(directory))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_lock_rejects_second_owner() {
        let directory = tempfile::tempdir().unwrap();

        let _first = PersistenceLock::acquire(directory.path()).unwrap();

        let error = PersistenceLock::acquire(directory.path()).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::WouldBlock);
    }

    #[test]
    fn lock_is_released_when_owner_is_dropped() {
        let directory = tempfile::tempdir().unwrap();

        let first = PersistenceLock::acquire(directory.path()).unwrap();

        drop(first);

        let _second = PersistenceLock::acquire(directory.path()).unwrap();
    }

    #[test]
    fn existing_lock_file_does_not_prevent_startup() {
        let directory = tempfile::tempdir().unwrap();

        fs::write(directory.path().join(LOCK_FILE_NAME), b"").unwrap();

        let _lock = PersistenceLock::acquire(directory.path()).unwrap();
    }

    #[test]
    fn shared_directory_requires_only_one_lock() {
        let directory = tempfile::tempdir().unwrap();

        let snapshot = directory.path().join("snapshot.json");

        let wal = directory.path().join("wal.log");

        let locks = acquire_persistence_locks(&snapshot, &wal).unwrap();

        assert_eq!(locks.len(), 1);
    }

    #[test]
    fn separate_directories_require_separate_locks() {
        let directory = tempfile::tempdir().unwrap();

        let snapshot = directory.path().join("snapshots/snapshot.json");

        let wal = directory.path().join("logs/wal.log");

        let locks = acquire_persistence_locks(&snapshot, &wal).unwrap();

        assert_eq!(locks.len(), 2);
    }

    #[test]
    fn identical_persistence_paths_are_rejected() {
        let directory = tempfile::tempdir().unwrap();

        let path = directory.path().join("database");

        let error = acquire_persistence_locks(&path, &path).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}
