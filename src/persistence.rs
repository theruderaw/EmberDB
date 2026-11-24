use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use crate::version::LEGACY_SNAPSHOT_VERSION;
pub use crate::version::SNAPSHOT_VERSION;
use tempfile::NamedTempFile;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u32,
    pub last_sequence: u64,
    pub entries: Vec<SnapshotEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expires_at_unix_ms: Option<u64>,
}

#[derive(Deserialize)]
struct SnapshotHeader {
    version: u32,
}

#[derive(Deserialize)]
struct LegacySnapshot {
    entries: Vec<SnapshotEntry>,
}

pub fn load_snapshot(path: &Path) -> io::Result<Option<Snapshot>> {
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(None);
        }

        Err(error) => return Err(error),
    };

    let invalid_snapshot = |error: serde_json::Error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid snapshot: {error}"),
        )
    };

    let header: SnapshotHeader = serde_json::from_slice(&data).map_err(invalid_snapshot)?;

    let snapshot = match header.version {
        LEGACY_SNAPSHOT_VERSION => {
            let legacy: LegacySnapshot = serde_json::from_slice(&data).map_err(invalid_snapshot)?;

            Snapshot {
                version: SNAPSHOT_VERSION,
                last_sequence: 0,
                entries: legacy.entries,
            }
        }

        SNAPSHOT_VERSION => serde_json::from_slice::<Snapshot>(&data).map_err(invalid_snapshot)?,

        version => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported snapshot version: {version}"),
            ));
        }
    };

    Ok(Some(snapshot))
}

pub fn save_snapshot(path: &Path, snapshot: &Snapshot) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    let mut temp = NamedTempFile::new_in(parent)?;

    serde_json::to_writer(temp.as_file_mut(), snapshot).map_err(io::Error::other)?;

    temp.flush()?;
    temp.as_file().sync_all()?;

    temp.persist(path).map_err(|error| error.error)?;

    #[cfg(unix)]
    File::open(parent)?.sync_all()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_round_trip() {
        let original = Snapshot {
            version: SNAPSHOT_VERSION,
            entries: vec![
                SnapshotEntry {
                    key: vec![0, 255, 10],
                    value: b"hello".to_vec(),
                    expires_at_unix_ms: None,
                },
                SnapshotEntry {
                    key: b"temporary".to_vec(),
                    value: b"data".to_vec(),
                    expires_at_unix_ms: Some(1_800_000_000_000),
                },
            ],
            last_sequence: 0,
        };

        let encoded = serde_json::to_vec(&original).unwrap();

        let decoded: Snapshot = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn snapshot_write_replaces_existing_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.json");

        let first = Snapshot {
            version: SNAPSHOT_VERSION,
            entries: vec![SnapshotEntry {
                key: b"counter".to_vec(),
                value: b"10".to_vec(),
                expires_at_unix_ms: None,
            }],
            last_sequence: 0,
        };

        save_snapshot(&path, &first).unwrap();

        let saved: Snapshot = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();

        assert_eq!(saved, first);

        // Replace the previous snapshot.
        let second = Snapshot {
            version: SNAPSHOT_VERSION,
            entries: vec![SnapshotEntry {
                key: b"counter".to_vec(),
                value: b"20".to_vec(),
                expires_at_unix_ms: None,
            }],
            last_sequence: 0,
        };

        save_snapshot(&path, &second).unwrap();

        let saved: Snapshot = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();

        assert_eq!(saved, second);

        // No temporary files should remain.
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn load_snapshot_round_trip() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.json");

        let original = Snapshot {
            version: SNAPSHOT_VERSION,
            entries: vec![SnapshotEntry {
                key: vec![0, 255, 10],
                value: b"hello".to_vec(),
                expires_at_unix_ms: None,
            }],
            last_sequence: 0,
        };

        save_snapshot(&path, &original).unwrap();

        let loaded = load_snapshot(&path).unwrap();

        assert_eq!(loaded, Some(original));
    }

    #[test]
    fn missing_snapshot_is_not_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing.json");

        assert_eq!(load_snapshot(&path).unwrap(), None);
    }

    #[test]
    fn corrupt_snapshot_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.json");

        std::fs::write(&path, b"{ invalid json").unwrap();

        let error = load_snapshot(&path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn unsupported_snapshot_version_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.json");

        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION + 1,
            last_sequence: 0,
            entries: vec![],
        };

        std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();

        let error = load_snapshot(&path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn legacy_snapshot_is_upgraded() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.json");

        let legacy = br#"{
        "version": 1,
        "entries": [
            {
                "key": [97],
                "value": [98],
                "expires_at_unix_ms": null
            }
        ]
    }"#;

        std::fs::write(&path, legacy).unwrap();

        let snapshot = load_snapshot(&path).unwrap().unwrap();

        assert_eq!(snapshot.version, SNAPSHOT_VERSION);
        assert_eq!(snapshot.last_sequence, 0);
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].key, b"a");
        assert_eq!(snapshot.entries[0].value, b"b");

        // Loading must not modify the existing file.
        assert_eq!(std::fs::read(&path).unwrap(), legacy);
    }

    #[test]
    fn snapshot_preserves_wal_sequence() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.json");

        let original = Snapshot {
            version: SNAPSHOT_VERSION,
            last_sequence: 420,
            entries: Vec::new(),
        };

        save_snapshot(&path, &original).unwrap();

        let restored = load_snapshot(&path).unwrap().unwrap();

        assert_eq!(restored.last_sequence, 420);
        assert_eq!(restored, original);
    }

    #[test]
    fn current_snapshot_requires_sequence() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.json");

        let malformed = serde_json::json!({
            "version": SNAPSHOT_VERSION,
            "entries": []
        });

        std::fs::write(&path, serde_json::to_vec(&malformed).unwrap()).unwrap();

        let error = load_snapshot(&path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
