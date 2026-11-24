use std::{
    fs::{File, OpenOptions},
    io::{self, ErrorKind, Write},
    path::Path,
};

use crate::{
    config::{WAL_CHECKPOINT_THRESHOLD_BYTES, WAL_MAX_SIZE_BYTES},
    wal::record::WalRecord,
};

#[derive(Debug)]
pub struct WalWriter {
    file: File,
    size: u64,
    last_sequence: u64,
    failed: bool,
}

impl WalWriter {
    pub fn open(path: &Path, last_sequence: u64, recovered_len: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(recovered_len == 0)
            .append(true)
            .open(path)?;

        let size = file.metadata()?.len();

        if size != recovered_len {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("WAL size mismatch: expected {recovered_len}, found {size}"),
            ));
        }

        if size > WAL_MAX_SIZE_BYTES {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "existing WAL exceeds maximum capacity",
            ));
        }

        #[cfg(unix)]
        {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));

            File::open(parent)?.sync_all()?;
        }

        Ok(Self {
            file,
            size,
            last_sequence,
            failed: false,
        })
    }

    pub fn append(&mut self, record: WalRecord) -> io::Result<()> {
        self.ensure_healthy()?;

        let expected_sequence = self
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("WAL sequence exhausted"))?;

        if record.sequence != expected_sequence {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "expected WAL sequence {}, received {}",
                    expected_sequence, record.sequence
                ),
            ));
        }

        let encoded = record.encode().map_err(|error| {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!("invalid WAL record: {error:?}"),
            )
        })?;

        let new_size = self
            .size
            .checked_add(encoded.len() as u64)
            .ok_or_else(|| io::Error::other("WAL size overflow"))?;

        if new_size > WAL_MAX_SIZE_BYTES {
            self.failed = true;

            return Err(io::Error::new(
                ErrorKind::StorageFull,
                "WAL maximum capacity exceeded",
            ));
        }

        if let Err(error) = self.file.write_all(&encoded) {
            self.failed = true;
            return Err(error);
        }

        self.size = new_size;
        self.last_sequence = expected_sequence;
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.ensure_healthy()?;

        if let Err(error) = self.file.sync_all() {
            self.failed = true;
            return Err(error);
        }

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub fn checkpoint_required(&self) -> bool {
        self.size >= WAL_CHECKPOINT_THRESHOLD_BYTES
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    fn ensure_healthy(&self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other(
                "WAL writer is suspended; recovery required",
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::codec::{Request, encode_request};
    use crate::wal::record::{RECORD_OVERHEAD, RecordKind};

    use std::fs;

    fn command_record(sequence: u64) -> WalRecord {
        WalRecord {
            kind: RecordKind::Command,
            sequence,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 0,
            payload: encode_request(&Request::Set {
                key: b"counter".to_vec(),
                value: b"100".to_vec(),
            })
            .unwrap(),
        }
    }

    #[test]
    fn append_writes_record_to_disk() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut writer = WalWriter::open(&path, 0, 0).unwrap();

        let record = command_record(1);

        writer.append(record.clone()).unwrap();
        writer.sync().unwrap();

        let bytes = fs::read(&path).unwrap();
        let decoded = WalRecord::decode(&bytes).unwrap();

        assert_eq!(decoded, record);
        assert_eq!(writer.last_sequence(), 1);
        assert_eq!(writer.size(), bytes.len() as u64);
    }

    #[test]
    fn multiple_records_are_appended() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut writer = WalWriter::open(&path, 0, 0).unwrap();

        let first = command_record(1);
        let second = command_record(2);

        let mut expected = first.clone().encode().unwrap();
        expected.extend_from_slice(&second.clone().encode().unwrap());

        writer.append(first).unwrap();
        writer.append(second).unwrap();
        writer.sync().unwrap();

        assert_eq!(fs::read(&path).unwrap(), expected);
        assert_eq!(writer.last_sequence(), 2);
    }

    #[test]
    fn out_of_order_sequence_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut writer = WalWriter::open(&path, 0, 0).unwrap();

        let error = writer.append(command_record(2)).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(writer.last_sequence(), 0);
        assert_eq!(writer.size(), 0);
        assert!(!writer.is_failed());

        writer.append(command_record(1)).unwrap();

        assert_eq!(writer.last_sequence(), 1);
    }

    #[test]
    fn reopening_preserves_existing_records() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut writer = WalWriter::open(&path, 0, 0).unwrap();

        writer.append(command_record(1)).unwrap();
        writer.sync().unwrap();

        let previous_size = writer.size();

        drop(writer);

        let mut reopened = WalWriter::open(&path, 1, previous_size).unwrap();

        reopened.append(command_record(2)).unwrap();
        reopened.sync().unwrap();

        assert_eq!(reopened.last_sequence(), 2);
        assert!(reopened.size() > previous_size);
    }

    #[test]
    fn unvalidated_existing_wal_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        fs::write(&path, b"existing data").unwrap();

        let error = WalWriter::open(&path, 0, 0).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);

        assert_eq!(fs::read(&path).unwrap(), b"existing data");
    }

    #[test]
    fn checkpoint_threshold_is_detected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut writer = WalWriter::open(&path, 0, 0).unwrap();

        assert!(!writer.checkpoint_required());

        let record = WalRecord {
            kind: RecordKind::Command,
            sequence: 1,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 0,
            payload: encode_request(&Request::Set {
                key: b"large".to_vec(),
                value: vec![0; WAL_CHECKPOINT_THRESHOLD_BYTES as usize],
            })
            .unwrap(),
        };

        writer.append(record).unwrap();

        assert!(writer.checkpoint_required());
    }

    #[test]
    fn capacity_limit_suspends_writer() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        // Sparse file: simulate a nearly full WAL without
        // physically writing 64 MiB of test data.
        let size = WAL_MAX_SIZE_BYTES - RECORD_OVERHEAD as u64;

        File::create(&path).unwrap().set_len(size).unwrap();

        let mut writer = WalWriter::open(&path, 100, size).unwrap();

        let error = writer.append(command_record(101)).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::StorageFull);
        assert!(writer.is_failed());
        assert_eq!(writer.last_sequence(), 100);
        assert_eq!(writer.size(), size);
        assert_eq!(fs::metadata(&path).unwrap().len(), size);

        assert!(writer.append(command_record(101)).is_err());
    }
}
