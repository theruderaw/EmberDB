use std::{
    fs::{File, OpenOptions},
    io::{self, ErrorKind, Read},
    path::Path,
};

use crate::{
    config::WAL_MAX_SIZE_BYTES,
    wal::record::{MAX_RECORD_SIZE, RECORD_OVERHEAD, WalRecord},
};

#[derive(Debug)]
pub struct WalReadResult {
    pub records: Vec<WalRecord>,
    pub valid_len: u64,
    pub incomplete_tail_len: u64,
}

impl WalReadResult {
    pub fn last_sequence(&self) -> Option<u64> {
        self.records.last().map(|record| record.sequence)
    }

    pub fn has_incomplete_tail(&self) -> bool {
        self.incomplete_tail_len > 0
    }
}

pub fn read_wal(path: &Path) -> io::Result<WalReadResult> {
    let mut file = match File::open(path) {
        Ok(file) => file,

        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(WalReadResult {
                records: Vec::new(),
                valid_len: 0,
                incomplete_tail_len: 0,
            });
        }

        Err(error) => return Err(error),
    };

    let file_size = file.metadata()?.len();

    if file_size > WAL_MAX_SIZE_BYTES {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "WAL exceeds maximum capacity",
        ));
    }

    let mut records: Vec<WalRecord> = Vec::new();
    let mut offset = 0_u64;

    while offset < file_size {
        let remaining = file_size - offset;

        if remaining < 4 {
            break;
        }

        let mut length_bytes = [0_u8; 4];

        file.read_exact(&mut length_bytes)?;

        let declated_length = u64::from(u32::from_be_bytes(length_bytes));

        let total_length = declated_length + 4;

        if total_length < RECORD_OVERHEAD as u64 || total_length > MAX_RECORD_SIZE as u64 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid WAL record length at offset {offset}"),
            ));
        }

        if total_length > remaining {
            break;
        }

        let mut buffer = vec![0_u8; total_length as usize];

        buffer[..4].copy_from_slice(&length_bytes);

        file.read_exact(&mut buffer[4..])?;

        let record = WalRecord::decode(&buffer).map_err(|error| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid WAL record at offset {offset}: {error:?}"),
            )
        })?;

        if let Some(previous) = records.last() {
            let expected = previous
                .sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "WAL sequence overflow"))?;

            if record.sequence != expected {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "WAL sequence mismatch at offset {offset}: \
                         expected {expected}, received {}",
                        record.sequence
                    ),
                ));
            }
        }

        records.push(record);
        offset += total_length;
    }

    Ok(WalReadResult {
        records,
        valid_len: offset,
        incomplete_tail_len: file_size - offset,
    })
}

pub fn discard_incomplete_tail(path: &Path, result: &WalReadResult) -> io::Result<()> {
    if !result.has_incomplete_tail() {
        return Ok(());
    }

    let file = OpenOptions::new().write(true).open(path)?;

    let expected_size = result
        .valid_len
        .checked_add(result.incomplete_tail_len)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "WAL size overflow"))?;

    if file.metadata()?.len() != expected_size {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "WAL changed after validation",
        ));
    }

    file.set_len(result.valid_len)?;
    file.sync_all()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::codec::{Request, encode_request};
    use crate::wal::record::RecordKind;
    use crate::wal::writer::WalWriter;

    use std::fs;
    use std::io::Write;

    fn command_record(sequence: u64) -> WalRecord {
        WalRecord {
            kind: RecordKind::Command,
            sequence,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 0,
            payload: encode_request(&Request::Set {
                key: b"counter".to_vec(),
                value: sequence.to_string().into_bytes(),
            })
            .unwrap(),
        }
    }

    #[test]
    fn missing_wal_returns_empty_result() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let result = read_wal(&path).unwrap();

        assert!(result.records.is_empty());
        assert_eq!(result.valid_len, 0);
        assert_eq!(result.last_sequence(), None);
        assert!(!result.has_incomplete_tail());
        assert!(!path.exists());
    }

    #[test]
    fn empty_wal_returns_empty_result() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        File::create(&path).unwrap();

        let result = read_wal(&path).unwrap();

        assert!(result.records.is_empty());
        assert_eq!(result.valid_len, 0);
        assert!(!result.has_incomplete_tail());
    }

    #[test]
    fn reads_multiple_consecutive_records() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut writer = WalWriter::open(&path, 0, 0).unwrap();

        for sequence in 1..=3 {
            writer.append(command_record(sequence)).unwrap();
        }

        writer.sync().unwrap();

        let result = read_wal(&path).unwrap();

        assert_eq!(
            result.records,
            vec![command_record(1), command_record(2), command_record(3),]
        );

        assert_eq!(result.last_sequence(), Some(3));
        assert_eq!(result.valid_len, writer.size());
        assert!(!result.has_incomplete_tail());
    }

    #[test]
    fn first_sequence_can_follow_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut writer = WalWriter::open(&path, 100, 0).unwrap();

        writer.append(command_record(101)).unwrap();
        writer.append(command_record(102)).unwrap();
        writer.sync().unwrap();

        let result = read_wal(&path).unwrap();

        assert_eq!(result.records[0].sequence, 101);
        assert_eq!(result.last_sequence(), Some(102));
    }

    #[test]
    fn incomplete_trailing_record_can_be_discarded() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let complete = command_record(1).encode().unwrap();
        let incomplete = command_record(2).encode().unwrap();

        let mut file = File::create(&path).unwrap();

        file.write_all(&complete).unwrap();
        file.write_all(&incomplete[..20]).unwrap();
        file.sync_all().unwrap();

        drop(file);

        let result = read_wal(&path).unwrap();

        assert_eq!(result.records, vec![command_record(1)]);
        assert_eq!(result.valid_len, complete.len() as u64);
        assert_eq!(result.incomplete_tail_len, 20);
        assert!(result.has_incomplete_tail());

        // Reading must not modify the WAL.
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            complete.len() as u64 + 20
        );

        discard_incomplete_tail(&path, &result).unwrap();

        assert_eq!(fs::read(&path).unwrap(), complete);

        let recovered = read_wal(&path).unwrap();

        assert!(!recovered.has_incomplete_tail());
        assert_eq!(recovered.last_sequence(), Some(1));
    }

    #[test]
    fn incomplete_length_header_is_detected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut bytes = command_record(1).encode().unwrap();
        bytes.extend_from_slice(&[0, 0]);

        fs::write(&path, bytes).unwrap();

        let result = read_wal(&path).unwrap();

        assert_eq!(result.records.len(), 1);
        assert_eq!(result.incomplete_tail_len, 2);
    }

    #[test]
    fn corrupted_complete_record_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut encoded = command_record(1).encode().unwrap();

        encoded[30] ^= 0xFF;

        fs::write(&path, &encoded).unwrap();

        let error = read_wal(&path).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);

        // Corrupt records must not be silently removed.
        assert_eq!(fs::read(&path).unwrap(), encoded);
    }

    #[test]
    fn sequence_gap_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut bytes = command_record(1).encode().unwrap();

        bytes.extend_from_slice(&command_record(3).encode().unwrap());

        fs::write(&path, bytes).unwrap();

        let error = read_wal(&path).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn duplicate_sequence_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        let mut bytes = command_record(1).encode().unwrap();

        bytes.extend_from_slice(&command_record(1).encode().unwrap());

        fs::write(&path, bytes).unwrap();

        let error = read_wal(&path).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn invalid_record_length_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        fs::write(&path, 1_u32.to_be_bytes()).unwrap();

        let error = read_wal(&path).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn oversized_wal_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.log");

        File::create(&path)
            .unwrap()
            .set_len(WAL_MAX_SIZE_BYTES + 1)
            .unwrap();

        let error = read_wal(&path).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }
}
