use crate::persistence::{Snapshot, load_snapshot};
use crate::wal::reader::{WalReadResult, read_wal};
use crate::wal::record::RecordKind;
use crate::wal::replay::ReplayState;

use std::io::{self, ErrorKind};
use std::path::Path;

#[derive(Debug)]
pub struct RecoveryResult {
    pub snapshot: Snapshot,
    pub wal: WalReadResult,
    pub replayed_commands: usize,
}

pub fn recover(
    snapshot_path: &Path,
    wal_path: &Path,
    recovery_timestamp: u64,
) -> io::Result<RecoveryResult> {
    let snapshot = load_snapshot(snapshot_path)?;

    let checkpoint_sequence = snapshot
        .as_ref()
        .map_or(0, |snapshot| snapshot.last_sequence);

    let mut state = match snapshot {
        Some(snapshot) => ReplayState::from_snapshot(snapshot)?,
        None => ReplayState::new(),
    };

    let wal = read_wal(wal_path)?;

    let mut last_sequence = checkpoint_sequence;
    let mut replayed_commands = 0;

    for record in &wal.records {
        // Skip commands already included in the snapshot.
        if record.sequence <= checkpoint_sequence {
            continue;
        }

        let expected_sequence = last_sequence
            .checked_add(1)
            .ok_or_else(|| invalid_data("WAL sequence exhausted"))?;

        if record.sequence != expected_sequence {
            return Err(invalid_data(format!(
                "recovery sequence mismatch: expected {}, received {}",
                expected_sequence, record.sequence
            )));
        }

        // Transaction-aware replay will be added in M12.8.
        if record.kind != RecordKind::Command || record.transaction_id != 0 {
            return Err(invalid_data("transaction replay is not yet supported"));
        }

        state
            .apply(&record.payload, record.timestamp_unix_ms)
            .map_err(|error| {
                invalid_data(format!(
                    "replay failed at sequence {}: {error}",
                    record.sequence
                ))
            })?;

        last_sequence = record.sequence;
        replayed_commands += 1;
    }

    let snapshot = state.into_snapshot(last_sequence, recovery_timestamp);

    Ok(RecoveryResult {
        snapshot,
        wal,
        replayed_commands,
    })
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Request, encode_request};
    use crate::persistence::{SNAPSHOT_VERSION, SnapshotEntry, save_snapshot};
    use crate::wal::record::WalRecord;

    use std::fs;
    use std::path::PathBuf;

    struct Fixture {
        _directory: tempfile::TempDir,
        snapshot_path: PathBuf,
        wal_path: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let snapshot_path = directory.path().join("snapshot.json");
            let wal_path = directory.path().join("wal.log");

            Self {
                _directory: directory,
                snapshot_path,
                wal_path,
            }
        }

        fn save_checkpoint(&self, last_sequence: u64, entries: Vec<SnapshotEntry>) {
            save_snapshot(
                &self.snapshot_path,
                &Snapshot {
                    version: SNAPSHOT_VERSION,
                    last_sequence,
                    entries,
                },
            )
            .unwrap();
        }

        fn write_records(&self, records: Vec<WalRecord>) {
            let mut bytes = Vec::new();

            for record in records {
                bytes.extend_from_slice(&record.encode().unwrap());
            }

            fs::write(&self.wal_path, bytes).unwrap();
        }

        fn recover(&self, timestamp: u64) -> io::Result<RecoveryResult> {
            recover(&self.snapshot_path, &self.wal_path, timestamp)
        }
    }

    fn entry(key: &[u8], value: &[u8], expires_at_unix_ms: Option<u64>) -> SnapshotEntry {
        SnapshotEntry {
            key: key.to_vec(),
            value: value.to_vec(),
            expires_at_unix_ms,
        }
    }

    fn command(sequence: u64, timestamp_unix_ms: u64, request: Request) -> WalRecord {
        WalRecord {
            kind: RecordKind::Command,
            sequence,
            timestamp_unix_ms,
            transaction_id: 0,
            payload: encode_request(&request).unwrap(),
        }
    }

    fn set(sequence: u64, timestamp: u64, key: &[u8], value: &[u8]) -> WalRecord {
        command(
            sequence,
            timestamp,
            Request::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            },
        )
    }

    fn increment(sequence: u64, timestamp: u64, key: &[u8]) -> WalRecord {
        command(
            sequence,
            timestamp,
            Request::Increment { key: key.to_vec() },
        )
    }

    #[test]
    fn recovers_empty_state_when_both_files_are_missing() {
        let fixture = Fixture::new();
        let result = fixture.recover(1000).unwrap();

        assert_eq!(result.snapshot.version, SNAPSHOT_VERSION);
        assert_eq!(result.snapshot.last_sequence, 0);
        assert!(result.snapshot.entries.is_empty());
        assert_eq!(result.replayed_commands, 0);
        assert!(result.wal.records.is_empty());
        assert_eq!(result.wal.valid_len, 0);
        assert!(!result.wal.has_incomplete_tail());
        assert!(!fixture.snapshot_path.exists());
        assert!(!fixture.wal_path.exists());
    }

    #[test]
    fn recovers_snapshot_without_wal_and_removes_expired_entries() {
        let fixture = Fixture::new();
        let live = entry(b"live", b"value", Some(5001));
        fixture.save_checkpoint(
            10,
            vec![live.clone(), entry(b"expired", b"old", Some(5000))],
        );
        let original = fs::read(&fixture.snapshot_path).unwrap();

        let result = fixture.recover(5000).unwrap();

        assert_eq!(result.snapshot.last_sequence, 10);
        assert_eq!(result.snapshot.entries, vec![live]);
        assert_eq!(result.replayed_commands, 0);
        assert!(result.wal.records.is_empty());
        assert_eq!(fs::read(&fixture.snapshot_path).unwrap(), original);
        assert!(!fixture.wal_path.exists());
    }

    #[test]
    fn recovers_wal_without_snapshot() {
        let fixture = Fixture::new();
        fixture.write_records(vec![
            set(1, 1000, b"counter", b"10"),
            increment(2, 2000, b"counter"),
            command(
                3,
                3000,
                Request::Decrement {
                    key: b"counter".to_vec(),
                },
            ),
            increment(4, 4000, b"counter"),
            set(5, 5000, b"deleted", b"value"),
            command(
                6,
                6000,
                Request::Delete {
                    key: b"deleted".to_vec(),
                },
            ),
        ]);
        let original = fs::read(&fixture.wal_path).unwrap();

        let result = fixture.recover(7000).unwrap();

        assert_eq!(result.snapshot.last_sequence, 6);
        assert_eq!(
            result.snapshot.entries,
            vec![entry(b"counter", b"11", None)]
        );
        assert_eq!(result.replayed_commands, 6);
        assert_eq!(result.wal.records.len(), 6);
        assert_eq!(result.wal.last_sequence(), Some(6));
        assert_eq!(result.wal.valid_len, original.len() as u64);
        assert!(!result.wal.has_incomplete_tail());
        assert_eq!(fs::read(&fixture.wal_path).unwrap(), original);
        assert!(!fixture.snapshot_path.exists());
    }

    #[test]
    fn skips_checkpointed_records_without_reapplying_them() {
        let fixture = Fixture::new();
        fixture.save_checkpoint(2, vec![entry(b"counter", b"11", None)]);
        // The old INCR would make the recovered counter incorrect if replayed twice.
        fixture.write_records(vec![
            increment(2, 2000, b"counter"),
            increment(3, 3000, b"counter"),
        ]);

        let result = fixture.recover(4000).unwrap();

        assert_eq!(result.snapshot.last_sequence, 3);
        assert_eq!(
            result.snapshot.entries,
            vec![entry(b"counter", b"12", None)]
        );
        assert_eq!(result.replayed_commands, 1);
        assert_eq!(result.wal.records.len(), 2);
    }

    #[test]
    fn recovers_wal_starting_immediately_after_checkpoint() {
        let fixture = Fixture::new();
        fixture.save_checkpoint(100, vec![entry(b"counter", b"10", None)]);
        fixture.write_records(vec![increment(101, 2000, b"counter")]);

        let result = fixture.recover(3000).unwrap();

        assert_eq!(result.snapshot.last_sequence, 101);
        assert_eq!(
            result.snapshot.entries,
            vec![entry(b"counter", b"11", None)]
        );
        assert_eq!(result.replayed_commands, 1);
    }

    #[test]
    fn preserves_checkpoint_sequence_when_wal_is_entirely_older() {
        let fixture = Fixture::new();
        let checkpoint = entry(b"counter", b"20", None);
        fixture.save_checkpoint(10, vec![checkpoint.clone()]);
        fixture.write_records(vec![
            increment(8, 1000, b"counter"),
            increment(9, 2000, b"counter"),
        ]);

        let result = fixture.recover(3000).unwrap();

        assert_eq!(result.snapshot.last_sequence, 10);
        assert_eq!(result.snapshot.entries, vec![checkpoint]);
        assert_eq!(result.replayed_commands, 0);
        assert_eq!(result.wal.last_sequence(), Some(9));
    }

    #[test]
    fn uses_historical_timestamps_before_filtering_expired_entries() {
        let fixture = Fixture::new();
        fixture.save_checkpoint(10, vec![entry(b"counter", b"100", Some(5000))]);
        fixture.write_records(vec![
            increment(11, 4000, b"counter"),
            command(
                12,
                4500,
                Request::Expire {
                    key: b"counter".to_vec(),
                    seconds: 10,
                },
            ),
        ]);

        let result = fixture.recover(6000).unwrap();

        assert_eq!(result.snapshot.last_sequence, 12);
        assert_eq!(
            result.snapshot.entries,
            vec![entry(b"counter", b"101", Some(14500))]
        );
        assert_eq!(result.replayed_commands, 2);

        let expired = fixture.recover(14500).unwrap();
        assert!(expired.snapshot.entries.is_empty());
        assert_eq!(expired.snapshot.last_sequence, 12);
        assert_eq!(expired.replayed_commands, 2);
    }

    #[test]
    fn recovers_complete_records_and_reports_incomplete_tail_without_truncating() {
        let fixture = Fixture::new();
        let complete = set(1, 1000, b"counter", b"10").encode().unwrap();
        let partial = increment(2, 2000, b"counter").encode().unwrap();
        let valid_len = complete.len() as u64;
        let mut bytes = complete;
        bytes.extend_from_slice(&partial[..20]);
        fs::write(&fixture.wal_path, &bytes).unwrap();

        let result = fixture.recover(3000).unwrap();

        assert_eq!(result.snapshot.last_sequence, 1);
        assert_eq!(
            result.snapshot.entries,
            vec![entry(b"counter", b"10", None)]
        );
        assert_eq!(result.replayed_commands, 1);
        assert_eq!(result.wal.valid_len, valid_len);
        assert_eq!(result.wal.incomplete_tail_len, 20);
        assert!(result.wal.has_incomplete_tail());
        assert_eq!(fs::read(&fixture.wal_path).unwrap(), bytes);
    }

    #[test]
    fn rejects_first_wal_sequence_without_snapshot_when_not_one() {
        let fixture = Fixture::new();
        fixture.write_records(vec![set(2, 1000, b"key", b"value")]);

        let error = fixture.recover(2000).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("expected 1, received 2"));
    }

    #[test]
    fn rejects_gap_between_checkpoint_and_first_wal_record() {
        let fixture = Fixture::new();
        fixture.save_checkpoint(10, vec![]);
        fixture.write_records(vec![set(12, 1000, b"key", b"value")]);

        let error = fixture.recover(2000).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("expected 11, received 12"));
    }

    #[test]
    fn rejects_gap_within_wal() {
        let fixture = Fixture::new();
        fixture.write_records(vec![
            set(1, 1000, b"key", b"value"),
            increment(3, 2000, b"counter"),
        ]);

        let error = fixture.recover(3000).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("WAL sequence mismatch"));
    }

    #[test]
    fn rejects_transaction_markers_after_checkpoint() {
        let fixture = Fixture::new();

        for kind in [RecordKind::Begin, RecordKind::Commit, RecordKind::Abort] {
            fixture.write_records(vec![WalRecord {
                kind,
                sequence: 1,
                timestamp_unix_ms: 1000,
                transaction_id: 42,
                payload: Vec::new(),
            }]);

            let error = fixture.recover(2000).unwrap_err();

            assert_eq!(error.kind(), ErrorKind::InvalidData);
            assert!(
                error
                    .to_string()
                    .contains("transaction replay is not yet supported")
            );
        }
    }

    #[test]
    fn rejects_transactional_command_after_checkpoint() {
        let fixture = Fixture::new();
        let mut record = set(1, 1000, b"key", b"value");
        record.transaction_id = 42;
        fixture.write_records(vec![record]);

        let error = fixture.recover(2000).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("transaction replay is not yet supported")
        );
    }

    #[test]
    fn reports_replay_failure_with_sequence_number() {
        let fixture = Fixture::new();
        fixture.write_records(vec![
            set(1, 1000, b"counter", b"10"),
            command(2, 2000, Request::Ping),
        ]);
        let original = fs::read(&fixture.wal_path).unwrap();

        let error = fixture.recover(3000).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("replay failed at sequence 2"));
        assert!(error.to_string().contains("non-mutating command in WAL"));
        assert_eq!(fs::read(&fixture.wal_path).unwrap(), original);
    }

    #[test]
    fn rejects_corrupt_snapshot_without_rewriting_it() {
        let fixture = Fixture::new();
        let bytes = b"{ invalid json";
        fs::write(&fixture.snapshot_path, bytes).unwrap();
        fixture.write_records(vec![set(1, 1000, b"key", b"value")]);

        let error = fixture.recover(2000).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("invalid snapshot"));
        assert_eq!(fs::read(&fixture.snapshot_path).unwrap().as_slice(), bytes);
    }

    #[test]
    fn rejects_corrupt_complete_wal_without_truncating_it() {
        let fixture = Fixture::new();
        let mut bytes = set(1, 1000, b"key", b"value").encode().unwrap();
        // Alter the checksum, leaving the record complete.
        *bytes.last_mut().unwrap() ^= 0xFF;
        fs::write(&fixture.wal_path, &bytes).unwrap();

        let error = fixture.recover(2000).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("ChecksumMismatch"));
        assert_eq!(fs::read(&fixture.wal_path).unwrap(), bytes);
    }
}
