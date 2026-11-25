use crate::codec::{Request, decode_request};
use crate::persistence::{Snapshot, SnapshotEntry};
use crate::version::SNAPSHOT_VERSION;

use std::collections::HashMap;
use std::io::{self, ErrorKind};

#[derive(Debug)]
struct ReplayEntry {
    value: Vec<u8>,
    expires_at_unix_ms: Option<u64>,
}

#[derive(Debug, Default)]
pub struct ReplayState {
    entries: HashMap<Vec<u8>, ReplayEntry>,
}

impl ReplayState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_snapshot(snapshot: Snapshot) -> io::Result<Self> {
        if snapshot.version != SNAPSHOT_VERSION {
            return Err(invalid_data("unsupported snapshot version"));
        }

        let mut entries = HashMap::new();

        for item in snapshot.entries {
            let previous = entries.insert(
                item.key,
                ReplayEntry {
                    value: item.value,
                    expires_at_unix_ms: item.expires_at_unix_ms,
                },
            );

            if previous.is_some() {
                return Err(invalid_data("duplicate snapshot key"));
            }
        }

        Ok(Self { entries })
    }

    fn remove_if_expired(&mut self, key: &[u8], timestamp: u64) {
        let expired = self
            .entries
            .get(key)
            .and_then(|entry| entry.expires_at_unix_ms)
            .is_some_and(|deadline| deadline <= timestamp);

        if expired {
            self.entries.remove(key);
        }
    }

    pub fn apply(&mut self, payload: &[u8], timestamp: u64) -> io::Result<()> {
        let request = decode_request(payload)
            .map_err(|error| invalid_data(format!("invalid WAL command: {error:?}")))?;

        match request {
            Request::Set { key, value } => {
                self.entries.insert(
                    key,
                    ReplayEntry {
                        value,
                        expires_at_unix_ms: None,
                    },
                );
            }

            Request::Delete { key } => {
                self.entries.remove(&key);
            }

            Request::Expire { key, seconds } => {
                self.remove_if_expired(&key, timestamp);

                if let Some(entry) = self.entries.get_mut(&key) {
                    if seconds <= 0 {
                        self.entries.remove(&key);
                    } else {
                        let milliseconds = (seconds as u64)
                            .checked_mul(1000)
                            .ok_or_else(|| invalid_data("expiration duration overflow"))?;

                        let deadline = timestamp
                            .checked_add(milliseconds)
                            .ok_or_else(|| invalid_data("expiration timestamp overflow"))?;

                        entry.expires_at_unix_ms = Some(deadline);
                    }
                }
            }

            Request::Increment { key } => {
                self.change_by(&key, 1, timestamp)?;
            }

            Request::Decrement { key } => {
                self.change_by(&key, -1, timestamp)?;
            }

            _ => {
                return Err(invalid_data("non-mutating command in WAL"));
            }
        }

        Ok(())
    }

    fn change_by(&mut self, key: &[u8], delta: i64, timestamp: u64) -> io::Result<()> {
        self.remove_if_expired(key, timestamp);

        let (current, expires_at_unix_ms) = match self.entries.get(key) {
            Some(entry) => {
                let value = std::str::from_utf8(&entry.value)
                    .map_err(|_| invalid_data("invalid counter encoding"))?;

                let current = value
                    .parse::<i64>()
                    .map_err(|_| invalid_data("invalid counter value"))?;

                (current, entry.expires_at_unix_ms)
            }

            None => (0, None),
        };

        let next = current
            .checked_add(delta)
            .ok_or_else(|| invalid_data("counter overflow during replay"))?;

        self.entries.insert(
            key.to_vec(),
            ReplayEntry {
                value: next.to_string().into_bytes(),
                expires_at_unix_ms,
            },
        );

        Ok(())
    }

    pub fn into_snapshot(self, last_sequence: u64, recovery_timestamp: u64) -> Snapshot {
        let entries = self
            .entries
            .into_iter()
            .filter(|(_, entry)| {
                entry
                    .expires_at_unix_ms
                    .is_none_or(|deadline| deadline > recovery_timestamp)
            })
            .map(|(key, entry)| SnapshotEntry {
                key,
                value: entry.value,
                expires_at_unix_ms: entry.expires_at_unix_ms,
            })
            .collect();

        Snapshot {
            version: SNAPSHOT_VERSION,
            last_sequence,
            entries,
        }
    }
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_request;

    fn apply(state: &mut ReplayState, request: Request, timestamp: u64) {
        let payload = encode_request(&request).unwrap();

        state.apply(&payload, timestamp).unwrap();
    }

    #[test]
    fn replay_recovers_counter_commands() {
        let mut state = ReplayState::new();
        let key = b"counter".to_vec();

        apply(
            &mut state,
            Request::Set {
                key: key.clone(),
                value: b"100".to_vec(),
            },
            1000,
        );

        apply(&mut state, Request::Increment { key: key.clone() }, 2000);

        apply(&mut state, Request::Increment { key }, 3000);

        let snapshot = state.into_snapshot(3, 4000);

        assert_eq!(snapshot.last_sequence, 3);
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].value, b"102");
    }

    #[test]
    fn replay_evaluates_historical_expiration() {
        let mut state = ReplayState::new();
        let key = b"counter".to_vec();

        apply(
            &mut state,
            Request::Set {
                key: key.clone(),
                value: b"100".to_vec(),
            },
            1000,
        );

        apply(
            &mut state,
            Request::Expire {
                key: key.clone(),
                seconds: 10,
            },
            2000,
        );

        // Key is still alive at timestamp 5000.
        apply(&mut state, Request::Increment { key: key.clone() }, 5000);

        // Key expired at timestamp 12000.
        // INCR must create a new counter.
        apply(&mut state, Request::Increment { key }, 13000);

        let snapshot = state.into_snapshot(4, 20000);

        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].value, b"1");
        assert_eq!(snapshot.entries[0].expires_at_unix_ms, None);
    }

    #[test]
    fn replay_preserves_absolute_expiration() {
        let mut state = ReplayState::new();
        let key = b"temporary".to_vec();

        apply(
            &mut state,
            Request::Set {
                key: key.clone(),
                value: b"hello".to_vec(),
            },
            1000,
        );

        apply(&mut state, Request::Expire { key, seconds: 60 }, 2000);

        let snapshot = state.into_snapshot(2, 3000);

        assert_eq!(snapshot.entries[0].expires_at_unix_ms, Some(62000));
    }

    #[test]
    fn expired_entries_are_removed_after_replay() {
        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            last_sequence: 10,
            entries: vec![SnapshotEntry {
                key: b"temporary".to_vec(),
                value: b"hello".to_vec(),
                expires_at_unix_ms: Some(5000),
            }],
        };

        let state = ReplayState::from_snapshot(snapshot).unwrap();

        let recovered = state.into_snapshot(10, 6000);

        assert!(recovered.entries.is_empty());
    }

    #[test]
    fn snapshot_entries_survive_until_historical_replay() {
        let key = b"counter".to_vec();

        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            last_sequence: 10,
            entries: vec![SnapshotEntry {
                key: key.clone(),
                value: b"100".to_vec(),
                expires_at_unix_ms: Some(5000),
            }],
        };

        let mut state = ReplayState::from_snapshot(snapshot).unwrap();

        apply(&mut state, Request::Increment { key }, 4000);

        let recovered = state.into_snapshot(11, 4500);

        assert_eq!(recovered.entries[0].value, b"101");

        assert_eq!(recovered.entries[0].expires_at_unix_ms, Some(5000));
    }

    #[test]
    fn replay_rejects_read_only_commands() {
        let mut state = ReplayState::new();

        let payload = encode_request(&Request::Ping).unwrap();

        let error = state.apply(&payload, 1000).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn replay_rejects_counter_overflow() {
        let mut state = ReplayState::new();
        let key = b"counter".to_vec();

        apply(
            &mut state,
            Request::Set {
                key: key.clone(),
                value: i64::MAX.to_string().into_bytes(),
            },
            1000,
        );

        let payload = encode_request(&Request::Increment { key }).unwrap();

        let error = state.apply(&payload, 2000).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }
}
