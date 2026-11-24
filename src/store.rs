//! Thread-safe, in-memory binary key/value storage.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::persistence::{SNAPSHOT_VERSION, Snapshot, SnapshotEntry};

#[derive(Debug)]
pub enum StoreError {
    LockPoisoned,
    InvalidExpiration,
    InvalidInteger,
    IntegerOverflow,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RestoreError {
    UnsupportedVersion(u32),
    DuplicateKey,
    InvalidExpiration,
    LockPoisoned,
}

#[derive(Debug, Clone)]
struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    Missing,
    Persistent,
    ExpiresIn(Duration),
}

#[derive(Default)]
pub struct Store {
    entries: RwLock<HashMap<Vec<u8>, Entry>>,
}

impl Entry {
    fn is_expired(&self) -> bool {
        self.expires_at
            .is_some_and(|deadline| Instant::now() >= deadline)
    }
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), StoreError> {
        let mut entries = self.entries.write().map_err(|_| StoreError::LockPoisoned)?;
        entries.insert(
            key,
            Entry {
                value,
                expires_at: None,
            },
        );
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let mut entries = self.entries.write().map_err(|_| StoreError::LockPoisoned)?;

        if entries.get(key).is_some_and(|entry| entry.is_expired()) {
            entries.remove(key);
            return Ok(None);
        }
        Ok(entries.get(key).map(|entry| entry.value.clone()))
    }

    pub fn ttl(&self, key: &[u8]) -> Result<Ttl, StoreError> {
        let mut entries = self.entries.write().map_err(|_| StoreError::LockPoisoned)?;

        let now = Instant::now();

        let Some(entry) = entries.get(key) else {
            return Ok(Ttl::Missing);
        };

        let expires_at = entry.expires_at;

        match expires_at {
            None => Ok(Ttl::Persistent),

            Some(deadline) if deadline <= now => {
                entries.remove(key);
                Ok(Ttl::Missing)
            }

            Some(deadline) => Ok(Ttl::ExpiresIn(deadline.duration_since(now))),
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool, StoreError> {
        let mut entries = self.entries.write().map_err(|_| StoreError::LockPoisoned)?;
        Ok(entries.remove(key).is_some_and(|entry| !entry.is_expired()))
    }

    pub fn exists(&self, key: &[u8]) -> Result<bool, StoreError> {
        let mut entries = self.entries.write().map_err(|_| StoreError::LockPoisoned)?;

        if entries.get(key).is_some_and(|entry| entry.is_expired()) {
            entries.remove(key);
            return Ok(false);
        }

        Ok(entries.contains_key(key))
    }

    pub fn expire(&self, key: &[u8], ttl: Duration) -> Result<bool, StoreError> {
        let mut entries = self.entries.write().map_err(|_| StoreError::LockPoisoned)?;

        let Some(entry) = entries.get_mut(key) else {
            return Ok(false);
        };

        if entry.is_expired() {
            entries.remove(key);
            return Ok(false);
        }

        if ttl.is_zero() {
            entries.remove(key);
            return Ok(true);
        }

        let deadline = Instant::now()
            .checked_add(ttl)
            .ok_or(StoreError::InvalidExpiration)?;
        entry.expires_at = Some(deadline);
        Ok(true)
    }

    pub fn increment(&self, key: &[u8]) -> Result<i64, StoreError> {
        self.change_by(key, 1)
    }

    pub fn decrement(&self, key: &[u8]) -> Result<i64, StoreError> {
        self.change_by(key, -1)
    }

    pub fn snapshot(&self) -> Result<Snapshot, StoreError> {
        self.snapshot_at_sequence(0)
    }

    pub fn snapshot_at_sequence(&self, last_sequence: u64) -> Result<Snapshot, StoreError> {
        let entries = self.entries.read().map_err(|_| StoreError::LockPoisoned)?;

        let now = Instant::now();
        let wall_now = SystemTime::now();

        let mut snapshot_entries = Vec::with_capacity(entries.len());

        for (key, entry) in entries.iter() {
            let expires_at_unix_ms = match entry.expires_at {
                None => None,

                Some(deadline) => {
                    if deadline <= now {
                        continue;
                    }

                    let remaining = deadline.duration_since(now);

                    let absolute = wall_now
                        .checked_add(remaining)
                        .ok_or(StoreError::InvalidExpiration)?;

                    let milliseconds = absolute
                        .duration_since(UNIX_EPOCH)
                        .map_err(|_| StoreError::InvalidExpiration)?
                        .as_millis();

                    Some(u64::try_from(milliseconds).map_err(|_| StoreError::InvalidExpiration)?)
                }
            };

            snapshot_entries.push(SnapshotEntry {
                key: key.clone(),
                value: entry.value.clone(),
                expires_at_unix_ms,
            });
        }

        Ok(Snapshot {
            version: SNAPSHOT_VERSION,
            last_sequence,
            entries: snapshot_entries,
        })
    }

    pub fn restore(&self, snapshot: &Snapshot) -> Result<(), RestoreError> {
        // 1. Check snapshot compatibility.
        if snapshot.version != SNAPSHOT_VERSION {
            return Err(RestoreError::UnsupportedVersion(snapshot.version));
        }

        // 2. Capture the current wall-clock and monotonic times.
        let now_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RestoreError::InvalidExpiration)?
            .as_millis();

        let now = Instant::now();

        // 3. Build the replacement database independently.
        let mut restored = HashMap::with_capacity(snapshot.entries.len());
        let mut seen = HashSet::with_capacity(snapshot.entries.len());

        for item in &snapshot.entries {
            if !seen.insert(item.key.clone()) {
                return Err(RestoreError::DuplicateKey);
            }

            let expires_at = match item.expires_at_unix_ms {
                None => None,
                Some(timestamp) if u128::from(timestamp) <= now_unix_ms => {
                    continue;
                }
                Some(timestamp) => {
                    let remaining_ms = u64::try_from(u128::from(timestamp) - now_unix_ms)
                        .map_err(|_| RestoreError::InvalidExpiration)?;

                    let deadline = now
                        .checked_add(Duration::from_millis(remaining_ms))
                        .ok_or(RestoreError::InvalidExpiration)?;

                    Some(deadline)
                }
            };

            restored.insert(
                item.key.clone(),
                Entry {
                    value: item.value.clone(),
                    expires_at,
                },
            );
        }

        let mut entries = self
            .entries
            .write()
            .map_err(|_| RestoreError::LockPoisoned)?;

        *entries = restored;

        Ok(())
    }

    fn change_by(&self, key: &[u8], delta: i64) -> Result<i64, StoreError> {
        let mut entries = self.entries.write().map_err(|_| StoreError::LockPoisoned)?;

        let (current, expires_at) = match entries.get(key).filter(|entry| !entry.is_expired()) {
            Some(entry) => {
                let text =
                    std::str::from_utf8(&entry.value).map_err(|_| StoreError::InvalidInteger)?;

                let number = text
                    .parse::<i64>()
                    .map_err(|_| StoreError::InvalidInteger)?;

                (number, entry.expires_at)
            }

            None => (0, None),
        };

        let next = current
            .checked_add(delta)
            .ok_or(StoreError::IntegerOverflow)?;

        entries.insert(
            key.to_vec(),
            Entry {
                value: next.to_string().into_bytes(),
                expires_at,
            },
        );

        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn basic_operations() {
        let store = Store::new();
        assert_eq!(store.get(b"name").unwrap(), None);
        store.set(b"name".to_vec(), b"rudra".to_vec()).unwrap();
        assert_eq!(store.get(b"name").unwrap(), Some(b"rudra".to_vec()));
        assert!(store.exists(b"name").unwrap());
        assert!(store.delete(b"name").unwrap());
        assert!(!store.delete(b"name").unwrap());
        assert!(!store.exists(b"name").unwrap());
    }

    #[test]
    fn overwrite_existing_key() {
        let store = Store::new();
        store.set(b"key".to_vec(), b"old".to_vec()).unwrap();
        store.set(b"key".to_vec(), b"new".to_vec()).unwrap();
        assert_eq!(store.get(b"key").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn binary_values_and_keys() {
        let store = Store::new();
        let key = vec![0, 255];
        let value = vec![0, 255, 10, 128];
        store.set(key.clone(), value.clone()).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn expire_sets_and_removes_ttl() {
        let store = Store::new();

        store.set(b"session".to_vec(), b"active".to_vec()).unwrap();

        // Assign a future deadline.
        assert!(store.expire(b"session", Duration::from_secs(60)).unwrap());

        assert_eq!(store.get(b"session").unwrap(), Some(b"active".to_vec()));

        // Zero TTL expires the key immediately.
        assert!(store.expire(b"session", Duration::ZERO).unwrap());

        assert_eq!(store.get(b"session").unwrap(), None);

        // The key no longer exists.
        assert!(!store.expire(b"session", Duration::from_secs(60)).unwrap());
    }
    #[test]
    fn expired_entries_are_hidden_from_get() {
        let store = Store::new();

        {
            let mut entries = store.entries.write().unwrap();

            entries.insert(
                b"expired".to_vec(),
                Entry {
                    value: b"old".to_vec(),
                    expires_at: Some(Instant::now() - Duration::from_secs(1)),
                },
            );

            entries.insert(
                b"active".to_vec(),
                Entry {
                    value: b"new".to_vec(),
                    expires_at: Some(Instant::now() + Duration::from_secs(60)),
                },
            );
        }

        assert_eq!(store.get(b"expired").unwrap(), None);

        assert_eq!(store.get(b"active").unwrap(), Some(b"new".to_vec()));
        let entries = store.entries.read().unwrap();

        assert!(!entries.contains_key(b"expired".as_slice()));
        assert!(entries.contains_key(b"active".as_slice()));
    }

    #[test]
    fn concurrent_writes() {
        let store = Arc::new(Store::new());
        let mut handles = Vec::new();

        for worker in 0..8 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for index in 0..100 {
                    let key = format!("key:{worker}:{index}");
                    store.set(key.into_bytes(), b"value".to_vec()).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for worker in 0..8 {
            for index in 0..100 {
                let key = format!("key:{worker}:{index}");
                assert_eq!(store.get(key.as_bytes()).unwrap(), Some(b"value".to_vec()));
            }
        }
    }

    #[test]
    fn expired_entries_are_missing_for_exists_and_delete() {
        let store = Store::new();

        {
            let mut entries = store.entries.write().unwrap();

            entries.insert(
                b"expired".to_vec(),
                Entry {
                    value: b"old".to_vec(),
                    expires_at: Some(Instant::now() - Duration::from_secs(1)),
                },
            );
        }

        // Expired keys are logically absent.
        assert!(!store.exists(b"expired").unwrap());

        // DELETE removes the stale entry but reports false.
        assert!(!store.delete(b"expired").unwrap());

        // Confirm physical cleanup.
        let entries = store.entries.read().unwrap();
        assert!(!entries.contains_key(b"expired".as_slice()));
    }

    #[test]
    fn exists_removes_expired_entries() {
        let store = Store::new();

        store.set(b"temporary".to_vec(), b"data".to_vec()).unwrap();

        // Give the key a zero-duration expiration.
        // Our expire() implementation deletes it immediately.
        assert!(store.expire(b"temporary", Duration::ZERO).unwrap());
        assert!(!store.exists(b"temporary").unwrap());

        // Insert an already-expired entry to test lazy cleanup.
        {
            let mut entries = store.entries.write().unwrap();

            entries.insert(
                b"stale".to_vec(),
                Entry {
                    value: b"old".to_vec(),
                    expires_at: Some(Instant::now() - Duration::from_secs(1)),
                },
            );
        }

        assert!(!store.exists(b"stale").unwrap());

        // EXISTS must have physically removed the expired entry.
        let entries = store.entries.read().unwrap();
        assert!(!entries.contains_key(b"stale".as_slice()));
    }

    #[test]
    fn ttl_reports_key_lifetime() {
        let store = Store::new();

        // Missing key.
        assert_eq!(store.ttl(b"session").unwrap(), Ttl::Missing);

        store.set(b"session".to_vec(), b"active".to_vec()).unwrap();

        // Key without expiration.
        assert_eq!(store.ttl(b"session").unwrap(), Ttl::Persistent);

        // Assign a 60-second expiration.
        assert!(store.expire(b"session", Duration::from_secs(60),).unwrap());

        match store.ttl(b"session").unwrap() {
            Ttl::ExpiresIn(remaining) => {
                assert!(remaining > Duration::ZERO);
                assert!(remaining <= Duration::from_secs(60));
            }
            other => panic!("Expected expiring key, got {other:?}"),
        }

        // Expire immediately.
        assert!(store.expire(b"session", Duration::ZERO,).unwrap());

        assert_eq!(store.ttl(b"session").unwrap(), Ttl::Missing);
    }

    #[test]
    fn ttl_removes_expired_entries() {
        let store = Store::new();

        // Insert an entry that expired one second ago.
        {
            let mut entries = store.entries.write().unwrap();

            entries.insert(
                b"expired".to_vec(),
                Entry {
                    value: b"old".to_vec(),
                    expires_at: Some(Instant::now() - Duration::from_secs(1)),
                },
            );
        }

        // An expired key must behave like a missing key.
        assert_eq!(store.ttl(b"expired").unwrap(), Ttl::Missing);

        // Verify that TTL also cleaned up the HashMap.
        let entries = store.entries.read().unwrap();

        assert!(!entries.contains_key(b"expired".as_slice()));
    }

    #[test]
    fn expire_does_not_revive_expired_keys() {
        let store = Store::new();

        // Insert an already-expired entry.
        {
            let mut entries = store.entries.write().unwrap();

            entries.insert(
                b"stale".to_vec(),
                Entry {
                    value: b"old".to_vec(),
                    expires_at: Some(Instant::now() - Duration::from_secs(1)),
                },
            );
        }

        // EXPIRE must not revive it.
        assert!(!store.expire(b"stale", Duration::from_secs(60)).unwrap());

        assert_eq!(store.get(b"stale").unwrap(), None);

        // Verify physical cleanup.
        let entries = store.entries.read().unwrap();
        assert!(!entries.contains_key(b"stale".as_slice()));
    }

    #[test]
    fn increment_updates_integer_atomically() {
        let store = Store::new();

        assert_eq!(store.increment(b"counter").unwrap(), 1);
        assert_eq!(store.increment(b"counter").unwrap(), 2);

        assert_eq!(store.get(b"counter").unwrap(), Some(b"2".to_vec()));

        store.set(b"counter".to_vec(), b"100".to_vec()).unwrap();

        assert_eq!(store.increment(b"counter").unwrap(), 101);

        store.set(b"counter".to_vec(), b"hello".to_vec()).unwrap();

        assert!(matches!(
            store.increment(b"counter"),
            Err(StoreError::InvalidInteger)
        ));

        // Invalid increments must not overwrite the original value.
        assert_eq!(store.get(b"counter").unwrap(), Some(b"hello".to_vec()));
    }

    #[test]
    fn decrement_and_integer_boundaries() {
        let store = Store::new();

        // A missing key starts at zero.
        assert_eq!(store.decrement(b"counter").unwrap(), -1);
        assert_eq!(store.decrement(b"counter").unwrap(), -2);
        assert_eq!(store.increment(b"counter").unwrap(), -1);

        // Maximum integer cannot be incremented.
        store
            .set(b"counter".to_vec(), i64::MAX.to_string().into_bytes())
            .unwrap();

        assert!(matches!(
            store.increment(b"counter"),
            Err(StoreError::IntegerOverflow)
        ));

        assert_eq!(
            store.get(b"counter").unwrap(),
            Some(i64::MAX.to_string().into_bytes())
        );

        // Minimum integer cannot be decremented.
        store
            .set(b"counter".to_vec(), i64::MIN.to_string().into_bytes())
            .unwrap();

        assert!(matches!(
            store.decrement(b"counter"),
            Err(StoreError::IntegerOverflow)
        ));

        assert_eq!(
            store.get(b"counter").unwrap(),
            Some(i64::MIN.to_string().into_bytes())
        );
    }

    #[test]
    fn snapshot_exports_live_entries() {
        let store = Store::new();

        store
            .set(b"persistent".to_vec(), b"hello".to_vec())
            .unwrap();

        store.set(b"temporary".to_vec(), b"world".to_vec()).unwrap();

        store.expire(b"temporary", Duration::from_secs(60)).unwrap();

        // Insert an already-expired entry.
        {
            let mut entries = store.entries.write().unwrap();

            entries.insert(
                b"stale".to_vec(),
                Entry {
                    value: b"old".to_vec(),
                    expires_at: Some(Instant::now() - Duration::from_secs(1)),
                },
            );
        }

        let snapshot = store.snapshot().unwrap();

        assert_eq!(snapshot.version, SNAPSHOT_VERSION);
        assert_eq!(snapshot.entries.len(), 2);

        let persistent = snapshot
            .entries
            .iter()
            .find(|entry| entry.key == b"persistent")
            .unwrap();

        assert_eq!(persistent.value, b"hello");
        assert_eq!(persistent.expires_at_unix_ms, None);

        let temporary = snapshot
            .entries
            .iter()
            .find(|entry| entry.key == b"temporary")
            .unwrap();

        assert_eq!(temporary.value, b"world");
        assert!(temporary.expires_at_unix_ms.is_some());

        // Expired entries must not enter the snapshot.
        assert!(!snapshot.entries.iter().any(|entry| entry.key == b"stale"));
    }

    #[test]
    fn restore_preserves_remaining_ttl() {
        let store = Store::new();

        // Create an expiration timestamp 60 seconds
        // into the future, expressed as Unix milliseconds.
        let expires_at = SystemTime::now()
            .checked_add(Duration::from_secs(60))
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            entries: vec![SnapshotEntry {
                key: b"temporary".to_vec(),
                value: b"hello".to_vec(),
                expires_at_unix_ms: Some(u64::try_from(expires_at).unwrap()),
            }],

            last_sequence: 0,
        };

        store.restore(&snapshot).unwrap();

        assert_eq!(store.get(b"temporary").unwrap(), Some(b"hello".to_vec()));

        match store.ttl(b"temporary").unwrap() {
            Ttl::ExpiresIn(remaining) => {
                assert!(remaining > Duration::from_secs(55));
                assert!(remaining <= Duration::from_secs(60));
            }
            other => panic!("Expected an expiring key, got {other:?}"),
        }
    }

    #[test]
    fn invalid_snapshot_preserves_existing_data() {
        let store = Store::new();

        store.set(b"original".to_vec(), b"safe".to_vec()).unwrap();

        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            entries: vec![
                SnapshotEntry {
                    key: b"duplicate".to_vec(),
                    value: b"first".to_vec(),
                    expires_at_unix_ms: None,
                },
                SnapshotEntry {
                    key: b"duplicate".to_vec(),
                    value: b"second".to_vec(),
                    expires_at_unix_ms: None,
                },
            ],
            last_sequence: 0,
        };

        assert_eq!(store.restore(&snapshot), Err(RestoreError::DuplicateKey));

        // The original database must remain unchanged.
        assert_eq!(store.get(b"original").unwrap(), Some(b"safe".to_vec()));

        assert_eq!(store.get(b"duplicate").unwrap(), None);
    }
    #[test]
    fn restore_replaces_existing_data() {
        let store = Store::new();

        store.set(b"old".to_vec(), b"previous".to_vec()).unwrap();

        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            entries: vec![
                SnapshotEntry {
                    key: b"counter".to_vec(),
                    value: b"42".to_vec(),
                    expires_at_unix_ms: None,
                },
                SnapshotEntry {
                    key: b"expired".to_vec(),
                    value: b"stale".to_vec(),
                    expires_at_unix_ms: Some(1),
                },
            ],
            last_sequence: 0,
        };

        store.restore(&snapshot).unwrap();

        // The snapshot replaces the old database.
        assert_eq!(store.get(b"old").unwrap(), None);

        // Persistent entries are restored.
        assert_eq!(store.get(b"counter").unwrap(), Some(b"42".to_vec()));

        // Expired entries are skipped.
        assert_eq!(store.get(b"expired").unwrap(), None);
    }
    #[test]
    fn snapshot_captures_wal_sequence() {
        let store = Store::new();

        store.set(b"counter".to_vec(), b"100".to_vec()).unwrap();

        let snapshot = store.snapshot_at_sequence(42).unwrap();

        assert_eq!(snapshot.version, SNAPSHOT_VERSION);
        assert_eq!(snapshot.last_sequence, 42);
        assert_eq!(snapshot.entries.len(), 1);
    }
}
