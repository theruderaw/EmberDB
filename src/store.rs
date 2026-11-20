//! Thread-safe, in-memory binary key/value storage.

use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug)]
pub enum StoreError {
    LockPoisoned,
}

#[derive(Default)]
pub struct Store {
    entries: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), StoreError> {
        let mut entries = self.entries.write().map_err(|_| StoreError::LockPoisoned)?;
        entries.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let entries = self.entries.read().map_err(|_| StoreError::LockPoisoned)?;
        Ok(entries.get(key).cloned())
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool, StoreError> {
        let mut entries = self.entries.write().map_err(|_| StoreError::LockPoisoned)?;
        Ok(entries.remove(key).is_some())
    }

    pub fn exists(&self, key: &[u8]) -> Result<bool, StoreError> {
        let entries = self.entries.read().map_err(|_| StoreError::LockPoisoned)?;
        Ok(entries.contains_key(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

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
}
