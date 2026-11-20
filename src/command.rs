//! Storage operations are separate from the binary command codec.
use std::time::Duration;

use crate::codec::{ErrorCode, Request, Response};
use crate::store::{Store, StoreError, Ttl};

pub fn execute(store: &Store, request: Request) -> Response {
    match request {
        Request::Ping => Response::Pong,
        Request::Set { key, value } => match store.set(key, value) {
            Ok(()) => Response::Ok,
            Err(_) => Response::Error(ErrorCode::InternalServerError),
        },
        Request::Get { key } => match store.get(&key) {
            Ok(Some(value)) => Response::Value(value),
            Ok(None) => Response::Nil, // Missing key is a normal Redis-like lookup, not HTTP 404.
            Err(_) => Response::Error(ErrorCode::InternalServerError),
        },
        Request::Delete { key } => match store.delete(&key) {
            Ok(true) => Response::Integer(1),
            Ok(false) => Response::Integer(0),
            Err(_) => Response::Error(ErrorCode::InternalServerError),
        },
        Request::Exists { key } => match store.exists(&key) {
            Ok(true) => Response::Integer(1),
            Ok(false) => Response::Integer(0),
            Err(_) => Response::Error(ErrorCode::InternalServerError),
        },
        Request::Expire { key, seconds } => {
            let ttl = if seconds <= 0 {
                Duration::ZERO
            } else {
                Duration::from_secs(seconds as u64)
            };

            match store.expire(&key, ttl) {
                Ok(existed) => Response::Integer(i64::from(existed)),
                Err(StoreError::InvalidExpiration) => Response::Error(ErrorCode::BadRequest),
                Err(StoreError::LockPoisoned) => Response::Error(ErrorCode::InternalServerError),
            }
        }
        Request::Ttl { key } => match store.ttl(&key) {
            Ok(Ttl::Missing) => Response::Integer(-2),
            Ok(Ttl::Persistent) => Response::Integer(-1),
            Ok(Ttl::ExpiresIn(remaining)) => match i64::try_from(remaining.as_secs()) {
                Ok(seconds) => Response::Integer(seconds),
                Err(_) => Response::Error(ErrorCode::InternalServerError),
            },
            Err(_) => Response::Error(ErrorCode::InternalServerError),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_work() {
        let store = Store::new();
        assert_eq!(execute(&store, Request::Ping), Response::Pong);
        assert_eq!(
            execute(&store, Request::Get { key: b"x".to_vec() }),
            Response::Nil
        );
        assert_eq!(
            execute(
                &store,
                Request::Set {
                    key: b"x".to_vec(),
                    value: b"hello".to_vec()
                }
            ),
            Response::Ok
        );
        assert_eq!(
            execute(&store, Request::Get { key: b"x".to_vec() }),
            Response::Value(b"hello".to_vec())
        );
        assert_eq!(
            execute(&store, Request::Exists { key: b"x".to_vec() }),
            Response::Integer(1)
        );
        assert_eq!(
            execute(&store, Request::Delete { key: b"x".to_vec() }),
            Response::Integer(1)
        );
        assert_eq!(
            execute(&store, Request::Delete { key: b"x".to_vec() }),
            Response::Integer(0)
        );
    }

    #[test]
    fn binary_data_survives_storage() {
        let store = Store::new();
        let key = vec![0, 255, 10];
        let value = vec![128, 0, 32, 255];
        assert_eq!(
            execute(
                &store,
                Request::Set {
                    key: key.clone(),
                    value: value.clone()
                }
            ),
            Response::Ok
        );
        assert_eq!(
            execute(&store, Request::Get { key }),
            Response::Value(value)
        );
    }

    #[test]
    fn expire_command_updates_storage() {
        let store = Store::new();
        let key = b"session".to_vec();

        // Missing key.
        assert_eq!(
            execute(
                &store,
                Request::Expire {
                    key: key.clone(),
                    seconds: 60,
                },
            ),
            Response::Integer(0)
        );

        store.set(key.clone(), b"active".to_vec()).unwrap();

        // Assign a positive TTL.
        assert_eq!(
            execute(
                &store,
                Request::Expire {
                    key: key.clone(),
                    seconds: 60,
                },
            ),
            Response::Integer(1)
        );

        assert!(store.exists(&key).unwrap());

        // Negative TTL removes the key immediately.
        assert_eq!(
            execute(
                &store,
                Request::Expire {
                    key: key.clone(),
                    seconds: -10,
                },
            ),
            Response::Integer(1)
        );

        assert!(!store.exists(&key).unwrap());
    }

    #[test]
    fn ttl_command_reports_remaining_lifetime() {
        let store = Store::new();
        let key = b"temporary".to_vec();

        // Missing key
        assert_eq!(
            execute(&store, Request::Ttl { key: key.clone() }),
            Response::Integer(-2)
        );

        // Persistent key
        store.set(key.clone(), b"value".to_vec()).unwrap();

        assert_eq!(
            execute(&store, Request::Ttl { key: key.clone() }),
            Response::Integer(-1)
        );

        // Set expiration
        assert_eq!(
            execute(
                &store,
                Request::Expire {
                    key: key.clone(),
                    seconds: 60,
                }
            ),
            Response::Integer(1)
        );

        // Query remaining TTL
        match execute(&store, Request::Ttl { key: key.clone() }) {
            Response::Integer(seconds) => {
                assert!((0..=60).contains(&seconds));
            }
            other => panic!("Expected integer TTL, got {other:?}"),
        }

        // Expire immediately
        assert_eq!(
            execute(
                &store,
                Request::Expire {
                    key: key.clone(),
                    seconds: 0,
                }
            ),
            Response::Integer(1)
        );

        assert_eq!(execute(&store, Request::Ttl { key }), Response::Integer(-2));
    }
}
