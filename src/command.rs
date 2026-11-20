//! Storage operations are separate from the binary command codec.
use crate::codec::{ErrorCode, Request, Response};
use crate::store::Store;

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
}
