use crate::store::Store;

pub fn execute(store: &Store, payload: &[u8]) -> Vec<u8> {
    let input = match std::str::from_utf8(payload) {
        Ok(input) => input,
        Err(_) => return b"ERR command must be UTF-8".to_vec(),
    };

    let words: Vec<&str> = input.split_whitespace().collect();

    match words.as_slice() {
        [cmd] if cmd.eq_ignore_ascii_case("PING") => b"PONG".to_vec(),

        [cmd, key, value] if cmd.eq_ignore_ascii_case("SET") => {
            match store.set(key.as_bytes().to_vec(), value.as_bytes().to_vec()) {
                Ok(()) => b"OK".to_vec(),
                Err(_) => storage_error(),
            }
        }
        [cmd, key] if cmd.eq_ignore_ascii_case("GET") => match store.get(key.as_bytes()) {
            Ok(Some(value)) => {
                let mut response = b"VALUE ".to_vec();
                response.extend_from_slice(&value);
                response
            }
            Ok(None) => b"NIL".to_vec(),
            Err(_) => storage_error(),
        },

        [cmd, key] if cmd.eq_ignore_ascii_case("DEL") => match store.delete(key.as_bytes()) {
            Ok(true) => b"INTEGER 1".to_vec(),
            Ok(false) => b"INTEGER 0".to_vec(),
            Err(_) => storage_error(),
        },

        [cmd, key] if cmd.eq_ignore_ascii_case("EXISTS") => match store.exists(key.as_bytes()) {
            Ok(true) => b"INTEGER 1".to_vec(),
            Ok(false) => b"INTEGER 0".to_vec(),
            Err(_) => storage_error(),
        },

        [] => b"ERR empty command".to_vec(),

        [cmd, ..]
            if ["PING", "SET", "GET", "DEL", "EXISTS"]
                .iter()
                .any(|known| cmd.eq_ignore_ascii_case(known)) =>
        {
            b"ERR invalid number of arguments".to_vec()
        }

        _ => b"ERR unknown command".to_vec(),
    }
}

fn storage_error() -> Vec<u8> {
    b"ERR internal storage error".to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_work() {
        let store = Store::new();

        assert_eq!(execute(&store, b"PING"), b"PONG");
        assert_eq!(execute(&store, b"GET name"), b"NIL");
        assert_eq!(execute(&store, b"SET name rudra"), b"OK");
        assert_eq!(execute(&store, b"GET name"), b"VALUE rudra");
        assert_eq!(execute(&store, b"EXISTS name"), b"INTEGER 1");
        assert_eq!(execute(&store, b"DEL name"), b"INTEGER 1");
        assert_eq!(execute(&store, b"GET name"), b"NIL");
    }

    #[test]
    fn invalid_commands() {
        let store = Store::new();

        assert_eq!(
            execute(&store, b"SET name"),
            b"ERR invalid number of arguments"
        );

        assert_eq!(execute(&store, b"UNKNOWN"), b"ERR unknown command");

        assert_eq!(execute(&store, &[255]), b"ERR command must be UTF-8");
    }
}
