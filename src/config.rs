use std::env;

const DEFAULT_ADDR: &str = "127.0.0.1:6380";

/// Load EmberDB's network address.
///
/// Priority:
/// 1. Existing process environment
/// 2. Values from .env
/// 3. Built-in default

pub fn load_address() -> String {
    let _ = dotenvy::dotenv();

    env::var("EMBERDB_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string())
}
