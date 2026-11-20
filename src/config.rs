//! Load local configuration from the process environment or optional `.env`.
//! Existing environment variables take priority over values from `.env`.

use std::env;

const DEFAULT_ADDR: &str = "127.0.0.1:6380";

pub fn load_address() -> String {
    // An .env file is optional in deployment and tests. dotenv() does not
    // replace environment variables that already exist in this process.
    let _ = dotenvy::dotenv();

    env::var("EMBERDB_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_owned())
}
