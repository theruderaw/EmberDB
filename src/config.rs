//! Load local configuration from the process environment or optional `.env`.
//! Existing environment variables take priority over values from `.env`.

use std::env;
use std::io;
use std::time::Duration;

use dotenvy::dotenv;

const DEFAULT_ADDR: &str = "127.0.0.1:6380";

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub max_clients: usize,
}

pub fn load_address() -> String {
    // An .env file is optional in deployment and tests. dotenv() does not
    // replace environment variables that already exist in this process.
    let _ = dotenvy::dotenv();

    env::var("EMBERDB_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_owned())
}

fn read_duration(name: &str, default_seconds: u64) -> io::Result<Duration> {
    let value = match std::env::var(name) {
        Ok(value) => value,

        Err(std::env::VarError::NotPresent) => {
            return Ok(Duration::from_secs(default_seconds));
        }

        Err(error) => {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, error));
        }
    };

    let seconds = value.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be a positive integer"),
        )
    })?;

    if seconds == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be greater than zero"),
        ));
    }

    Ok(Duration::from_secs(seconds))
}

fn read_positive_usize(name: &str, default: usize) -> io::Result<usize> {
    let value = match std::env::var(name) {
        Ok(value) => value,

        Err(std::env::VarError::NotPresent) => {
            return Ok(default);
        }

        Err(error) => {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, error));
        }
    };

    let number = value.parse::<usize>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be a positive integer"),
        )
    })?;

    if number == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be greater than zero"),
        ));
    }

    Ok(number)
}

impl ServerConfig {
    pub fn from_env() -> io::Result<Self> {
        let _ = dotenv();

        Ok(Self {
            bind_addr: { load_address() },
            read_timeout: read_duration("EMBERDB_READ_TIMEOUT_SECS", 30)?,
            write_timeout: read_duration("EMBERDB_WRITE_TIMEOUT_SECS", 10)?,
            shutdown_timeout: read_duration("EMBERDB_SHUTDOWN_TIMEOUT_SECS", 10)?,
            max_clients: read_positive_usize("EMBERDB_MAX_CLIENTS", 128)?,
        })
    }
}
