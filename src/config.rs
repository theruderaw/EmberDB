use serde::Deserialize;
use serde::Serialize;
use std::env;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::time::Duration;

use crate::config::WalSyncMode::Always;

// Runtime configuration used by the TCP server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub max_clients: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub shutdown_timeout: Duration,
}

pub const WAL_CHECKPOINT_THRESHOLD_BYTES: u64 = 16 * 1024;
pub const WAL_MAX_SIZE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum WalSyncMode {
    #[default]
    Always,
    Group,
    Periodic,
    Os,
}

#[derive(Debug, Clone)]
pub struct GroupCommitConfig {
    pub max_batch_size: usize,
    pub max_wait: Duration,
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub path: PathBuf,
    pub sync_mode: WalSyncMode,
    pub sync_interval: Duration,
    pub group_commit: GroupCommitConfig,
}

// Runtime persistence configuration.
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub snapshot_path: PathBuf,
    pub wal: WalConfig,
}

// Root application configuration.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub persistence: PersistenceConfig,
}

// Raw JSON representations.
#[derive(Debug, Deserialize)]
struct ConfigFile {
    server: ServerSettings,
    persistence: PersistenceSettings,
}

#[derive(Debug, Deserialize)]
struct ServerSettings {
    bind_addr: String,
    max_clients: usize,
    read_timeout_secs: u64,
    write_timeout_secs: u64,
    shutdown_timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
struct PersistenceSettings {
    snapshot_path: PathBuf,

    #[serde(default)]
    wal: WalSettings,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WalSettings {
    path: Option<PathBuf>,
    sync_mode: WalSyncMode,
    sync_interval_ms: u64,
    group_commit: GroupCommitSettings,
}

impl Default for WalSettings {
    fn default() -> Self {
        Self {
            path: None,
            sync_mode: Always,
            sync_interval_ms: 1000,
            group_commit: GroupCommitSettings::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct GroupCommitSettings {
    max_batch_size: usize,
    max_wait_ms: u64,
}

impl Default for GroupCommitSettings {
    fn default() -> Self {
        Self {
            max_batch_size: 64,
            max_wait_ms: 5,
        }
    }
}

fn load_wal_config(persistence: &PersistenceSettings) -> io::Result<WalConfig> {
    let settings = &persistence.wal;

    let path = settings
        .path
        .clone()
        .unwrap_or_else(|| persistence.snapshot_path.with_file_name("wal.log"));
    if path.as_os_str().is_empty() {
        return Err(invalid_config("persistence.wal.path must not be empty"));
    }

    if path == persistence.snapshot_path {
        return Err(invalid_config("WAL and snapshot paths must be different"));
    }
    if settings.sync_interval_ms == 0 {
        return Err(invalid_config(
            "persistence.wal.sync_interval_ms must be greater than 0",
        ));
    }
    if settings.group_commit.max_batch_size == 0 {
        return Err(invalid_config(
            "persistence.wal.group_commit.max_batch_size must be greater than zero",
        ));
    }

    if settings.group_commit.max_wait_ms == 0 {
        return Err(invalid_config(
            "persistence.wal.group_commit.max_wait_ms must be greater than zero",
        ));
    }
    Ok(WalConfig {
        path,
        sync_mode: settings.sync_mode,
        sync_interval: Duration::from_millis(settings.sync_interval_ms),
        group_commit: GroupCommitConfig {
            max_batch_size: settings.group_commit.max_batch_size,
            max_wait: Duration::from_millis(settings.group_commit.max_wait_ms),
        },
    })
}

impl AppConfig {
    pub fn load() -> io::Result<Self> {
        // Optional .env for environment-specific settings.
        // Existing process environment variables take precedence.
        let _ = dotenvy::dotenv();

        // Default: config.json in the working directory.
        let path = env::var_os("EMBERDB_CONFIG_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("config.json"));

        let bytes = fs::read(&path)?;

        let file: ConfigFile = serde_json::from_slice(&bytes).map_err(|error| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("failed to parse {}: {error}", path.display()),
            )
        })?;

        let ConfigFile {
            server: settings,
            persistence,
        } = file;

        let bind_addr = env_override("EMBERDB_ADDR")?.unwrap_or(settings.bind_addr);

        if bind_addr.trim().is_empty() {
            return Err(invalid_config("server.bind_addr must not be empty"));
        }

        let max_clients = read_positive_usize("EMBERDB_MAX_CLIENTS", settings.max_clients)?;

        let read_timeout = read_duration("EMBERDB_READ_TIMEOUT_SECS", settings.read_timeout_secs)?;

        let write_timeout =
            read_duration("EMBERDB_WRITE_TIMEOUT_SECS", settings.write_timeout_secs)?;

        let shutdown_timeout = read_duration(
            "EMBERDB_SHUTDOWN_TIMEOUT_SECS",
            settings.shutdown_timeout_secs,
        )?;

        if persistence.snapshot_path.as_os_str().is_empty() {
            return Err(invalid_config(
                "persistence.snapshot_path must not be empty",
            ));
        }

        let wal = load_wal_config(&persistence)?;

        Ok(Self {
            server: ServerConfig {
                bind_addr,
                max_clients,
                read_timeout,
                write_timeout,
                shutdown_timeout,
            },

            persistence: PersistenceConfig {
                snapshot_path: persistence.snapshot_path,
                wal,
            },
        })
    }
}

// Read an optional environment override.
fn env_override(name: &str) -> io::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),

        Err(env::VarError::NotPresent) => Ok(None),

        Err(error) => Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("{name}: {error}"),
        )),
    }
}

// Parse a positive connection-count setting.
fn read_positive_usize(name: &str, default: usize) -> io::Result<usize> {
    let value = match env_override(name)? {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| invalid_config(format!("{name} must be a positive integer")))?,

        None => default,
    };

    if value == 0 {
        return Err(invalid_config(format!("{name} must be greater than zero")));
    }

    Ok(value)
}

// Parse a positive timeout setting in seconds.
fn read_duration(name: &str, default_seconds: u64) -> io::Result<Duration> {
    let seconds = match env_override(name)? {
        Some(raw) => raw
            .parse::<u64>()
            .map_err(|_| invalid_config(format!("{name} must be a positive integer")))?,

        None => default_seconds,
    };

    if seconds == 0 {
        return Err(invalid_config(format!("{name} must be greater than zero")));
    }

    Ok(Duration::from_secs(seconds))
}

fn invalid_config(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, message.into())
}
