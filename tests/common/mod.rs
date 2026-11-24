use serde_json::json;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

pub fn isolated_server_command(
    address: SocketAddr,
    max_clients: usize,
    read_timeout_secs: u64,
    write_timeout_secs: u64,
    shutdown_timeout_secs: u64,
) -> (TempDir, Command) {
    let directory = tempfile::tempdir().expect("failed to create test directory");

    let config_path = directory.path().join("config.json");
    let snapshot_path = directory.path().join("snapshot.json");

    let config = json!({
        "server": {
            "bind_addr": address.to_string(),
            "max_clients": max_clients,
            "read_timeout_secs": read_timeout_secs,
            "write_timeout_secs": write_timeout_secs,
            "shutdown_timeout_secs": shutdown_timeout_secs
        },
        "persistence": {
            "snapshot_path": snapshot_path
        }
    });

    fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap())
        .expect("failed to write test configuration");

    // Prevent dotenv from discovering a parent .env file.
    fs::write(directory.path().join(".env"), b"").expect("failed to create isolated .env");

    let mut command = Command::new(env!("CARGO_BIN_EXE_ember-server"));

    configure_process(&mut command, &config_path);

    (directory, command)
}

pub fn configure_process(command: &mut Command, config: &Path) {
    command
        .current_dir(config.parent().unwrap())
        .env("EMBERDB_CONFIG_PATH", config);

    for variable in [
        "EMBERDB_ADDR",
        "EMBERDB_MAX_CLIENTS",
        "EMBERDB_READ_TIMEOUT_SECS",
        "EMBERDB_WRITE_TIMEOUT_SECS",
        "EMBERDB_SHUTDOWN_TIMEOUT_SECS",
    ] {
        command.env_remove(variable);
    }
}
