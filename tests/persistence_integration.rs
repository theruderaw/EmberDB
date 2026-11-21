#![cfg(unix)]

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::json;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);

fn create_config(directory: &Path) -> (PathBuf, String) {
    // Reserve an available local port, then release it.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);

    let config_path = directory.join("config.json");
    let snapshot_path = directory.join("snapshot.json");

    let config = json!({
        "server": {
            "bind_addr": address,
            "max_clients": 16,
            "read_timeout_secs": 30,
            "write_timeout_secs": 5,
            "shutdown_timeout_secs": 5
        },
        "persistence": {
            "snapshot_path": snapshot_path
        }
    });

    fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();

    (config_path, address)
}

fn configure_process(command: &mut Command, config: &Path) {
    command
        .current_dir(config.parent().unwrap())
        .env("EMBERDB_CONFIG_PATH", config);

    // Prevent inherited environment overrides from
    // interfering with the test configuration.
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

fn wait_for_exit(child: &mut Child) -> ExitStatus {
    let deadline = Instant::now() + EXIT_TIMEOUT;

    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }

        assert!(
            Instant::now() < deadline,
            "EmberDB did not exit within {EXIT_TIMEOUT:?}"
        );

        thread::sleep(Duration::from_millis(25));
    }
}

struct TestServer {
    child: Child,
}

impl TestServer {
    fn spawn(config: &Path) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ember-server"));
        configure_process(&mut command, config);

        let child = command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start EmberDB");

        Self { child }
    }

    fn wait_until_ready(&mut self, address: &str) {
        let deadline = Instant::now() + STARTUP_TIMEOUT;

        loop {
            if TcpStream::connect(address).is_ok() {
                return;
            }

            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("EmberDB exited before becoming ready: {status}");
            }

            assert!(
                Instant::now() < deadline,
                "EmberDB did not start within {STARTUP_TIMEOUT:?}"
            );

            thread::sleep(Duration::from_millis(25));
        }
    }

    fn shutdown(&mut self) {
        let status = Command::new("kill")
            .arg("-INT")
            .arg(self.child.id().to_string())
            .status()
            .expect("failed to signal EmberDB");

        assert!(status.success(), "SIGINT delivery failed");

        let exit_status = wait_for_exit(&mut self.child);

        assert!(
            exit_status.success(),
            "EmberDB graceful shutdown failed: {exit_status}"
        );
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn run_cli(config: &Path, commands: &str) -> Vec<String> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ember-cli"));
    configure_process(&mut command, config);

    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start EmberDB CLI");

    {
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(commands.as_bytes()).unwrap();
    }

    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "CLI failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).unwrap();

    // Capture responses printed after the CLI prompt.
    stdout
        .lines()
        .filter_map(|line| line.strip_prefix("ember> "))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect()
}

#[test]
fn snapshot_survives_graceful_restart() {
    let directory = tempfile::tempdir().unwrap();
    let (config, address) = create_config(directory.path());

    // First server process.
    let mut server = TestServer::spawn(&config);
    server.wait_until_ready(&address);

    let responses = run_cli(
        &config,
        "\
SET username rudra
SET counter 100
INCR counter
SET expiring alive
EXPIRE expiring 120
SET expired soon
EXPIRE expired 1
QUIT
",
    );

    assert_eq!(
        responses,
        [
            "OK",
            "OK",
            "INTEGER 101",
            "OK",
            "INTEGER 1",
            "OK",
            "INTEGER 1",
        ]
    );

    // Trigger snapshot creation.
    server.shutdown();

    let snapshot_path = directory.path().join("snapshot.json");
    assert!(snapshot_path.is_file());

    let snapshot: serde_json::Value =
        serde_json::from_slice(&fs::read(&snapshot_path).unwrap()).unwrap();

    assert_eq!(snapshot["version"], 1);
    assert!(snapshot["entries"].is_array());

    // Ensure the short-lived key expires.
    thread::sleep(Duration::from_millis(1200));

    // A completely new server process must restore
    // the persisted database.
    let mut restored = TestServer::spawn(&config);
    restored.wait_until_ready(&address);

    let responses = run_cli(
        &config,
        "\
GET username
GET counter
TTL counter
GET expiring
TTL expiring
GET expired
QUIT
",
    );

    assert_eq!(responses.len(), 6, "{responses:?}");
    assert_eq!(responses[0], "VALUE rudra");
    assert_eq!(responses[1], "VALUE 101");
    assert_eq!(responses[2], "INTEGER -1");
    assert_eq!(responses[3], "VALUE alive");
    assert_eq!(responses[5], "NIL");

    // The future expiration must retain its remaining TTL.
    let remaining: i64 = responses[4]
        .strip_prefix("INTEGER ")
        .expect("TTL must return an integer")
        .parse()
        .unwrap();

    assert!(
        remaining > 0 && remaining <= 120,
        "invalid restored TTL: {remaining}"
    );

    restored.shutdown();
}

#[test]
fn corrupt_snapshot_prevents_server_startup() {
    let directory = tempfile::tempdir().unwrap();
    let (config, _address) = create_config(directory.path());

    let snapshot_path = directory.path().join("snapshot.json");

    // Write an intentionally malformed snapshot.
    fs::write(&snapshot_path, b"{ invalid json").unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_ember-server"));
    configure_process(&mut command, &config);

    let mut server = TestServer {
        child: command
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    };

    let status = wait_for_exit(&mut server.child);

    let mut stderr = String::new();
    server
        .child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();

    assert!(!status.success(), "server accepted corrupt snapshot");

    assert!(
        stderr.contains("invalid snapshot"),
        "unexpected startup error: {stderr}"
    );

    // Startup must not overwrite the damaged snapshot.
    assert_eq!(fs::read(&snapshot_path).unwrap(), b"{ invalid json");
}
