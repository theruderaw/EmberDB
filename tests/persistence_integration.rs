#![cfg(unix)]

use emberdb::codec::{Request, encode_request};
use emberdb::persistence::load_snapshot;
use emberdb::wal::record::{RecordKind, WalRecord};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};
mod common;

use common::{configure_process, isolated_server_command};
use tempfile::TempDir;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);

fn create_server() -> (TempDir, Command, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let (directory, command) = isolated_server_command(address, 16, 30, 5, 5);

    (directory, command, address.to_string())
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
    fn spawn(mut command: Command) -> Self {
        let child = command
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
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
    let (directory, command, address) = create_server();
    let config = directory.path().join("config.json");

    // First server process.
    let mut server = TestServer::spawn(command);
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

    assert_eq!(snapshot["version"], emberdb::version::SNAPSHOT_VERSION);

    assert_eq!(snapshot["last_sequence"], 0);
    assert!(snapshot["entries"].is_array());

    // Ensure the short-lived key expires.
    thread::sleep(Duration::from_millis(1200));

    // A completely new server process must restore
    // the persisted database.
    let mut command = Command::new(env!("CARGO_BIN_EXE_ember-server"));

    configure_process(&mut command, &config);

    let mut restored = TestServer::spawn(command);
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
    let (directory, mut command, _address) = create_server();

    let snapshot_path = directory.path().join("snapshot.json");

    // Write an intentionally malformed snapshot.
    fs::write(&snapshot_path, b"{ invalid json").unwrap();

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

#[test]
fn wal_recovery_survives_restart_without_duplicate_replay() {
    let (directory, command, address) = create_server();

    let config = directory.path().join("config.json");
    let snapshot_path = directory.path().join("snapshot.json");
    let wal_path = directory.path().join("wal.log");

    let requests = [
        Request::Set {
            key: b"counter".to_vec(),
            value: b"10".to_vec(),
        },
        Request::Increment {
            key: b"counter".to_vec(),
        },
    ];

    let mut wal_bytes = Vec::new();

    for (index, request) in requests.iter().enumerate() {
        let record = WalRecord {
            kind: RecordKind::Command,
            sequence: index as u64 + 1,
            timestamp_unix_ms: 1000 + index as u64,
            transaction_id: 0,
            payload: encode_request(request).unwrap(),
        };

        wal_bytes.extend_from_slice(&record.encode().unwrap());
    }

    fs::write(&wal_path, &wal_bytes).unwrap();

    // First startup: recover from the WAL alone.
    let mut server = TestServer::spawn(command);
    server.wait_until_ready(&address);

    assert_eq!(run_cli(&config, "GET counter\nQUIT\n"), ["VALUE 11"],);

    server.shutdown();

    // Graceful shutdown must preserve the recovered sequence.
    let snapshot = load_snapshot(&snapshot_path).unwrap().unwrap();

    assert_eq!(snapshot.last_sequence, 2);
    assert_eq!(snapshot.entries.len(), 1);
    assert_eq!(snapshot.entries[0].key, b"counter");
    assert_eq!(snapshot.entries[0].value, b"11");

    // Recovery must not modify the WAL.
    assert_eq!(fs::read(&wal_path).unwrap(), wal_bytes,);

    // Second startup: snapshot + previously replayed WAL.
    let mut command = Command::new(env!("CARGO_BIN_EXE_ember-server"));

    configure_process(&mut command, &config);

    let mut restored = TestServer::spawn(command);
    restored.wait_until_ready(&address);

    assert_eq!(run_cli(&config, "GET counter\nQUIT\n"), ["VALUE 11"],);

    restored.shutdown();

    let snapshot = load_snapshot(&snapshot_path).unwrap().unwrap();

    assert_eq!(snapshot.last_sequence, 2);
    assert_eq!(snapshot.entries[0].value, b"11");
}
