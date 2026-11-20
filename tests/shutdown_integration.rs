#![cfg(unix)]

//! End-to-end shutdown tests for EmberDB.
//! Uses POSIX SIGINT to simulate Ctrl+C.

use emberdb::codec::{Request, Response, decode_response, encode_request};
use emberdb::protocol::{read_frame, write_frame};

use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(20);

struct TestServer {
    child: Child,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl TestServer {
    fn wait_for_exit(&mut self, timeout: Duration) -> (ExitStatus, String, String) {
        let started = Instant::now();

        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                let mut stdout = String::new();
                let mut stderr = String::new();

                self.child
                    .stdout
                    .take()
                    .unwrap()
                    .read_to_string(&mut stdout)
                    .unwrap();

                self.child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut stderr)
                    .unwrap();

                return (status, stdout, stderr);
            }

            assert!(
                started.elapsed() < timeout,
                "EmberDB failed to exit within {timeout:?}"
            );

            thread::sleep(POLL_INTERVAL);
        }
    }

    fn interrupt(&self) {
        // Linux/Unix equivalent of pressing Ctrl+C.
        let status = Command::new("kill")
            .arg("-INT")
            .arg(self.child.id().to_string())
            .status()
            .expect("failed to execute kill");

        assert!(status.success(), "failed to send SIGINT");
    }
}

fn start_server(read_timeout_secs: u64, shutdown_timeout_secs: u64) -> (TestServer, TcpStream) {
    // Ask the OS for an available TCP port.
    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let address: SocketAddr = probe.local_addr().unwrap();
    drop(probe);

    let child = Command::new(env!("CARGO_BIN_EXE_ember-server"))
        .env("EMBERDB_ADDR", address.to_string())
        .env("EMBERDB_MAX_CLIENTS", "8")
        .env("EMBERDB_READ_TIMEOUT_SECS", read_timeout_secs.to_string())
        .env("EMBERDB_WRITE_TIMEOUT_SECS", "5")
        .env(
            "EMBERDB_SHUTDOWN_TIMEOUT_SECS",
            shutdown_timeout_secs.to_string(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start EmberDB");

    let mut server = TestServer { child };

    // Don't just verify that TCP connect succeeds:
    // complete a PING/PONG exchange to prove that
    // the connection has an active server-side worker.
    let ping = encode_request(&Request::Ping).unwrap();

    for _ in 0..150 {
        if let Some(status) = server.child.try_wait().unwrap() {
            panic!("EmberDB exited during startup: {status}");
        }

        if let Ok(mut client) = TcpStream::connect_timeout(&address, Duration::from_millis(200)) {
            client
                .set_read_timeout(Some(Duration::from_millis(500)))
                .unwrap();

            client
                .set_write_timeout(Some(Duration::from_millis(500)))
                .unwrap();

            if write_frame(&mut client, &ping).is_ok()
                && let Ok(Some(payload)) = read_frame(&mut client)
                && decode_response(&payload) == Ok(Response::Pong)
            {
                return (server, client);
            }
        }

        thread::sleep(POLL_INTERVAL);
    }

    panic!("EmberDB did not become ready");
}

#[test]
fn graceful_shutdown_waits_for_client_disconnect() {
    let (mut server, client) = start_server(30, 5);

    server.interrupt();

    // The server must not exit while a client
    // is still connected and the grace period remains.
    thread::sleep(Duration::from_millis(150));

    assert!(
        server.child.try_wait().unwrap().is_none(),
        "server exited before client disconnected"
    );

    // Simulate the client disconnecting normally.
    drop(client);

    let (status, stdout, stderr) = server.wait_for_exit(Duration::from_secs(7));

    assert!(status.success(), "server exited abnormally: {stderr}");

    assert!(
        stdout.contains("EmberDB stopped accepting connections"),
        "{stdout}"
    );

    assert!(stdout.contains("All client connections closed"), "{stdout}");

    assert!(stdout.contains("EmberDB shutdown complete"), "{stdout}");

    assert!(!stderr.contains("Shutdown deadline exceeded"), "{stderr}");
}

#[test]
fn shutdown_deadline_terminates_with_active_client() {
    // Read timeout is much longer than shutdown deadline.
    let (mut server, client) = start_server(30, 1);

    let started = Instant::now();
    server.interrupt();

    // Keep the TCP connection alive deliberately.
    let (status, stdout, stderr) = server.wait_for_exit(Duration::from_secs(5));

    assert!(status.success(), "server exited abnormally: {stderr}");

    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown exceeded expected deadline"
    );

    assert!(
        stdout.contains("EmberDB stopped accepting connections"),
        "{stdout}"
    );

    assert!(stderr.contains("Shutdown deadline exceeded"), "{stderr}");

    assert!(
        stderr.contains("Exiting with active connections"),
        "{stderr}"
    );

    assert!(stdout.contains("EmberDB shutdown complete"), "{stdout}");

    drop(client);
}
