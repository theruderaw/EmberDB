//! End-to-end atomic-counter tests for EmberDB's binary TCP protocol.
//!
//! Every test launches a real ember-server on its own ephemeral loopback port.
//! No external database or running server is required.
mod common;

use common::isolated_server_command;
use tempfile::TempDir;

use emberdb::codec::{ErrorCode, Request, Response, decode_response, encode_request};
use emberdb::protocol::{read_frame, write_frame};

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const STARTUP_IO_TIMEOUT: Duration = Duration::from_millis(500);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_DELAY: Duration = Duration::from_millis(20);

struct TestServer {
    child: Child,
    _directory: TempDir,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn set_timeouts(stream: &TcpStream, timeout: Duration) {
    stream
        .set_read_timeout(Some(timeout))
        .expect("failed to set client read timeout");
    stream
        .set_write_timeout(Some(timeout))
        .expect("failed to set client write timeout");
}

fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))
        .expect("failed to connect to ember-server");
    set_timeouts(&stream, IO_TIMEOUT);
    stream
}

fn request(stream: &mut TcpStream, command: Request) -> Response {
    let payload = encode_request(&command).expect("failed to encode request");
    write_frame(stream, &payload).expect("failed to send request");

    let payload = read_frame(stream)
        .expect("failed to read response")
        .expect("server closed the connection unexpectedly");
    decode_response(&payload).expect("server returned an invalid binary response")
}

fn start_server() -> (TestServer, SocketAddr) {
    // Reserve a free OS-assigned port, then release it for ember-server.
    let probe = TcpListener::bind("127.0.0.1:0").expect("failed to allocate test port");
    let address = probe.local_addr().expect("failed to obtain test address");
    drop(probe);

    let (directory, mut command) = isolated_server_command(address, 32, 30, 10, 2);

    let child = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start ember-server");

    let mut server = TestServer {
        child,
        _directory: directory,
    };
    let ping = encode_request(&Request::Ping).expect("PING must encode");

    // A TCP handshake alone does not prove that a worker admitted the client.
    // Wait for a successful binary PING/PONG exchange instead.
    for _ in 0..150 {
        if let Some(status) = server
            .child
            .try_wait()
            .expect("failed to check server status")
        {
            panic!("ember-server exited during startup: {status}");
        }

        if let Ok(mut client) = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) {
            set_timeouts(&client, STARTUP_IO_TIMEOUT);

            if write_frame(&mut client, &ping).is_ok()
                && let Ok(Some(payload)) = read_frame(&mut client)
                && decode_response(&payload) == Ok(Response::Pong)
            {
                return (server, address);
            }
        }

        thread::sleep(RETRY_DELAY);
    }

    panic!("ember-server did not become ready");
}

#[test]
fn concurrent_tcp_increments_have_no_lost_or_duplicate_results() {
    const WORKERS: usize = 8;
    const INCREMENTS_PER_WORKER: usize = 100;

    let (_server, address) = start_server();
    let mut admin = connect(address);
    let key = b"shared-counter".to_vec();

    assert_eq!(
        request(
            &mut admin,
            Request::Set {
                key: key.clone(),
                value: b"0".to_vec(),
            },
        ),
        Response::Ok,
    );

    // Open every connection and verify admission before releasing the workers.
    // This keeps the test focused on atomic counter behavior, not startup races.
    let mut clients = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let mut client = connect(address);
        assert_eq!(request(&mut client, Request::Ping), Response::Pong);
        clients.push(client);
    }

    let start = Arc::new(Barrier::new(WORKERS + 1));
    let mut handles = Vec::with_capacity(WORKERS);

    for mut client in clients {
        let start = Arc::clone(&start);
        let key = key.clone();

        handles.push(thread::spawn(move || {
            start.wait();

            let mut results = Vec::with_capacity(INCREMENTS_PER_WORKER);
            for _ in 0..INCREMENTS_PER_WORKER {
                match request(&mut client, Request::Increment { key: key.clone() }) {
                    Response::Integer(value) => results.push(value),
                    other => panic!("INCR returned unexpected response: {other:?}"),
                }
            }
            results
        }));
    }

    // Let all eight workers begin issuing requests at approximately the same time.
    start.wait();

    let mut returned_values = Vec::with_capacity(WORKERS * INCREMENTS_PER_WORKER);
    for handle in handles {
        returned_values.extend(handle.join().expect("counter worker panicked"));
    }

    let expected_total = i64::try_from(WORKERS * INCREMENTS_PER_WORKER).unwrap();
    returned_values.sort_unstable();

    // Not only must the final value be correct: every intermediate result must
    // appear exactly once, demonstrating a serialized increment operation.
    assert_eq!(returned_values, (1..=expected_total).collect::<Vec<i64>>());
    assert_eq!(
        request(&mut admin, Request::Get { key }),
        Response::Value(expected_total.to_string().into_bytes()),
    );
}

#[test]
fn decrement_and_signed_boundaries_work_over_tcp() {
    let (_server, address) = start_server();
    let mut client = connect(address);
    let key = b"signed-counter".to_vec();

    assert_eq!(
        request(&mut client, Request::Decrement { key: key.clone() }),
        Response::Integer(-1),
    );
    assert_eq!(
        request(&mut client, Request::Increment { key: key.clone() }),
        Response::Integer(0),
    );

    assert_eq!(
        request(
            &mut client,
            Request::Set {
                key: key.clone(),
                value: i64::MAX.to_string().into_bytes(),
            },
        ),
        Response::Ok,
    );
    assert_eq!(
        request(&mut client, Request::Increment { key: key.clone() }),
        Response::Error(ErrorCode::BadRequest),
    );
    assert_eq!(
        request(&mut client, Request::Get { key: key.clone() }),
        Response::Value(i64::MAX.to_string().into_bytes()),
    );

    assert_eq!(
        request(
            &mut client,
            Request::Set {
                key: key.clone(),
                value: i64::MIN.to_string().into_bytes(),
            },
        ),
        Response::Ok,
    );
    assert_eq!(
        request(&mut client, Request::Decrement { key: key.clone() }),
        Response::Error(ErrorCode::BadRequest),
    );
    assert_eq!(
        request(&mut client, Request::Get { key }),
        Response::Value(i64::MIN.to_string().into_bytes()),
    );
    assert_eq!(request(&mut client, Request::Ping), Response::Pong);
}

#[test]
fn invalid_counter_values_are_rejected_without_mutation() {
    let (_server, address) = start_server();
    let mut client = connect(address);
    let key = b"invalid-counter".to_vec();

    for invalid in [
        b"hello".to_vec(),
        vec![0xFF, 0x00],
        b"9223372036854775808".to_vec(),
    ] {
        assert_eq!(
            request(
                &mut client,
                Request::Set {
                    key: key.clone(),
                    value: invalid.clone(),
                },
            ),
            Response::Ok,
        );

        assert_eq!(
            request(&mut client, Request::Increment { key: key.clone() }),
            Response::Error(ErrorCode::BadRequest),
        );
        assert_eq!(
            request(&mut client, Request::Decrement { key: key.clone() }),
            Response::Error(ErrorCode::BadRequest),
        );
        assert_eq!(
            request(&mut client, Request::Get { key: key.clone() }),
            Response::Value(invalid),
        );
    }

    assert_eq!(request(&mut client, Request::Ping), Response::Pong);
}

#[test]
fn counter_updates_preserve_ttl_and_new_keys_are_persistent() {
    let (_server, address) = start_server();
    let mut client = connect(address);
    let key = b"expiring-counter".to_vec();

    assert_eq!(
        request(
            &mut client,
            Request::Set {
                key: key.clone(),
                value: b"40".to_vec(),
            },
        ),
        Response::Ok,
    );
    assert_eq!(
        request(
            &mut client,
            Request::Expire {
                key: key.clone(),
                seconds: 60,
            },
        ),
        Response::Integer(1),
    );
    assert_eq!(
        request(&mut client, Request::Increment { key: key.clone() }),
        Response::Integer(41),
    );
    assert_eq!(
        request(&mut client, Request::Decrement { key: key.clone() }),
        Response::Integer(40),
    );

    match request(&mut client, Request::Ttl { key: key.clone() }) {
        Response::Integer(seconds) => {
            assert!((0..=60).contains(&seconds), "unexpected TTL: {seconds}");
        }
        other => panic!("TTL returned unexpected response: {other:?}"),
    }

    // Immediate expiration removes the key. INCR recreates it without a TTL.
    assert_eq!(
        request(
            &mut client,
            Request::Expire {
                key: key.clone(),
                seconds: 0,
            },
        ),
        Response::Integer(1),
    );
    assert_eq!(
        request(&mut client, Request::Increment { key: key.clone() }),
        Response::Integer(1),
    );
    assert_eq!(
        request(&mut client, Request::Ttl { key }),
        Response::Integer(-1),
    );
}
