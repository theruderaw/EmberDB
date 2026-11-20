//! End-to-end TTL and expiration tests for the EmberDB v2 TCP protocol.
//!
//! Each test starts a real ember-server on an ephemeral loopback port. The
//! server is terminated automatically, including when a test panics.

use emberdb::codec::{ErrorCode, Request, Response, decode_response, encode_request};
use emberdb::protocol::{read_frame, write_frame};

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

const STARTUP_RETRIES: usize = 150;
const RETRY_DELAY: Duration = Duration::from_millis(20);
const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const STARTUP_IO_TIMEOUT: Duration = Duration::from_millis(500);
const IO_TIMEOUT: Duration = Duration::from_secs(3);

struct TestServer {
    child: Child,
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

/// Return a live, admitted TCP client. PING/PONG proves that the server is
/// ready, rather than merely accepting connections at the TCP layer.
fn start_server() -> (TestServer, TcpStream) {
    let probe = TcpListener::bind("127.0.0.1:0").expect("failed to allocate a test port");
    let address: SocketAddr = probe.local_addr().expect("failed to get test address");
    drop(probe);

    let child = Command::new(env!("CARGO_BIN_EXE_ember-server"))
        .env("EMBERDB_ADDR", address.to_string())
        .env("EMBERDB_MAX_CLIENTS", "16")
        .env("EMBERDB_READ_TIMEOUT_SECS", "30")
        .env("EMBERDB_WRITE_TIMEOUT_SECS", "5")
        .env("EMBERDB_SHUTDOWN_TIMEOUT_SECS", "2")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to launch ember-server");

    let mut server = TestServer { child };
    let ping = encode_request(&Request::Ping).expect("PING must encode");

    for _ in 0..STARTUP_RETRIES {
        if let Some(status) = server.child.try_wait().expect("failed to inspect server") {
            panic!("ember-server exited during startup: {status}");
        }

        if let Ok(mut stream) = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) {
            set_timeouts(&stream, STARTUP_IO_TIMEOUT);

            if write_frame(&mut stream, &ping).is_ok()
                && let Ok(Some(payload)) = read_frame(&mut stream)
                && decode_response(&payload) == Ok(Response::Pong)
            {
                set_timeouts(&stream, IO_TIMEOUT);
                return (server, stream);
            }
        }

        thread::sleep(RETRY_DELAY);
    }

    panic!("ember-server did not become ready");
}

fn request(client: &mut TcpStream, command: Request) -> Response {
    let payload = encode_request(&command).expect("request encoding failed");
    write_frame(client, &payload).expect("failed to send request frame");

    let response = read_frame(client)
        .expect("failed to read response frame")
        .expect("server closed the connection unexpectedly");
    decode_response(&response).expect("invalid binary response from server")
}

#[test]
fn ttl_reports_missing_persistent_and_expiring_keys() {
    let (_server, mut client) = start_server();
    let key = b"lifetime".to_vec();

    assert_eq!(
        request(&mut client, Request::Ttl { key: key.clone() }),
        Response::Integer(-2),
    );

    assert_eq!(
        request(
            &mut client,
            Request::Set {
                key: key.clone(),
                value: b"value".to_vec(),
            },
        ),
        Response::Ok,
    );
    assert_eq!(
        request(&mut client, Request::Ttl { key: key.clone() }),
        Response::Integer(-1),
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

    match request(&mut client, Request::Ttl { key }) {
        Response::Integer(seconds) => assert!(
            (0..=60).contains(&seconds),
            "unexpected remaining TTL: {seconds}"
        ),
        other => panic!("expected integer TTL, got {other:?}"),
    }
}

#[test]
fn expired_key_disappears_from_get_exists_and_ttl() {
    let (_server, mut client) = start_server();
    let key = b"short-lived".to_vec();

    assert_eq!(
        request(
            &mut client,
            Request::Set {
                key: key.clone(),
                value: b"hello".to_vec(),
            },
        ),
        Response::Ok,
    );
    assert_eq!(
        request(
            &mut client,
            Request::Expire {
                key: key.clone(),
                seconds: 1,
            },
        ),
        Response::Integer(1),
    );

    thread::sleep(Duration::from_millis(1200));

    assert_eq!(
        request(&mut client, Request::Get { key: key.clone() }),
        Response::Nil,
    );
    assert_eq!(
        request(&mut client, Request::Exists { key: key.clone() }),
        Response::Integer(0),
    );
    assert_eq!(
        request(&mut client, Request::Ttl { key: key.clone() }),
        Response::Integer(-2),
    );
    assert_eq!(
        request(&mut client, Request::Delete { key }),
        Response::Integer(0),
    );
}

#[test]
fn zero_and_negative_expiration_delete_immediately() {
    let (_server, mut client) = start_server();

    for seconds in [0, -10] {
        let key = format!("immediate:{seconds}").into_bytes();

        assert_eq!(
            request(
                &mut client,
                Request::Set {
                    key: key.clone(),
                    value: b"data".to_vec(),
                },
            ),
            Response::Ok,
        );
        assert_eq!(
            request(
                &mut client,
                Request::Expire {
                    key: key.clone(),
                    seconds,
                },
            ),
            Response::Integer(1),
        );
        assert_eq!(
            request(&mut client, Request::Get { key: key.clone() }),
            Response::Nil,
        );
        assert_eq!(
            request(&mut client, Request::Ttl { key: key.clone() }),
            Response::Integer(-2),
        );
        assert_eq!(
            request(&mut client, Request::Expire { key, seconds: 30 }),
            Response::Integer(0),
        );
    }
}

#[test]
fn overwriting_key_clears_its_ttl() {
    let (_server, mut client) = start_server();
    let key = b"overwrite".to_vec();

    assert_eq!(
        request(
            &mut client,
            Request::Set {
                key: key.clone(),
                value: b"first".to_vec(),
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
        request(
            &mut client,
            Request::Set {
                key: key.clone(),
                value: b"second".to_vec(),
            },
        ),
        Response::Ok,
    );
    assert_eq!(
        request(&mut client, Request::Ttl { key: key.clone() }),
        Response::Integer(-1),
    );
    assert_eq!(
        request(&mut client, Request::Get { key }),
        Response::Value(b"second".to_vec()),
    );
}

#[test]
fn unrepresentable_expiration_returns_400_and_connection_survives() {
    let (_server, mut client) = start_server();
    let key = b"overflow".to_vec();

    assert_eq!(
        request(
            &mut client,
            Request::Set {
                key: key.clone(),
                value: b"safe".to_vec(),
            },
        ),
        Response::Ok,
    );

    // On Linux, Instant::checked_add cannot represent this deadline.
    // EmberDB must report 400 rather than panic or corrupt the key.
    assert_eq!(
        request(
            &mut client,
            Request::Expire {
                key: key.clone(),
                seconds: i64::MAX,
            },
        ),
        Response::Error(ErrorCode::BadRequest),
    );
    assert_eq!(
        request(&mut client, Request::Ttl { key: key.clone() }),
        Response::Integer(-1),
    );
    assert_eq!(
        request(&mut client, Request::Get { key }),
        Response::Value(b"safe".to_vec()),
    );
    assert_eq!(request(&mut client, Request::Ping), Response::Pong);
}
