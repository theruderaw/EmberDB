//! End-to-end tests for the EmberDB v2 TCP protocol.
//!
//! These tests launch an actual ember-server process. Each test uses its own
//! ephemeral TCP port so tests can run in parallel without port conflicts.

use emberdb::codec::{ErrorCode, Request, Response, VERSION, decode_response, encode_request};
use emberdb::protocol::{MAX_FRAME_SIZE, read_frame, write_frame};

use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const IO_TIMEOUT: Duration = Duration::from_secs(3);
const RETRY_DELAY: Duration = Duration::from_millis(20);

/// Own the child server process and terminate it even if a test panics.
struct TestServer {
    child: Child,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start a server using a port allocated by the OS, with a predictable limit.
///
/// The temporary listener is released before the server binds, so there is
/// a small port-reuse race. This is acceptable for the current test harness.
fn start_server_with_limit(max_clients: usize) -> (TestServer, SocketAddr) {
    let probe = TcpListener::bind("127.0.0.1:0").expect("failed to reserve a test port");
    let address = probe.local_addr().expect("failed to read test port");
    drop(probe);

    let child = Command::new(env!("CARGO_BIN_EXE_ember-server"))
        .env("EMBERDB_ADDR", address.to_string())
        .env("EMBERDB_MAX_CLIENTS", max_clients.to_string())
        .env("EMBERDB_READ_TIMEOUT_SECS", "30")
        .env("EMBERDB_WRITE_TIMEOUT_SECS", "5")
        .stdout(Stdio::null())
        .spawn()
        .expect("failed to launch ember-server");

    let mut server = TestServer { child };

    // A successful TCP connection means the listening socket is ready.
    // This probe briefly occupies a connection slot. Tests that enforce a
    // one-client limit use wait_for_available_client() to handle its cleanup.
    for _ in 0..150 {
        if let Some(status) = server.child.try_wait().expect("failed to inspect server") {
            panic!("ember-server exited before becoming ready: {status}");
        }

        if TcpStream::connect_timeout(&address, CONNECT_TIMEOUT).is_ok() {
            return (server, address);
        }

        thread::sleep(RETRY_DELAY);
    }

    panic!("ember-server did not begin listening");
}

fn start_server() -> (TestServer, SocketAddr) {
    start_server_with_limit(128)
}

fn set_timeouts(stream: &TcpStream, duration: Duration) {
    stream
        .set_read_timeout(Some(duration))
        .expect("failed to set client read timeout");
    stream
        .set_write_timeout(Some(duration))
        .expect("failed to set client write timeout");
}

fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)
        .expect("failed to connect to ember-server");
    set_timeouts(&stream, IO_TIMEOUT);
    stream
}

fn request(stream: &mut TcpStream, command: Request) -> Response {
    let encoded = encode_request(&command).expect("failed to encode request");
    write_frame(stream, &encoded).expect("failed to send request");

    let payload = read_frame(stream)
        .expect("failed to read response frame")
        .expect("server closed connection unexpectedly");
    decode_response(&payload).expect("server sent an invalid v2 response")
}

fn raw_request(stream: &mut TcpStream, payload: &[u8]) -> Response {
    write_frame(stream, payload).expect("failed to send raw request");

    let payload = read_frame(stream)
        .expect("failed to read response frame")
        .expect("server closed connection unexpectedly");
    decode_response(&payload).expect("server sent an invalid v2 response")
}

/// Retry until a connection is *admitted* by the server, not merely accepted
/// by the TCP stack. Rejected connections receive an unsolicited 503.
fn wait_for_available_client(address: SocketAddr) -> TcpStream {
    let ping = encode_request(&Request::Ping).expect("PING must encode");

    for _ in 0..100 {
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) {
            // Keep unsuccessful retries short, particularly when a worker is
            // still releasing the slot used by the readiness probe.
            set_timeouts(&stream, Duration::from_millis(500));

            if write_frame(&mut stream, &ping).is_ok()
                && let Ok(Some(payload)) = read_frame(&mut stream)
                && decode_response(&payload) == Ok(Response::Pong)
            {
                set_timeouts(&stream, IO_TIMEOUT);
                return stream;
            }
        }

        thread::sleep(RETRY_DELAY);
    }

    panic!("no client slot became available");
}

#[test]
fn clients_share_binary_values() {
    let (_server, address) = start_server();
    let mut a = connect(address);
    let mut b = connect(address);

    let key = vec![0, 255, 10];
    let value = vec![128, 0, 32, 255];

    assert_eq!(request(&mut a, Request::Ping), Response::Pong);
    assert_eq!(
        request(
            &mut a,
            Request::Set {
                key: key.clone(),
                value: value.clone(),
            },
        ),
        Response::Ok,
    );
    assert_eq!(
        request(&mut b, Request::Get { key: key.clone() }),
        Response::Value(value),
    );
    assert_eq!(
        request(&mut b, Request::Delete { key: key.clone() }),
        Response::Integer(1),
    );
    assert_eq!(request(&mut a, Request::Get { key }), Response::Nil);
    assert_eq!(request(&mut a, Request::Ping), Response::Pong);
    assert_eq!(request(&mut b, Request::Ping), Response::Pong);
}

#[test]
fn errors_have_http_equivalent_codes_and_connection_survives() {
    let (_server, address) = start_server();
    let mut client = connect(address);

    assert_eq!(
        raw_request(&mut client, &[VERSION, 0x77]),
        Response::Error(ErrorCode::NotImplemented),
    );
    assert_eq!(
        raw_request(&mut client, &[VERSION, 0x03, 0]),
        Response::Error(ErrorCode::BadRequest),
    );
    assert_eq!(
        raw_request(&mut client, &[1, 0x01]),
        Response::Error(ErrorCode::VersionNotSupported),
    );
    assert_eq!(request(&mut client, Request::Ping), Response::Pong);
}

#[test]
fn oversized_frame_returns_413_then_disconnects() {
    let (_server, address) = start_server();
    let mut client = connect(address);

    // The server can reject a frame from its header without reading its body.
    let too_large = (MAX_FRAME_SIZE as u32 + 1).to_be_bytes();
    client
        .write_all(&too_large)
        .expect("failed to send length header");

    let payload = read_frame(&mut client)
        .expect("failed to read 413 response")
        .expect("server closed without sending 413");
    assert_eq!(
        decode_response(&payload),
        Ok(Response::Error(ErrorCode::PayloadTooLarge)),
    );
    assert_eq!(read_frame(&mut client).unwrap(), None);
}

#[test]
fn connection_limit_rejects_and_recovers() {
    let (_server, address) = start_server_with_limit(1);

    // The first client reserves the only available permit.
    let mut client_a = wait_for_available_client(address);
    assert_eq!(request(&mut client_a, Request::Ping), Response::Pong);

    // The server must return a structured v2 error before closing client B.
    // B does not have to send a request first: 503 is connection-level.
    let mut client_b = connect(address);
    let payload = read_frame(&mut client_b)
        .expect("failed to read 503 response")
        .expect("server closed without sending 503");
    assert_eq!(
        decode_response(&payload),
        Ok(Response::Error(ErrorCode::ServiceUnavailable)),
    );
    assert_eq!(read_frame(&mut client_b).unwrap(), None);
    drop(client_b);

    // Closing A releases its permit asynchronously through Drop.
    drop(client_a);

    // C should be admitted once that permit is released.
    let mut client_c = wait_for_available_client(address);
    assert_eq!(request(&mut client_c, Request::Ping), Response::Pong);
}
