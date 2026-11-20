use emberdb::codec::{decode_response, encode_request, ErrorCode, Request, Response, VERSION};
use emberdb::protocol::{read_frame, write_frame, MAX_FRAME_SIZE};

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

struct TestServer {
    child: Child,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_server() -> (TestServer, SocketAddr) {
    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);

    let child = Command::new(env!("CARGO_BIN_EXE_ember-server"))
        .env("EMBERDB_ADDR", address.to_string())
        .stdout(Stdio::null())
        .spawn()
        .expect("could not launch EmberDB");
    let mut server = TestServer { child };

    for _ in 0..100 {
        if let Some(status) = server.child.try_wait().unwrap() {
            panic!("EmberDB exited before becoming ready: {status}");
        }
        if TcpStream::connect(address).is_ok() {
            return (server, address);
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("server was not ready within two seconds");
}

fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    stream
}

fn request(stream: &mut TcpStream, command: Request) -> Response {
    let encoded = encode_request(&command).unwrap();
    write_frame(stream, &encoded).unwrap();
    let payload = read_frame(stream).unwrap().expect("unexpected disconnect");
    decode_response(&payload).unwrap()
}

fn raw_request(stream: &mut TcpStream, payload: &[u8]) -> Response {
    write_frame(stream, payload).unwrap();
    let payload = read_frame(stream).unwrap().expect("unexpected disconnect");
    decode_response(&payload).unwrap()
}

#[test]
fn clients_share_binary_values() {
    let (_server, address) = start_server();
    let mut a = connect(address);
    let mut b = connect(address);
    let key = vec![0, 255, 10];
    let value = vec![128, 0, 32, 255];

    assert_eq!(request(&mut a, Request::Ping), Response::Pong);
    assert_eq!(request(&mut a, Request::Set { key: key.clone(), value: value.clone() }), Response::Ok);
    assert_eq!(request(&mut b, Request::Get { key: key.clone() }), Response::Value(value));
    assert_eq!(request(&mut b, Request::Delete { key: key.clone() }), Response::Integer(1));
    assert_eq!(request(&mut a, Request::Get { key }), Response::Nil);
    assert_eq!(request(&mut a, Request::Ping), Response::Pong);
    assert_eq!(request(&mut b, Request::Ping), Response::Pong);
}

#[test]
fn errors_have_http_equivalent_codes_and_connection_survives() {
    let (_server, address) = start_server();
    let mut client = connect(address);

    assert_eq!(raw_request(&mut client, &[VERSION, 0x77]), Response::Error(ErrorCode::NotImplemented));
    assert_eq!(raw_request(&mut client, &[VERSION, 0x03, 0]), Response::Error(ErrorCode::BadRequest));
    assert_eq!(raw_request(&mut client, &[1, 0x01]), Response::Error(ErrorCode::VersionNotSupported));
    assert_eq!(request(&mut client, Request::Ping), Response::Pong);
}

#[test]
fn oversized_frame_returns_413_then_disconnects() {
    let (_server, address) = start_server();
    let mut client = connect(address);
    let too_large = (MAX_FRAME_SIZE as u32 + 1).to_be_bytes();
    // Only send the header. The server rejects the size before reading body bytes.
    use std::io::Write;
    client.write_all(&too_large).unwrap();

    let payload = read_frame(&mut client).unwrap().expect("expected 413 response");
    assert_eq!(decode_response(&payload), Ok(Response::Error(ErrorCode::PayloadTooLarge)));
    assert_eq!(read_frame(&mut client).unwrap(), None);
}
