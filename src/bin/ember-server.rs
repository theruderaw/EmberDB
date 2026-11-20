use emberdb::codec::{decode_request, encode_response, CodecError, ErrorCode, Response};
use emberdb::command::execute;
use emberdb::config::load_address;
use emberdb::protocol::{read_frame, write_frame};
use emberdb::store::Store;

use std::io::{self, ErrorKind};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

fn send_response(stream: &mut TcpStream, response: &Response) -> io::Result<()> {
    let payload = encode_response(response).map_err(|error| {
        io::Error::new(ErrorKind::InvalidData, format!("response encoding failed: {error:?}"))
    })?;
    write_frame(stream, &payload)
}

fn handle_client(mut stream: TcpStream, store: &Store) -> io::Result<()> {
    let address = stream.peer_addr()?;
    println!("Client connected: {address}");

    loop {
        let payload = match read_frame(&mut stream) {
            Ok(Some(payload)) => payload,
            Ok(None) => break,
            // In the current protocol.rs, InvalidData means an oversized
            // length header. Reply 413 then close: payload bytes remain unread.
            Err(error) if error.kind() == ErrorKind::InvalidData => {
                send_response(&mut stream, &Response::Error(ErrorCode::PayloadTooLarge))?;
                break;
            }
            Err(error) => return Err(error),
        };

        let response = match decode_request(&payload) {
            Ok(request) => execute(store, request),
            Err(CodecError::Malformed) => Response::Error(ErrorCode::BadRequest),
            Err(CodecError::TooLarge) => Response::Error(ErrorCode::PayloadTooLarge),
            Err(CodecError::UnknownOpcode) => Response::Error(ErrorCode::NotImplemented),
            Err(CodecError::UnsupportedVersion) => {
                Response::Error(ErrorCode::VersionNotSupported)
            }
        };
        send_response(&mut stream, &response)?;
    }

    println!("Client disconnected: {address}");
    Ok(())
}

fn main() -> io::Result<()> {
    let address = load_address();
    let listener = TcpListener::bind(&*address)?;
    let store = Arc::new(Store::new());
    println!("EmberDB listening on {}", listener.local_addr()?);

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    if let Err(error) = handle_client(stream, &store) {
                        eprintln!("Client error: {error}");
                    }
                });
            }
            Err(error) => eprintln!("Accept error: {error}"),
        }
    }
    Ok(())
}
