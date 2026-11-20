use emberdb::codec::{decode_response, encode_request, CodecError, ErrorCode, Request, Response};
use emberdb::config::load_address;
use emberdb::protocol::{read_frame, write_frame};

use std::io::{self, ErrorKind, Write};
use std::net::TcpStream;

fn parse_command(input: &str) -> Result<Request, (ErrorCode, &'static str)> {
    let words: Vec<&str> = input.split_whitespace().collect();
    match words.as_slice() {
        [cmd] if cmd.eq_ignore_ascii_case("PING") => Ok(Request::Ping),
        [cmd, key, values @ ..]
            if cmd.eq_ignore_ascii_case("SET") && !values.is_empty() =>
        {
            Ok(Request::Set {
                key: key.as_bytes().to_vec(),
                value: values.join(" ").into_bytes(),
            })
        }
        [cmd, key] if cmd.eq_ignore_ascii_case("GET") => Ok(Request::Get {
            key: key.as_bytes().to_vec(),
        }),
        [cmd, key] if cmd.eq_ignore_ascii_case("DEL") => Ok(Request::Delete {
            key: key.as_bytes().to_vec(),
        }),
        [cmd, key] if cmd.eq_ignore_ascii_case("EXISTS") => Ok(Request::Exists {
            key: key.as_bytes().to_vec(),
        }),
        [cmd, ..]
            if ["PING", "SET", "GET", "DEL", "EXISTS"]
                .iter()
                .any(|known| cmd.eq_ignore_ascii_case(known)) =>
        {
            Err((ErrorCode::BadRequest, "invalid number of arguments"))
        }
        _ => Err((ErrorCode::NotImplemented, "unknown command")),
    }
}

fn display_response(response: Response) {
    match response {
        Response::Pong => println!("PONG"),
        Response::Ok => println!("OK"),
        Response::Nil => println!("NIL"),
        Response::Integer(value) => println!("INTEGER {value}"),
        Response::Value(bytes) => match std::str::from_utf8(&bytes) {
            Ok(value) => println!("VALUE {value}"),
            Err(_) => println!("VALUE (binary) {bytes:?}"),
        },
        Response::Error(status) => {
            println!("ERR {} {}", status as u16, status.reason());
        }
    }
}

fn main() -> io::Result<()> {
    let address = load_address();
    let mut stream = TcpStream::connect(&*address)?;
    println!("Connected to EmberDB at {address}");
    println!("Type QUIT to exit.");

    loop {
        print!("ember> ");
        io::stdout().flush()?;
        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            break;
        }
        let input = input.trim();
        if input.eq_ignore_ascii_case("QUIT") {
            break;
        }
        if input.is_empty() {
            continue;
        }

        let request = match parse_command(input) {
            Ok(request) => request,
            Err((status, message)) => {
                println!("ERR {} {}: {message}", status as u16, status.reason());
                continue;
            }
        };
        let payload = match encode_request(&request) {
            Ok(payload) => payload,
            Err(CodecError::TooLarge) => {
                let status = ErrorCode::PayloadTooLarge;
                println!("ERR {} {}", status as u16, status.reason());
                continue;
            }
            Err(error) => {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("request encoding failed: {error:?}"),
                ));
            }
        };
        write_frame(&mut stream, &payload)?;

        let response_payload = match read_frame(&mut stream)? {
            Some(payload) => payload,
            None => {
                println!("Server disconnected");
                break;
            }
        };
        let response = decode_response(&response_payload).map_err(|error| {
            io::Error::new(ErrorKind::InvalidData, format!("invalid server response: {error:?}"))
        })?;
        display_response(response);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_set_multiword_value() {
        assert_eq!(
            parse_command("SET greeting hello world"),
            Ok(Request::Set {
                key: b"greeting".to_vec(),
                value: b"hello world".to_vec(),
            })
        );
        assert_eq!(
            parse_command("SET name"),
            Err((ErrorCode::BadRequest, "invalid number of arguments"))
        );
        assert_eq!(
            parse_command("UNKNOWN"),
            Err((ErrorCode::NotImplemented, "unknown command"))
        );
    }
}
