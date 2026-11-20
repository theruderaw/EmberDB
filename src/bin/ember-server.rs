use std::{
    io::{self, BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn handle_client(stream: TcpStream) -> io::Result<()> {
    let address = stream.peer_addr()?;
    println!("Client connected: {address}");

    let mut reader = BufReader::new(stream);
    let mut command = String::new();

    loop {
        command.clear();

        let bytes_read = reader.read_line(&mut command)?;

        if bytes_read == 0 {
            break;
        }

        let response = if command.trim().eq_ignore_ascii_case("PING") {
            "PONG\n"
        } else {
            "ERR unknown command"
        };

        reader.get_mut().write_all(response.as_bytes())?;
    }

    println!("Client disconnected: {address}");
    Ok(())
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6380")?;

    println!("EmberDB is listening @ 127.0.0.1:6380");

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                thread::spawn(move || {
                    if let Err(error) = handle_client(stream) {
                        eprintln!("Client error: {error}")
                    }
                });
            }
            Err(error) => eprintln!("Accept error: {error}"),
        }
    }
    Ok(())
}
