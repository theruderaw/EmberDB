use emberdb::{
    command::execute,
    protocol::{read_frame, write_frame},
    store::Store,
};

use std::{
    io::{self},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
};

fn handle_client(mut stream: TcpStream, store: &Store) -> io::Result<()> {
    let adddress = stream.peer_addr()?;

    println!("Client connected: {adddress}");

    while let Some(payload) = read_frame(&mut stream)? {
        let response = execute(store, &payload);
        write_frame(&mut stream, &response)?;
    }

    println!("Client disconnected: {adddress}");

    Ok(())
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6380")?;
    let store = Arc::new(Store::new());

    println!("Emberdb listening on 127.0.0.1:6380");

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                let store = Arc::clone(&store);

                thread::spawn(move || {
                    if let Err(error) = handle_client(stream, &store) {
                        eprintln!("Client error: {error}")
                    }
                });
            }
            Err(error) => {
                eprintln!("Accept error: {error}")
            }
        }
    }

    Ok(())
}
