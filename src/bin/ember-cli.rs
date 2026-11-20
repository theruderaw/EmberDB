use std::{
    io::{self, BufRead, BufReader, Write},
    net::TcpStream,
};

fn main() -> io::Result<()> {
    let stream = TcpStream::connect("127.0.0.1:6380")?;
    let mut server = BufReader::new(stream);

    println!("Connected to EmberDB");
    println!("Type PING, or QUIT to exit");

    loop {
        print!("ember>");
        io::stdout().flush()?;

        let mut input = String::new();

        if io::stdin().read_line(&mut input)? == 0 {
            break;
        }

        let command = input.trim();

        if command.is_empty() {
            continue;
        }

        server.get_mut().write_all(command.as_bytes())?;
        server.get_mut().write_all(b"\n")?;

        let mut response = String::new();

        if server.read_line(&mut response)? == 0 {
            println!("Server disconncted.");
            break;
        }

        println!("{}", response.trim_end());
    }

    Ok(())
}
