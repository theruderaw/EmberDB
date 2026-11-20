use std::io::{self, ErrorKind, Read, Write};

pub const MAX_FRAME_SIZE: usize = 1024 * 1024;

pub fn write_frame<W: Write>(writer: &mut W, payload: &[u8]) -> io::Result<()> {
    if payload.len() > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "frame exceeds maximum size",
        ));
    }

    let length = (payload.len() as u32).to_be_bytes();

    writer.write_all(&length)?;
    writer.write_all(payload)?;

    Ok(())
}

pub fn read_frame<R: Read>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0u8; 4];

    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == ErrorKind::Interrupted => {
                continue;
            }

            Err(error) => return Err(error),
        }
    }

    reader.read_exact(&mut header[1..])?;

    let length = u32::from_be_bytes(header) as usize;

    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "incoming frame exceeds maximum size",
        ));
    }

    let mut payload = vec![0u8; length];

    reader.read_exact(&mut payload)?;

    Ok(Some(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn ping_round_trip() {
        let mut buffer = Vec::new();

        write_frame(&mut buffer, b"PING").unwrap();

        assert_eq!(buffer, b"\x00\x00\x00\x04PING");

        let mut reader = Cursor::new(buffer);

        let frame = read_frame(&mut reader).unwrap();
        assert_eq!(frame, Some(b"PING".to_vec()));
    }

    #[test]
    fn multiple_frames() {
        let mut buffer = Vec::new();

        write_frame(&mut buffer, b"PING").unwrap();
        write_frame(&mut buffer, b"HELLO").unwrap();

        let mut reader = Cursor::new(buffer);

        assert_eq!(read_frame(&mut reader).unwrap(), Some(b"PING".to_vec()));

        assert_eq!(read_frame(&mut reader).unwrap(), Some(b"HELLO".to_vec()));

        assert_eq!(read_frame(&mut reader).unwrap(), None);
    }

    #[test]
    fn empty_frame() {
        let mut buffer = Vec::new();

        write_frame(&mut buffer, b"").unwrap();

        assert_eq!(buffer, [0, 0, 0, 0]);

        let mut reader = Cursor::new(buffer);

        assert_eq!(read_frame(&mut reader).unwrap(), Some(Vec::new()));
    }

    #[test]
    fn binary_payload() {
        let payload = [0x00, 0xFF, 0x0A, 0x42];
        let mut buffer = Vec::new();

        write_frame(&mut buffer, &payload).unwrap();

        let mut reader = Cursor::new(buffer);

        assert_eq!(read_frame(&mut reader).unwrap(), Some(payload.to_vec()));
    }

    #[test]
    fn clean_eof() {
        let mut reader = Cursor::new(Vec::<u8>::new());

        assert_eq!(read_frame(&mut reader).unwrap(), None);
    }

    #[test]
    fn truncated_header() {
        let mut reader = Cursor::new(vec![0, 0]);

        let error = read_frame(&mut reader).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn truncated_payload() {
        let data = b"\x00\x00\x00\x04PI";
        let mut reader = Cursor::new(data);

        let error = read_frame(&mut reader).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn oversized_incoming_frame() {
        let length = (MAX_FRAME_SIZE as u32 + 1).to_be_bytes();
        let mut reader = Cursor::new(length);

        let error = read_frame(&mut reader).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn oversized_outgoing_frame() {
        let payload = vec![0u8; MAX_FRAME_SIZE + 1];
        let mut buffer = Vec::new();

        let error = write_frame(&mut buffer, &payload).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(buffer.is_empty());
    }
}
