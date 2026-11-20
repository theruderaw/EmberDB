//! Byte framing for TCP streams.
//!
//! Wire envelope: `[payload_len: u32 big-endian][payload bytes]`.
//! This module has no knowledge of command/response semantics.

use std::io::{self, ErrorKind, Read, Write};

pub const MAX_FRAME_SIZE: usize = 1024 * 1024;

/// Write one complete frame, rejecting oversized payloads before any bytes
/// are written. An I/O error after the header starts means the connection
/// cannot be assumed to be usable for another frame.
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

/// Read one complete frame.
///
/// - `Ok(Some(bytes))`: a complete frame (which may be empty)
/// - `Ok(None)`: clean EOF between frames
/// - `UnexpectedEof`: the connection ended halfway through a frame
/// - `InvalidData`: frame length exceeds the 1 MiB limit
pub fn read_frame<R: Read>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0u8; 4];

    // Distinguish clean EOF from an incomplete header. A single read() isn't
    // guaranteed to return all the bytes requested over a TCP connection.
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    reader.read_exact(&mut header[1..])?;

    let length = u32::from_be_bytes(header) as usize;
    if length > MAX_FRAME_SIZE {
        // Only this size check produces InvalidData in read_frame(). The
        // server uses it to return 413 and close the connection.
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "incoming frame exceeds maximum size",
        ));
    }

    let mut payload = vec![0; length];
    reader.read_exact(&mut payload)?;
    Ok(Some(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn ping_round_trip_and_exact_wire_bytes() {
        let mut out = Vec::new();
        write_frame(&mut out, b"PING").unwrap();
        assert_eq!(out, b"\x00\x00\x00\x04PING");
        let mut input = Cursor::new(out);
        assert_eq!(read_frame(&mut input).unwrap(), Some(b"PING".to_vec()));
        assert_eq!(read_frame(&mut input).unwrap(), None);
    }

    #[test]
    fn multiple_frames() {
        let mut out = Vec::new();
        write_frame(&mut out, b"PING").unwrap();
        write_frame(&mut out, b"HELLO").unwrap();
        let mut input = Cursor::new(out);
        assert_eq!(read_frame(&mut input).unwrap(), Some(b"PING".to_vec()));
        assert_eq!(read_frame(&mut input).unwrap(), Some(b"HELLO".to_vec()));
        assert_eq!(read_frame(&mut input).unwrap(), None);
    }

    #[test]
    fn empty_frame_is_not_eof() {
        let mut out = Vec::new();
        write_frame(&mut out, b"").unwrap();
        let mut input = Cursor::new(out);
        assert_eq!(read_frame(&mut input).unwrap(), Some(Vec::new()));
        assert_eq!(read_frame(&mut input).unwrap(), None);
    }

    #[test]
    fn binary_payload() {
        let payload = [0, 255, 10, 128];
        let mut out = Vec::new();
        write_frame(&mut out, &payload).unwrap();
        let mut input = Cursor::new(out);
        assert_eq!(read_frame(&mut input).unwrap(), Some(payload.to_vec()));
    }

    #[test]
    fn clean_eof() {
        assert_eq!(read_frame(&mut Cursor::new(Vec::<u8>::new())).unwrap(), None);
    }

    #[test]
    fn truncated_header_is_error() {
        let error = read_frame(&mut Cursor::new(vec![0, 0])).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn truncated_payload_is_error() {
        let error = read_frame(&mut Cursor::new(b"\x00\x00\x00\x04PI")).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn oversized_incoming_header_is_rejected() {
        let header = (MAX_FRAME_SIZE as u32 + 1).to_be_bytes();
        let error = read_frame(&mut Cursor::new(header)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn oversized_outgoing_frame_is_rejected_before_writing() {
        let mut out = Vec::new();
        let payload = vec![0; MAX_FRAME_SIZE + 1];
        let error = write_frame(&mut out, &payload).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(out.is_empty());
    }
}
