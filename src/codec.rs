//! EmberDB v2 command codec.
//!
//! TCP frame: [payload_length: u32 BE][payload bytes]
//! Request payload: [version: u8][opcode: u8][command arguments]
//! Response payload: [version: u8][tag: u8][response data]
//! Error response: [version: u8][0xFF][HTTP-style status: u16 BE]
//!
//! These numeric statuses are inspired by HTTP; this is NOT HTTP.

use crate::protocol::MAX_FRAME_SIZE;

pub const VERSION: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Ping,
    Set { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Delete { key: Vec<u8> },
    Exists { key: Vec<u8> },
}

/// HTTP-equivalent error numbers. Success results have their own response tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    BadRequest = 400,
    PayloadTooLarge = 413,
    InternalServerError = 500,
    NotImplemented = 501,
    VersionNotSupported = 505,
}

impl ErrorCode {
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            400 => Some(Self::BadRequest),
            413 => Some(Self::PayloadTooLarge),
            500 => Some(Self::InternalServerError),
            501 => Some(Self::NotImplemented),
            505 => Some(Self::VersionNotSupported),
            _ => None,
        }
    }

    pub const fn reason(self) -> &'static str {
        match self {
            Self::BadRequest => "Bad Request",
            Self::PayloadTooLarge => "Content Too Large",
            Self::InternalServerError => "Internal Server Error",
            Self::NotImplemented => "Not Implemented",
            Self::VersionNotSupported => "Version Not Supported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Pong,
    Ok,
    Value(Vec<u8>),
    Nil,
    Integer(u64),
    Error(ErrorCode),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecError {
    Malformed,
    UnsupportedVersion,
    UnknownOpcode,
    TooLarge,
}

/// Append `[length: u32 BE][raw bytes]`, checking the entire payload budget.
fn append_blob(out: &mut Vec<u8>, data: &[u8]) -> Result<(), CodecError> {
    let length = u32::try_from(data.len()).map_err(|_| CodecError::TooLarge)?;
    let new_size = out
        .len()
        .checked_add(4)
        .and_then(|size| size.checked_add(data.len()))
        .ok_or(CodecError::TooLarge)?;

    if new_size > MAX_FRAME_SIZE {
        return Err(CodecError::TooLarge);
    }

    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(data);
    Ok(())
}

/// Take bytes from the front of `input` without out-of-bounds indexing.
fn take<'a>(input: &mut &'a [u8], count: usize) -> Result<&'a [u8], CodecError> {
    if input.len() < count {
        return Err(CodecError::Malformed);
    }
    let (head, tail) = input.split_at(count);
    *input = tail;
    Ok(head)
}

fn read_u8(input: &mut &[u8]) -> Result<u8, CodecError> {
    Ok(take(input, 1)?[0])
}

fn read_u16(input: &mut &[u8]) -> Result<u16, CodecError> {
    let bytes: [u8; 2] = take(input, 2)?
        .try_into()
        .map_err(|_| CodecError::Malformed)?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u32(input: &mut &[u8]) -> Result<u32, CodecError> {
    let bytes: [u8; 4] = take(input, 4)?
        .try_into()
        .map_err(|_| CodecError::Malformed)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(input: &mut &[u8]) -> Result<u64, CodecError> {
    let bytes: [u8; 8] = take(input, 8)?
        .try_into()
        .map_err(|_| CodecError::Malformed)?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_blob(input: &mut &[u8]) -> Result<Vec<u8>, CodecError> {
    let length = read_u32(input)? as usize;
    Ok(take(input, length)?.to_vec())
}

fn require_end(input: &[u8]) -> Result<(), CodecError> {
    if input.is_empty() {
        Ok(())
    } else {
        Err(CodecError::Malformed)
    }
}

pub fn encode_request(request: &Request) -> Result<Vec<u8>, CodecError> {
    let mut out = vec![VERSION];
    match request {
        Request::Ping => out.push(0x01),
        Request::Set { key, value } => {
            out.push(0x02);
            append_blob(&mut out, key)?;
            append_blob(&mut out, value)?;
        }
        Request::Get { key } => {
            out.push(0x03);
            append_blob(&mut out, key)?;
        }
        Request::Delete { key } => {
            out.push(0x04);
            append_blob(&mut out, key)?;
        }
        Request::Exists { key } => {
            out.push(0x05);
            append_blob(&mut out, key)?;
        }
    }
    Ok(out)
}

pub fn decode_request(data: &[u8]) -> Result<Request, CodecError> {
    if data.len() > MAX_FRAME_SIZE {
        return Err(CodecError::TooLarge);
    }
    let mut input = data;
    if read_u8(&mut input)? != VERSION {
        return Err(CodecError::UnsupportedVersion);
    }
    let request = match read_u8(&mut input)? {
        0x01 => Request::Ping,
        0x02 => Request::Set {
            key: read_blob(&mut input)?,
            value: read_blob(&mut input)?,
        },
        0x03 => Request::Get {
            key: read_blob(&mut input)?,
        },
        0x04 => Request::Delete {
            key: read_blob(&mut input)?,
        },
        0x05 => Request::Exists {
            key: read_blob(&mut input)?,
        },
        _ => return Err(CodecError::UnknownOpcode),
    };
    require_end(input)?;
    Ok(request)
}

pub fn encode_response(response: &Response) -> Result<Vec<u8>, CodecError> {
    let mut out = vec![VERSION];
    match response {
        Response::Pong => out.push(0x01),
        Response::Ok => out.push(0x02),
        Response::Value(value) => {
            out.push(0x03);
            append_blob(&mut out, value)?;
        }
        Response::Nil => out.push(0x04),
        Response::Integer(number) => {
            out.push(0x05);
            out.extend_from_slice(&number.to_be_bytes());
        }
        Response::Error(status) => {
            out.push(0xFF);
            out.extend_from_slice(&(*status as u16).to_be_bytes());
        }
    }
    Ok(out)
}

pub fn decode_response(data: &[u8]) -> Result<Response, CodecError> {
    if data.len() > MAX_FRAME_SIZE {
        return Err(CodecError::TooLarge);
    }
    let mut input = data;
    if read_u8(&mut input)? != VERSION {
        return Err(CodecError::UnsupportedVersion);
    }
    let response = match read_u8(&mut input)? {
        0x01 => Response::Pong,
        0x02 => Response::Ok,
        0x03 => Response::Value(read_blob(&mut input)?),
        0x04 => Response::Nil,
        0x05 => Response::Integer(read_u64(&mut input)?),
        0xFF => {
            let status = read_u16(&mut input)?;
            let error = ErrorCode::from_u16(status).ok_or(CodecError::Malformed)?;
            Response::Error(error)
        }
        _ => return Err(CodecError::UnknownOpcode),
    };
    require_end(input)?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trip_including_binary() {
        let commands = [
            Request::Ping,
            Request::Set {
                key: vec![0, 255],
                value: vec![10, 0, 128],
            },
            Request::Get {
                key: b"hello world".to_vec(),
            },
            Request::Delete {
                key: b"key".to_vec(),
            },
            Request::Exists {
                key: b"key".to_vec(),
            },
        ];
        for command in commands {
            assert_eq!(
                decode_request(&encode_request(&command).unwrap()),
                Ok(command)
            );
        }
    }

    #[test]
    fn response_round_trip() {
        let responses = [
            Response::Pong,
            Response::Ok,
            Response::Nil,
            Response::Value(vec![0, 255, 10]),
            Response::Integer(u64::MAX),
            Response::Error(ErrorCode::BadRequest),
            Response::Error(ErrorCode::PayloadTooLarge),
            Response::Error(ErrorCode::InternalServerError),
            Response::Error(ErrorCode::NotImplemented),
            Response::Error(ErrorCode::VersionNotSupported),
        ];
        for response in responses {
            assert_eq!(
                decode_response(&encode_response(&response).unwrap()),
                Ok(response)
            );
        }
    }

    #[test]
    fn error_status_is_big_endian_u16() {
        assert_eq!(
            encode_response(&Response::Error(ErrorCode::BadRequest)).unwrap(),
            [VERSION, 0xFF, 0x01, 0x90] // 400 decimal
        );
        assert_eq!(
            encode_response(&Response::Error(ErrorCode::NotImplemented)).unwrap(),
            [VERSION, 0xFF, 0x01, 0xF5] // 501 decimal
        );
    }

    #[test]
    fn invalid_frames_are_rejected() {
        assert_eq!(
            decode_request(&[VERSION, 0x03, 0, 0, 0, 5, b'a']),
            Err(CodecError::Malformed)
        );
        assert_eq!(
            decode_request(&[VERSION, 0x01, 99]),
            Err(CodecError::Malformed)
        );
        assert_eq!(
            decode_request(&[VERSION, 0x77]),
            Err(CodecError::UnknownOpcode)
        );
        assert_eq!(
            decode_request(&[1, 0x01]),
            Err(CodecError::UnsupportedVersion)
        );
        assert_eq!(
            decode_response(&[VERSION, 0xFF, 0x03, 0xE7]),
            Err(CodecError::Malformed)
        );
        assert_eq!(
            decode_response(&[VERSION, 0xFF, 0x01]),
            Err(CodecError::Malformed)
        );
    }

    #[test]
    fn oversized_request_is_rejected() {
        let request = Request::Set {
            key: b"key".to_vec(),
            value: vec![0; MAX_FRAME_SIZE],
        };
        assert_eq!(encode_request(&request), Err(CodecError::TooLarge));
    }
}
