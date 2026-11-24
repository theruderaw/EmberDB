use crate::version::WAL_VERSION;

pub const RECORD_OVERHEAD: usize = 34;
pub const MAX_PAYLOAD_SIZE: usize = 1024 * 1024;
pub const MAX_RECORD_SIZE: usize = RECORD_OVERHEAD + MAX_PAYLOAD_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    Command = 0x01,
    Begin = 0x02,
    Commit = 0x03,
    Abort = 0x04,
}

impl TryFrom<u8> for RecordKind {
    type Error = WalRecordError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Command),
            0x02 => Ok(Self::Begin),
            0x03 => Ok(Self::Commit),
            0x04 => Ok(Self::Abort),
            _ => Err(WalRecordError::InvalidRecordKind(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub kind: RecordKind,
    pub sequence: u64,
    pub timestamp_unix_ms: u64,
    pub transaction_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum WalRecordError {
    InvalidSequence,
    InvalidTransaction,
    InvalidPayload,
    PayloadTooLarge,
    InvalidLength,
    ChecksumMismatch,
    UnsupportedVersion(u8),
    InvalidRecordKind(u8),
}

impl WalRecord {
    pub fn validate(&self) -> Result<(), WalRecordError> {
        if self.sequence == 0 {
            return Err(WalRecordError::InvalidSequence);
        }

        if self.payload.len() > MAX_PAYLOAD_SIZE {
            return Err(WalRecordError::PayloadTooLarge);
        }

        match self.kind {
            RecordKind::Command => {
                if self.payload.is_empty() {
                    return Err(WalRecordError::InvalidPayload);
                }
            }

            RecordKind::Begin | RecordKind::Commit | RecordKind::Abort => {
                if self.transaction_id == 0 {
                    return Err(WalRecordError::InvalidTransaction);
                }

                if !self.payload.is_empty() {
                    return Err(WalRecordError::InvalidPayload);
                }
            }
        }

        Ok(())
    }

    pub fn encode(self) -> Result<Vec<u8>, WalRecordError> {
        self.validate()?;

        let total_size = RECORD_OVERHEAD + self.payload.len();

        let record_length =
            u32::try_from(total_size - 4).map_err(|_| WalRecordError::PayloadTooLarge)?;

        let mut buffer = Vec::with_capacity(total_size);

        buffer.extend_from_slice(&record_length.to_be_bytes());

        buffer.push(WAL_VERSION);
        buffer.push(self.kind as u8);

        buffer.extend_from_slice(&self.sequence.to_be_bytes());

        buffer.extend_from_slice(&self.timestamp_unix_ms.to_be_bytes());

        buffer.extend_from_slice(&self.transaction_id.to_be_bytes());

        buffer.extend_from_slice(&self.payload);

        let checksum = crc32c::crc32c(&buffer);

        buffer.extend_from_slice(&checksum.to_be_bytes());

        Ok(buffer)
    }

    pub fn decode(buffer: &[u8]) -> Result<Self, WalRecordError> {
        if buffer.len() < RECORD_OVERHEAD || buffer.len() > MAX_RECORD_SIZE {
            return Err(WalRecordError::InvalidLength);
        }
        let declared_length = u32::from_be_bytes(buffer[0..4].try_into().unwrap()) as usize;
        if declared_length != buffer.len() - 4 {
            return Err(WalRecordError::InvalidLength);
        }

        let checksum_offset = buffer.len() - 4;

        let stored_checksum = u32::from_be_bytes(buffer[checksum_offset..].try_into().unwrap());

        let calculated_checksum = crc32c::crc32c(&buffer[..checksum_offset]);

        if stored_checksum != calculated_checksum {
            return Err(WalRecordError::ChecksumMismatch);
        }

        let version = buffer[4];

        if version != WAL_VERSION {
            return Err(WalRecordError::UnsupportedVersion(version));
        }

        let kind = RecordKind::try_from(buffer[5])?;
        let sequence = u64::from_be_bytes(buffer[6..14].try_into().unwrap());
        let timestamp_unix_ms = u64::from_be_bytes(buffer[14..22].try_into().unwrap());
        let transaction_id = u64::from_be_bytes(buffer[22..30].try_into().unwrap());
        let payload = buffer[30..checksum_offset].to_vec();

        let record = Self {
            kind,
            sequence,
            timestamp_unix_ms,
            transaction_id,
            payload,
        };

        record.validate()?;

        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Request, encode_request};

    #[test]
    fn command_preserves_original_codec_bytes() {
        let payload = encode_request(&Request::Set {
            key: b"counter".to_vec(),
            value: b"100".to_vec(),
        })
        .unwrap();

        let record = WalRecord {
            kind: RecordKind::Command,
            sequence: 1,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 0,
            payload: payload.clone(),
        };

        let encoded = record.encode().unwrap();

        assert_eq!(encoded.len(), RECORD_OVERHEAD + payload.len());

        assert_eq!(&encoded[30..encoded.len() - 4], payload.as_slice());
    }

    #[test]
    fn encoded_record_contains_valid_checksum() {
        let record = WalRecord {
            kind: RecordKind::Command,
            sequence: 1,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 0,
            payload: vec![0x02, 0x01],
        };

        let encoded = record.encode().unwrap();

        let checksum_offset = encoded.len() - 4;

        let stored_checksum = u32::from_be_bytes(encoded[checksum_offset..].try_into().unwrap());

        assert_eq!(stored_checksum, crc32c::crc32c(&encoded[..checksum_offset]));
    }

    #[test]
    fn transaction_marker_requires_transaction_id() {
        let record = WalRecord {
            kind: RecordKind::Begin,
            sequence: 1,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 0,
            payload: Vec::new(),
        };

        assert_eq!(record.encode(), Err(WalRecordError::InvalidTransaction));
    }

    #[test]
    fn transaction_marker_encodes_without_payload() {
        let record = WalRecord {
            kind: RecordKind::Commit,
            sequence: 5,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 42,
            payload: Vec::new(),
        };

        assert_eq!(record.encode().unwrap().len(), RECORD_OVERHEAD);
    }

    #[test]
    fn zero_sequence_is_rejected() {
        let record = WalRecord {
            kind: RecordKind::Command,
            sequence: 0,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 0,
            payload: vec![0x02, 0x01],
        };

        assert_eq!(record.encode(), Err(WalRecordError::InvalidSequence));
    }

    #[test]
    fn record_round_trip() {
        let record = WalRecord {
            kind: RecordKind::Command,
            sequence: 42,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 10,
            payload: encode_request(&Request::Increment {
                key: b"counter".to_vec(),
            })
            .unwrap(),
        };

        let encoded = record.clone().encode().unwrap();
        let decoded = WalRecord::decode(&encoded).unwrap();

        assert_eq!(decoded, record);
    }

    #[test]
    fn corrupted_record_is_rejected() {
        let record = WalRecord {
            kind: RecordKind::Command,
            sequence: 1,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 0,
            payload: vec![0x02, 0x01],
        };

        let mut encoded = record.encode().unwrap();

        encoded[30] ^= 0xFF;

        assert_eq!(
            WalRecord::decode(&encoded),
            Err(WalRecordError::ChecksumMismatch)
        );
    }

    #[test]
    fn truncated_record_is_rejected() {
        let record = WalRecord {
            kind: RecordKind::Command,
            sequence: 1,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 0,
            payload: vec![0x02, 0x01],
        };

        let mut encoded = record.encode().unwrap();

        encoded.pop();

        assert_eq!(
            WalRecord::decode(&encoded),
            Err(WalRecordError::InvalidLength)
        );
    }

    #[test]
    fn transaction_markers_round_trip() {
        for kind in [RecordKind::Begin, RecordKind::Commit, RecordKind::Abort] {
            let record = WalRecord {
                kind,
                sequence: 1,
                timestamp_unix_ms: 1_800_000_000_000,
                transaction_id: 42,
                payload: Vec::new(),
            };

            let encoded = record.clone().encode().unwrap();

            assert_eq!(WalRecord::decode(&encoded).unwrap(), record);
        }
    }

    #[test]
    fn unsupported_wal_version_is_rejected() {
        let record = WalRecord {
            kind: RecordKind::Command,
            sequence: 1,
            timestamp_unix_ms: 1_800_000_000_000,
            transaction_id: 0,
            payload: vec![0x02, 0x01],
        };

        let mut encoded = record.encode().unwrap();

        encoded[4] = WAL_VERSION + 1;

        let checksum_offset = encoded.len() - 4;
        let checksum = crc32c::crc32c(&encoded[..checksum_offset]);

        encoded[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());

        assert_eq!(
            WalRecord::decode(&encoded),
            Err(WalRecordError::UnsupportedVersion(WAL_VERSION + 1))
        );
    }
}
