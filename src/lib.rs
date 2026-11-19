//! Length-prefixed CRC32 write-ahead records.
//!
//! A successful append writes one record and then calls `sync_data`.
//! Recovery returns only records whose CRC matches and whose bytes are
//! fully present. A truncated tail is not a record. A CRC mismatch stops
//! decoding and is not returned as a valid record.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFFFFFFu32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// Every byte belonged to a valid record.
    End,
    /// Bytes remain, but they are not a complete record.
    Truncated,
    /// A complete record's stored CRC did not match.
    CrcMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeResult {
    pub records: Vec<Record>,
    pub stop: StopReason,
    pub stop_offset: usize,
}

/// Record bytes: `[key_len:u32 le][val_len:u32 le][key][value][crc32:u32 le]`.
/// The CRC covers everything before the checksum.
pub fn encode_record(key: &str, value: &str) -> Vec<u8> {
    let key_bytes = key.as_bytes();
    let value_bytes = value.as_bytes();
    let mut payload = Vec::with_capacity(8 + key_bytes.len() + value_bytes.len() + 4);
    payload.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
    payload.extend_from_slice(key_bytes);
    payload.extend_from_slice(value_bytes);
    let checksum = crc32(&payload);
    payload.extend_from_slice(&checksum.to_le_bytes());
    payload
}

fn read_u32(buf: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let bytes = buf.get(offset..end)?;
    Some(u32::from_le_bytes(bytes.try_into().ok()?))
}

/// Decode a buffer of concatenated records.
///
/// Stops at the first truncated tail or CRC mismatch. Records already
/// decoded stay in `records`; the bad tail is not pushed.
pub fn decode_records(buffer: &[u8]) -> DecodeResult {
    let mut records = Vec::new();
    let mut offset = 0usize;
    while offset < buffer.len() {
        if buffer.len().saturating_sub(offset) < 12 {
            return DecodeResult {
                records,
                stop: StopReason::Truncated,
                stop_offset: offset,
            };
        }
        let key_len = match read_u32(buffer, offset) {
            Some(n) => n as usize,
            None => {
                return DecodeResult {
                    records,
                    stop: StopReason::Truncated,
                    stop_offset: offset,
                };
            }
        };
        let val_len = match read_u32(buffer, offset + 4) {
            Some(n) => n as usize,
            None => {
                return DecodeResult {
                    records,
                    stop: StopReason::Truncated,
                    stop_offset: offset,
                };
            }
        };
        let payload_size = match 8usize
            .checked_add(key_len)
            .and_then(|n| n.checked_add(val_len))
        {
            Some(n) => n,
            None => {
                return DecodeResult {
                    records,
                    stop: StopReason::Truncated,
                    stop_offset: offset,
                };
            }
        };
        let record_size = match payload_size.checked_add(4) {
            Some(n) => n,
            None => {
                return DecodeResult {
                    records,
                    stop: StopReason::Truncated,
                    stop_offset: offset,
                };
            }
        };
        let record_end = match offset.checked_add(record_size) {
            Some(n) if n <= buffer.len() => n,
            _ => {
                return DecodeResult {
                    records,
                    stop: StopReason::Truncated,
                    stop_offset: offset,
                };
            }
        };
        let payload = &buffer[offset..offset + payload_size];
        let stored_crc = read_u32(buffer, offset + payload_size).unwrap();
        if stored_crc != crc32(payload) {
            return DecodeResult {
                records,
                stop: StopReason::CrcMismatch,
                stop_offset: offset,
            };
        }
        let key = String::from_utf8_lossy(&payload[8..8 + key_len]).into_owned();
        let value = String::from_utf8_lossy(&payload[8 + key_len..]).into_owned();
        records.push(Record { key, value });
        offset = record_end;
    }
    DecodeResult {
        records,
        stop: StopReason::End,
        stop_offset: offset,
    }
}

pub struct WalEngine {
    file: File,
    path: String,
}

impl WalEngine {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        Ok(Self {
            file,
            path: path.as_ref().to_string_lossy().into_owned(),
        })
    }

    pub fn append(&mut self, key: &str, value: &str) -> io::Result<()> {
        let record = encode_record(key, value);
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&record)?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn recover(&mut self) -> io::Result<usize> {
        println!("[NEXUS-WAL] Recovering log: {}", self.path);
        self.file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        self.file.read_to_end(&mut buffer)?;
        let decoded = decode_records(&buffer);
        for rec in &decoded.records {
            println!("  -> Recovered: [{}] = {}", rec.key, rec.value);
        }
        if decoded.stop == StopReason::CrcMismatch {
            println!(
                "  [!] CRC mismatch at offset {}. Log corrupted, stopping recovery.",
                decoded.stop_offset
            );
        }
        println!(
            "[NEXUS-WAL] Recovery complete. Total records: {}",
            decoded.records.len()
        );
        Ok(decoded.records.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrip() {
        let bytes = encode_record("user:1001", "balance: 5000");
        let decoded = decode_records(&bytes);
        assert_eq!(decoded.stop, StopReason::End);
        assert_eq!(
            decoded.records,
            vec![Record {
                key: "user:1001".into(),
                value: "balance: 5000".into(),
            }]
        );
    }

    #[test]
    fn crc_mismatch_is_rejected() {
        let mut bytes = encode_record("user:1002", "balance: 1200");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let decoded = decode_records(&bytes);
        assert_eq!(decoded.stop, StopReason::CrcMismatch);
        assert!(
            decoded.records.is_empty(),
            "mismatched record must not be returned"
        );

        let mut prefixed = encode_record("ok", "yes");
        let mut bad = encode_record("bad", "no");
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        prefixed.extend_from_slice(&bad);
        let decoded = decode_records(&prefixed);
        assert_eq!(decoded.stop, StopReason::CrcMismatch);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key, "ok");
        assert!(decoded.records.iter().all(|r| r.key != "bad"));
    }

    #[test]
    fn truncated_tail_is_not_a_valid_record() {
        let mut bytes = encode_record("alpha", "one");
        bytes.extend_from_slice(&encode_record("beta", "two"));
        bytes.pop();
        let decoded = decode_records(&bytes);
        assert_eq!(decoded.stop, StopReason::Truncated);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key, "alpha");
        assert!(decoded.records.iter().all(|r| r.key != "beta"));

        let mut only = encode_record("only", "partial");
        only.truncate(only.len() / 2);
        let decoded = decode_records(&only);
        assert_eq!(decoded.stop, StopReason::Truncated);
        assert!(decoded.records.is_empty());
    }
}
