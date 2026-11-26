//! # nexus-wal
//!
//! High-performance, zero-dependency Write-Ahead Log (WAL) engine in pure Rust.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
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
    End,
    Truncated,
    CrcMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeResult {
    pub records: Vec<Record>,
    pub stop: StopReason,
    pub stop_offset: usize,
}

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

pub fn decode_records(buffer: &[u8]) -> DecodeResult {
    let mut records = Vec::new();
    let mut offset = 0usize;
    while offset < buffer.len() {
        if buffer[offset..].iter().all(|&b| b == 0) {
            return DecodeResult {
                records,
                stop: StopReason::End,
                stop_offset: offset,
            };
        }
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
        let payload_size = match 8usize.checked_add(key_len).and_then(|n| n.checked_add(val_len)) {
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

pub trait Storage: Send {
    fn write_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()>;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn sync_data(&mut self) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn truncate(&mut self, len: u64) -> io::Result<()>;
}

pub struct FileStorage {
    file: File,
}

impl FileStorage {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Self { file })
    }
}

impl Storage for FileStorage {
    fn write_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let mut f = self.file.try_clone()?;
        f.seek(SeekFrom::Start(offset))?;
        f.read(buf)
    }
    fn sync_data(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
}

#[derive(Debug, Clone, Default)]
pub struct FaultConfig {
    pub tear_after_writes: Option<u64>,
    pub bit_flip_prob: f64,
    pub drop_sync: bool,
}

pub struct FaultyStorage {
    data: Vec<u8>,
    cfg: FaultConfig,
    write_count: u64,
}

impl FaultyStorage {
    pub fn new(cfg: FaultConfig) -> Self {
        Self {
            data: Vec::new(),
            cfg,
            write_count: 0,
        }
    }
}

impl Storage for FaultyStorage {
    fn write_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.write_count += 1;
        let mut buf = data.to_vec();
        if self.cfg.bit_flip_prob > 0.0 && !buf.is_empty() {
            let seed = self.write_count.wrapping_mul(0x9E37_79B9);
            if (seed % 1000) as f64 / 1000.0 < self.cfg.bit_flip_prob {
                let idx = (seed as usize) % buf.len();
                buf[idx] ^= 1 << ((seed >> 8) % 8);
            }
        }
        if let Some(limit) = self.cfg.tear_after_writes {
            if self.write_count > limit {
                buf.truncate(buf.len() / 2);
            }
        }
        let end = offset as usize + buf.len();
        if end > self.data.len() {
            self.data.resize(end, 0);
        }
        self.data[offset as usize..end].copy_from_slice(&buf);
        Ok(())
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let start = offset as usize;
        if start >= self.data.len() {
            return Ok(0);
        }
        let n = (self.data.len() - start).min(buf.len());
        buf[..n].copy_from_slice(&self.data[start..start + n]);
        Ok(n)
    }
    fn sync_data(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.data.len() as u64)
    }
    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.data.truncate(len as usize);
        Ok(())
    }
}

const PAGE_SIZE: usize = 4096;

pub struct WalEngine<S: Storage = FileStorage> {
    storage: S,
    pending: Vec<u8>,
    records: u64,
    durable_offset: u64,
}

impl WalEngine<FileStorage> {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let storage = FileStorage::open(path)?;
        let durable_offset = storage.len()?;
        Ok(Self {
            storage,
            pending: Vec::new(),
            records: 0,
            durable_offset,
        })
    }
}

impl<S: Storage> WalEngine<S> {
    pub fn with_storage(storage: S) -> io::Result<Self> {
        let durable_offset = storage.len()?;
        Ok(Self {
            storage,
            pending: Vec::new(),
            records: 0,
            durable_offset,
        })
    }

    pub fn len(&self) -> u64 {
        self.records
    }

    pub fn is_empty(&self) -> bool {
        self.records == 0
    }

    pub fn durable_len(&self) -> u64 {
        self.durable_offset
    }

    pub fn append_buffered(&mut self, key: &str, value: &str) {
        let rec = encode_record(key, value);
        self.pending.extend_from_slice(&rec);
        self.records += 1;
    }

    pub fn append(&mut self, key: &str, value: &str) -> io::Result<()> {
        self.append_buffered(key, value);
        self.flush()
    }

    pub fn append_batch(&mut self, entries: &[(&str, &str)]) -> io::Result<()> {
        for &(k, v) in entries {
            self.append_buffered(k, v);
        }
        self.flush()
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let unaligned = self.pending.len() % PAGE_SIZE;
        if unaligned != 0 {
            let pad = PAGE_SIZE - unaligned;
            self.pending.resize(self.pending.len() + pad, 0);
        }
        let offset = self.durable_offset;
        self.storage.write_at(offset, &self.pending)?;
        self.storage.sync_data()?;
        self.durable_offset += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }

    pub fn recover_records(&mut self) -> io::Result<Vec<Record>> {
        let len = self.storage.len()?;
        let mut buf = vec![0u8; len as usize];
        if len > 0 {
            self.storage.read_at(0, &mut buf)?;
        }
        let decoded = decode_records(&buf);
        if decoded.stop != StopReason::End {
            self.storage.truncate(decoded.stop_offset as u64)?;
            self.durable_offset = decoded.stop_offset as u64;
        } else {
            self.durable_offset = len;
        }
        self.records = decoded.records.len() as u64;
        self.pending.clear();
        Ok(decoded.records)
    }

    pub fn truncate_to(&mut self, n: u64) -> io::Result<()> {
        let records = self.recover_records()?;
        if n as usize >= records.len() {
            return Ok(());
        }
        let mut new_data = Vec::new();
        for r in records.iter().take(n as usize) {
            new_data.extend_from_slice(&encode_record(&r.key, &r.value));
        }
        let unaligned = new_data.len() % PAGE_SIZE;
        if unaligned != 0 {
            let pad = PAGE_SIZE - unaligned;
            new_data.resize(new_data.len() + pad, 0);
        }
        self.storage.truncate(0)?;
        if !new_data.is_empty() {
            self.storage.write_at(0, &new_data)?;
            self.storage.sync_data()?;
        }
        self.durable_offset = new_data.len() as u64;
        self.records = n;
        self.pending.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let bytes = encode_record("user:1001", "balance: 5000");
        let decoded = decode_records(&bytes);
        assert_eq!(decoded.stop, StopReason::End);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key, "user:1001");
    }

    #[test]
    fn crc_mismatch_is_rejected() {
        let mut bytes = encode_record("k", "v");
        *bytes.last_mut().unwrap() ^= 0xff;
        let decoded = decode_records(&bytes);
        assert_eq!(decoded.stop, StopReason::CrcMismatch);
        assert!(decoded.records.is_empty());
    }

    #[test]
    fn truncated_tail() {
        let mut bytes = encode_record("a", "1");
        bytes.extend_from_slice(&encode_record("b", "2"));
        bytes.pop();
        let decoded = decode_records(&bytes);
        assert_eq!(decoded.stop, StopReason::Truncated);
        assert_eq!(decoded.records.len(), 1);
    }

    #[test]
    fn empty_and_padding() {
        assert_eq!(decode_records(&[]).stop, StopReason::End);
        let mut bytes = encode_record("x", "y");
        bytes.resize(4096, 0);
        let decoded = decode_records(&bytes);
        assert_eq!(decoded.stop, StopReason::End);
        assert_eq!(decoded.records.len(), 1);
    }

    #[test]
    fn engine_roundtrip() {
        let storage = FaultyStorage::new(FaultConfig::default());
        let mut wal = WalEngine::with_storage(storage).unwrap();
        wal.append("a", "1").unwrap();
        wal.append("b", "2").unwrap();
        let recs = wal.recover_records().unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].key, "a");
        assert_eq!(recs[1].key, "b");
    }

    #[test]
    fn group_commit_alignment() {
        let storage = FaultyStorage::new(FaultConfig::default());
        let mut wal = WalEngine::with_storage(storage).unwrap();
        wal.append_batch(&[("a", "1"), ("b", "2")]).unwrap();
        assert_eq!(wal.durable_len() % PAGE_SIZE as u64, 0);
        let recs = wal.recover_records().unwrap();
        assert_eq!(recs.len(), 2);
    }
}
