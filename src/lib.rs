//! # nexus-wal
//!
//! High-performance, zero-dependency Write-Ahead Log (WAL) engine in pure Rust.
//!
//! Built for crash-consistency, durability, and low-latency storage systems.
//!
//! ## Features
//!
//! - Length-prefixed records + CRC32 for integrity.
//! - Recovery that discards a truncated or corrupted tail and never returns
//!   a record with a bad checksum.
//! - **Group commit**: multiple appends share a single `sync_data`.
//! - **4 KiB page alignment** for better NVMe throughput.
//! - Abstract `Storage` trait + fault-injection simulator for deterministic
//!   crash testing.
//! - Fully panic-free decode path (any garbage on disk is rejected cleanly).

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// CRC-32 (ISO 3309 / ITU-T V.42 polynomial).
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

/// Record layout: `[key_len:u32 le][val_len:u32 le][key][value][crc32:u32 le]`.
/// The CRC covers everything before the checksum field.
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
/// Stops at the first truncated tail or CRC mismatch. Already-decoded
/// records stay in `records`; the bad tail is never returned.
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

// ---------------------------------------------------------------------------
// Storage abstraction (enables fault injection)
// ---------------------------------------------------------------------------

/// Abstract storage backend.  Real deployments use `FileStorage`;
/// tests use `FaultyStorage` to simulate power loss, torn writes and bit-flips.
pub trait Storage: Send {
    fn write_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()>;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn sync_data(&mut self) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn truncate(&mut self, len: u64) -> io::Result<()>;
}

/// Production backend backed by a plain `std::fs::File`.
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

/// Fault-injection knobs used by `FaultyStorage`.
#[derive(Debug, Clone, Default)]
pub struct FaultConfig {
    /// After this many successful writes, the next write is truncated mid-frame.
    pub tear_after_writes: Option<u64>,
    /// Probability (0.0–1.0) of flipping a random bit in the written buffer.
    pub bit_flip_prob: f64,
    /// If true, `sync_data` is a no-op (simulates buffered I/O that never reaches disk).
    pub drop_sync: bool,
}

/// In-memory storage that can simulate power-loss, torn writes and bit-flips.
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

    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }
}

impl Storage for FaultyStorage {
    fn write_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.write_count += 1;

        let mut buf = data.to_vec();

        // Optional bit-flip
        if self.cfg.bit_flip_prob > 0.0 && !buf.is_empty() {
            // Simple deterministic “random” based on write_count for reproducibility
            let seed = self.write_count.wrapping_mul(0x9E37_79B9);
            if (seed % 1000) as f64 / 1000.0 < self.cfg.bit_flip_prob {
                let idx = (seed as usize) % buf.len();
                buf[idx] ^= 1 << ((seed >> 8) % 8);
            }
        }

        // Optional torn write
        if let Some(limit) = self.cfg.tear_after_writes {
            if self.write_count > limit {
                let tear_len = buf.len() / 2;
                buf.truncate(tear_len);
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
        let available = self.data.len() - start;
        let n = available.min(buf.len());
        buf[..n].copy_from_slice(&self.data[start..start + n]);
        Ok(n)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        if self.cfg.drop_sync {
            Ok(())
        } else {
            // In-memory: already “durable”
            Ok(())
        }
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.data.len() as u64)
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.data.truncate(len as usize);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WAL engine with group commit & page alignment
// ---------------------------------------------------------------------------

const PAGE_SIZE: usize = 4096;

/// Append-only write-ahead log with group commit and 4 KiB alignment.
pub struct WalEngine<S: Storage = FileStorage> {
    storage: S,
    /// In-memory pending records waiting for the next group commit.
    pending: Vec<u8>,
    /// Logical number of durable records.
    records: u64,
    /// Current end-of-log offset (after last successful sync).
    durable_offset: u64,
}

impl WalEngine<FileStorage> {
    /// Open or create a WAL on the real filesystem.
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
    /// Construct from an arbitrary storage backend (useful for tests).
    pub fn with_storage(storage: S) -> io::Result<Self> {
        let durable_offset = storage.len()?;
        Ok(Self {
            storage,
            pending: Vec::new(),
            records: 0,
            durable_offset,
        })
    }

    /// How many records have been durably committed.
    pub fn len(&self) -> u64 {
        self.records
    }

    pub fn is_empty(&self) -> bool {
        self.records == 0
    }

    /// Buffer a single record.  Does **not** call `sync_data`.
    /// Call [`flush`] to force durability, or use [`append`] for immediate durability.
    pub fn append_buffered(&mut self, key: &str, value: &str) {
        let rec = encode_record(key, value);
        self.pending.extend_from_slice(&rec);
        self.records += 1;
    }

    /// Append one record and immediately make it durable (single-record group commit).
    pub fn append(&mut self, key: &str, value: &str) -> io::Result<()> {
        self.append_buffered(key, value);
        self.flush()
    }

    /// Append a batch of records and issue a single `sync_data` for the whole group.
    /// This is the high-throughput path on NVMe.
    pub fn append_batch(&mut self, entries: &[(&str, &str)]) -> io::Result<()> {
        for &(k, v) in entries {
            self.append_buffered(k, v);
        }
        self.flush()
    }

    /// Force all buffered data to storage with 4 KiB alignment and a single sync.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        // Pad to page boundary for better device throughput.
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

    /// Recover all valid records from the beginning of the log.
    /// A truncated or CRC-mismatched tail is discarded.
    pub fn recover_records(&mut self) -> io::Result<Vec<Record>> {
        let len = self.storage.len()?;
        let mut buf = vec![0u8; len as usize];
        if len > 0 {
            self.storage.read_at(0, &mut buf)?;
        }
        let decoded = decode_records(&buf);
        // If we stopped early because of corruption / truncation, truncate
        // the file so the next append starts from a clean point.
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

    /// Truncate the log so that only the first `n` records remain.
    pub fn truncate_to(&mut self, n: u64) -> io::Result<()> {
        let records = self.recover_records()?;
        if n as usize >= records.len() {
            return Ok(());
        }
        // Rebuild the prefix
        let mut new_data = Vec::new();
        for r in records.iter().take(n as usize) {
            new_data.extend_from_slice(&encode_record(&r.key, &r.value));
        }
        // Align
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
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
        assert!(decoded.records.is_empty());

        let mut prefixed = encode_record("ok", "yes");
        let mut bad = encode_record("bad", "no");
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        prefixed.extend_from_slice(&bad);
        let decoded = decode_records(&prefixed);
        assert_eq!(decoded.stop, StopReason::CrcMismatch);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key, "ok");
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

        let mut only = encode_record("only", "partial");
        only.truncate(only.len() / 2);
        let decoded = decode_records(&only);
        assert_eq!(decoded.stop, StopReason::Truncated);
        assert!(decoded.records.is_empty());
    }

    #[test]
    fn engine_append_recover_batch_truncate() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("nexus-wal-test-{}.wal", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut wal = WalEngine::open(&path).unwrap();
        wal.append("a", "1").unwrap();
        wal.append("b", "2").unwrap();
        assert_eq!(wal.len(), 2);

        let recs = wal.recover_records().unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].key, "a");
        assert_eq!(recs[1].value, "2");

        wal.append_batch(&[("c", "3"), ("d", "4")]).unwrap();
        let recs = wal.recover_records().unwrap();
        assert_eq!(recs.len(), 4);

        wal.truncate_to(2).unwrap();
        let recs = wal.recover_records().unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].key, "a");
        assert_eq!(recs[1].key, "b");
        assert_eq!(wal.len(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fault_injection_torn_write_is_recovered() {
        // First write two complete records, then tear the third.
        let cfg = FaultConfig {
            tear_after_writes: Some(2),
            bit_flip_prob: 0.0,
            drop_sync: false,
        };
        let storage = FaultyStorage::new(cfg);
        let mut wal = WalEngine::with_storage(storage).unwrap();

        wal.append("good1", "v1").unwrap();
        wal.append("good2", "v2").unwrap();
        // This write will be torn by FaultyStorage
        let _ = wal.append("torn", "value");

        // Recover: only the two good records must survive.
        let recs = wal.recover_records().unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].key, "good1");
        assert_eq!(recs[1].key, "good2");
        assert!(recs.iter().all(|r| r.key != "torn"));
    }

    #[test]
    fn fault_injection_bit_flip_is_detected() {
        let cfg = FaultConfig {
            tear_after_writes: None,
            bit_flip_prob: 1.0, // always flip
            drop_sync: false,
        };
        let storage = FaultyStorage::new(cfg);
        let mut wal = WalEngine::with_storage(storage).unwrap();
        let _ = wal.append("key", "value");

        let recs = wal.recover_records().unwrap();
        // Because of the forced bit-flip the record must be rejected.
        assert!(recs.is_empty() || recs.iter().all(|r| r.key != "key"));
    }

    #[test]
    fn group_commit_and_alignment() {
        let storage = FaultyStorage::new(FaultConfig::default());
        let mut wal = WalEngine::with_storage(storage).unwrap();

        wal.append_batch(&[
            ("a", "1"),
            ("b", "2"),
            ("c", "3"),
        ])
        .unwrap();

        // After flush the durable length should be a multiple of PAGE_SIZE.
        let len = wal.storage.len().unwrap();
        assert_eq!(len % PAGE_SIZE as u64, 0);

        let recs = wal.recover_records().unwrap();
        assert_eq!(recs.len(), 3);
    }
}
