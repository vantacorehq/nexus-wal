// NEXUS-WAL - High-Performance Write-Ahead Log Engine
// Compile: cargo build --release
// Run: ./target/release/nexus-wal

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

// Custom CRC32 implementation (No external crates)
fn crc32(data: &[u8]) -> u32 {
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

struct WalEngine {
    file: File,
    path: String,
}

impl WalEngine {
    // Open or create the log file
    fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
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

    // Append a key-value record to the log with integrity checking
    fn append(&mut self, key: &str, value: &str) -> io::Result<()> {
        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();

        // Record format: [key_len(4)] [val_len(4)] [key] [value] [crc32(4)]
        let key_len = key_bytes.len() as u32;
        let val_len = value_bytes.len() as u32;

        let mut payload = Vec::new();
        payload.extend_from_slice(&key_len.to_le_bytes());
        payload.extend_from_slice(&val_len.to_le_bytes());
        payload.extend_from_slice(key_bytes);
        payload.extend_from_slice(value_bytes);

        let checksum = crc32(&payload);

        // Move cursor to the end of the file
        self.file.seek(SeekFrom::End(0))?;
        
        // Write payload and checksum
        self.file.write_all(&payload)?;
        self.file.write_all(&checksum.to_le_bytes())?;
        
        // Force OS to flush data to the physical disk (fsync)
        self.file.sync_data()?;

        Ok(())
    }

    // Replay the log to recover state
    fn recover(&mut self) -> io::Result<()> {
        println!("[NEXUS-WAL] Recovering log: {}", self.path);
        self.file.seek(SeekFrom::Start(0))?;

        let mut total_records = 0;
        let mut buffer = Vec::new();
        self.file.read_to_end(&mut buffer)?;

        let mut offset = 0;
        while offset + 12 <= buffer.len() {
            let key_len = u32::from_le_bytes(buffer[offset..offset+4].try_into().unwrap()) as usize;
            let val_len = u32::from_le_bytes(buffer[offset+4..offset+8].try_into().unwrap()) as usize;
            
            let payload_size = 8 + key_len + val_len;
            let record_size = payload_size + 4; // + 4 bytes for CRC32

            if offset + record_size > buffer.len() { break; }

            let payload = &buffer[offset..offset+payload_size];
            let stored_crc = u32::from_le_bytes(buffer[offset+payload_size..offset+record_size].try_into().unwrap());
            
            let computed_crc = crc32(payload);

            if stored_crc == computed_crc {
                let key = String::from_utf8_lossy(&payload[8..8+key_len]);
                let value = String::from_utf8_lossy(&payload[8+key_len..]);
                println!("  -> Recovered: [{}] = {}", key, value);
                total_records += 1;
            } else {
                println!("  [!] CRC mismatch at offset {}. Log corrupted, stopping recovery.", offset);
                break;
            }

            offset += record_size;
        }

        println!("[NEXUS-WAL] Recovery complete. Total records: {}", total_records);
        Ok(())
    }
}

fn main() -> io::Result<()> {
    let log_path = "nexus_data.wal";
    println!("[NEXUS-WAL] Initializing storage engine...");

    let mut wal = WalEngine::open(log_path)?;
    
    // Simulate transaction writes
    println!("[NEXUS-WAL] Appending records...");
    wal.append("user:1001", "balance: 5000")?;
    wal.append("user:1002", "balance: 1200")?;
    wal.append("config:network", "mainnet")?;
    println!("[NEXUS-WAL] Records written and synced to disk.");

    // Simulate system restart and recovery
    println!("\n--- Simulating System Crash and Restart ---\n");
    let mut wal_recovered = WalEngine::open(log_path)?;
    wal_recovered.recover()?;

    Ok(())
}
