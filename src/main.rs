// NEXUS-WAL - length-prefixed CRC32 log with sync_data after each append.
// Compile: cargo build --release
// Run: ./target/release/nexus-wal

use std::io;

use nexus_wal::WalEngine;

fn main() -> io::Result<()> {
    let log_path = "nexus_data.wal";
    println!("[NEXUS-WAL] Initializing storage engine...");

    let mut wal = WalEngine::open(log_path)?;

    println!("[NEXUS-WAL] Appending records...");
    wal.append("user:1001", "balance: 5000")?;
    wal.append("user:1002", "balance: 1200")?;
    wal.append("config:network", "mainnet")?;
    println!("[NEXUS-WAL] Records written and synced to disk.");

    println!("\n--- Simulating System Crash and Restart ---\n");
    let mut wal_recovered = WalEngine::open(log_path)?;
    let decoded = wal_recovered.recover()?;
    println!("[NEXUS-WAL] Recovered {} records (stop={:?})", decoded.records.len(), decoded.stop);
    for rec in &decoded.records {
        println!("  -> [{}] = {}", rec.key, rec.value);
    }

    Ok(())
}
