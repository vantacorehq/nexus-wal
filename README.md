<p align="center">
  <img src="banner.svg" alt="banner" width="100%">
</p>

# NEXUS-WAL

**ZERO-DEPENDENCY WRITE-AHEAD LOG ENGINE.**

High-performance, pure-Rust append-only log built for crash consistency, durability and low-latency storage systems.

## What is NEXUS-WAL?

Every robust database, distributed system and message broker relies on a Write-Ahead Log (WAL) to guarantee data durability. Before data is applied to the main storage engine it is appended to a WAL. If the system crashes the WAL is replayed to restore a consistent state.

NEXUS-WAL is a small pure-Rust append-only log with:

* length-prefixed records + CRC32
* recovery that discards a truncated or corrupted tail
* **group commit** (many records → one `sync_data`)
* **4 KiB page alignment** for NVMe-friendly throughput
* abstract `Storage` trait + deterministic **fault injection**

## Guarantees

| Property              | Guarantee                                                                 |
|-----------------------|---------------------------------------------------------------------------|
| Durability            | After a successful `append` / `flush` the record survives process crash  |
| Recovery              | CRC mismatch or truncated tail → record is discarded, earlier records OK |
| Panic-freedom         | `decode_records` never panics on arbitrary bytes                         |
| Thread-safety         | Single-writer (the `WalEngine` handle is `!Sync` by design)              |
| Dependencies          | Zero runtime crates                                                      |
| Worst-case append     | O(1) amortized (group commit + page pad)                                 |

## Quick start

```rust
use nexus_wal::WalEngine;

let mut wal = WalEngine::open("data.wal")?;
wal.append("user:42", r#"{"balance":100}"#)?;
wal.append_batch(&[
    ("evt:1", "created"),
    ("evt:2", "updated"),
])?;

let records = wal.recover_records()?;
```

## Fault injection (deterministic crash testing)

```rust
use nexus_wal::{WalEngine, FaultyStorage, FaultConfig};

let cfg = FaultConfig {
    tear_after_writes: Some(5),   // tear the 6th write mid-frame
    bit_flip_prob: 0.01,          // occasional bit-flips
    drop_sync: false,
};
let storage = FaultyStorage::new(cfg);
let mut wal = WalEngine::with_storage(storage)?;

// … write some records …
// After a simulated crash, recover_records() must still return only
// the records that were fully written and checksummed.
```

## Group commit & page alignment

```rust
// Buffer many records, then one fsync for the whole group.
wal.append_buffered("k1", "v1");
wal.append_buffered("k2", "v2");
wal.flush()?;   // pads to 4 KiB boundary + single sync_data
```

## Fuzzing the decoder

```bash
cargo install cargo-fuzz
cargo fuzz run decode
```

The fuzzer feeds completely random bytes into `decode_records`. The function is required to never panic.

## Build & test

```bash
cargo test
cargo clippy -- -D warnings
```

## License

MIT
