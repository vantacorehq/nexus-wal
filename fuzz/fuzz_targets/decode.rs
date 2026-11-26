#![no_main]
use libfuzzer_sys::fuzz_target;
use nexus_wal::decode_records;

fuzz_target!(|data: &[u8]| {
    // Must never panic on arbitrary input.
    let _ = decode_records(data);
});
