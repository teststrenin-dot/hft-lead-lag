use std::path::Path;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FileFingerprint {
    pub(super) modified: SystemTime,
    pub(super) len: u64,
    pub(super) content_hash: u64,
}

pub(super) fn hash_content_deterministic(bytes: &[u8]) -> u64 {
    // FNV-1a 64-bit hash keeps fingerprinting deterministic and dependency-free.
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub(super) fn read_file_fingerprint(path: &Path) -> Option<FileFingerprint> {
    let metadata = std::fs::metadata(path).ok()?;
    let modified = metadata.modified().ok()?;
    let content = std::fs::read(path).ok()?;
    Some(FileFingerprint {
        modified,
        len: metadata.len(),
        content_hash: hash_content_deterministic(&content),
    })
}

pub(super) fn file_fingerprint_changed(
    previous: Option<FileFingerprint>,
    current: Option<FileFingerprint>,
) -> bool {
    match current {
        Some(current) => previous != Some(current),
        None => false,
    }
}
