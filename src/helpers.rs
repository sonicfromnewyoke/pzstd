use crate::error::{PzstdError, Result};

/// Magic number identifying the start of a Zstd data frame (RFC 8878 §3.1.1).
pub const ZSTD_MAGIC_NUMBER: u32 = 0xFD2FB528;

/// Lower bound of the magic number range for skippable frames (RFC 8878 §3.1.2).
/// pzstd uses these frames to store the size of the next compressed data frame,
/// enabling fast frame boundary detection without parsing block headers.
pub const ZSTD_MAGIC_SKIP_MIN: u32 = 0x184D2A50;

/// Upper bound of the magic number range for skippable frames.
/// Valid skippable magic numbers span `0x184D2A50..=0x184D2A5F` (16 values).
pub const ZSTD_MAGIC_SKIP_MAX: u32 = 0x184D2A5F;

/// Size of the magic number field in bytes.
pub const MAGIC_SIZE: usize = 4;

/// Size of a block header in bytes.
/// Layout (little-endian, 24 bits): Last_Block (1 bit) | Block_Type (2 bits) | Block_Size (21 bits).
pub const BLOCK_HEADER_SIZE: usize = 3;

/// Size of a skippable frame header: 4 bytes magic + 4 bytes frame data size.
pub const SKIPPABLE_FRAME_HEADER_SIZE: usize = 8;

/// Size of the optional content checksum (xxHash-64 truncated to 32 bits).
pub const CHECKSUM_SIZE: usize = 4;

/// Default maximum decompressed size per frame (512 MB).
pub const DEFAULT_FRAME_CAPACITY: usize = 512 * 1024 * 1024;

/// Returns `true` if the given magic number falls in the skippable frame range.
#[inline(always)]
pub const fn is_skippable_magic(magic: u32) -> bool {
    magic >= ZSTD_MAGIC_SKIP_MIN && magic <= ZSTD_MAGIC_SKIP_MAX
}

/// Read a 3-byte little-endian block header at the given offset.
/// Returns the raw 24-bit value zero-extended to u32.
#[inline(always)]
pub fn read_block_header(bytes: &[u8], offset: usize) -> Result<u32> {
    let b = bytes
        .get(offset..offset + 3)
        .ok_or(PzstdError::UnexpectedEof {
            offset,
            needed: 3,
            available: bytes.len().saturating_sub(offset),
        })?;
    Ok(u32::from_le_bytes([b[0], b[1], b[2], 0]))
}

/// Read a little-endian u32 at the given offset.
#[inline(always)]
pub fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let b = bytes
        .get(offset..offset + 4)
        .ok_or(PzstdError::UnexpectedEof {
            offset,
            needed: 4,
            available: bytes.len().saturating_sub(offset),
        })?;
    Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

/// Read a little-endian u64 at the given offset.
#[inline(always)]
pub fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let b = bytes
        .get(offset..offset + 8)
        .ok_or(PzstdError::UnexpectedEof {
            offset,
            needed: 8,
            available: bytes.len().saturating_sub(offset),
        })?;
    Ok(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

/// Read a little-endian u16 at the given offset.
#[inline(always)]
pub fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let b = bytes
        .get(offset..offset + 2)
        .ok_or(PzstdError::UnexpectedEof {
            offset,
            needed: 2,
            available: bytes.len().saturating_sub(offset),
        })?;
    Ok(u16::from_le_bytes([b[0], b[1]]))
}

/// Read a single byte at the given offset.
#[inline(always)]
pub fn read_u8(bytes: &[u8], offset: usize) -> Result<u8> {
    bytes.get(offset).copied().ok_or(PzstdError::UnexpectedEof {
        offset,
        needed: 1,
        available: 0,
    })
}
