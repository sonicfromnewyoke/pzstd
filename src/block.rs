use crate::error::{PzstdError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum BlockType {
    /// Raw block (type 0): uncompressed data stored as-is.
    /// Block_Size equals the number of data bytes that follow.
    Raw,

    /// RLE block (type 1): a single byte repeated Block_Size times.
    /// Only 1 byte of data follows the header, regardless of Block_Size.
    Rle,

    /// Compressed block (type 2): data compressed using Huffman and FSE.
    /// Block_Size is the compressed size; decompressed size may be larger.
    Compressed,

    /// Reserved (type 3): not defined by the spec. Encountering this is an error.
    Reserved,
}

#[derive(Debug, Clone)]
pub struct BlockHeader {
    pub last: bool,
    pub block_type: BlockType,
    pub size: u32,
}

impl BlockHeader {
    /// Parse a block header from a 24-bit raw value (3 bytes, little-endian).
    ///
    /// Layout:
    ///   bit 0:     Last_Block
    ///   bits 1-2:  Block_Type
    ///   bits 3-23: Block_Size
    pub fn parse(raw: u32, offset: usize) -> Result<Self> {
        let last = (raw & 1) != 0;
        let btype = (raw >> 1) & 0x3;
        let size = (raw >> 3) & 0x1FFFFF;

        let block_type = match btype {
            0 => BlockType::Raw,
            1 => BlockType::Rle,
            2 => BlockType::Compressed,
            3 => {
                return Err(PzstdError::InvalidBlockType {
                    offset,
                    block_type: 3,
                });
            }
            _ => unreachable!(),
        };

        Ok(Self {
            last,
            block_type,
            size,
        })
    }
}
