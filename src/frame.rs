use crate::{
    block::{BlockHeader, BlockType},
    error::{PzstdError, Result},
    helpers::{
        BLOCK_HEADER_SIZE, CHECKSUM_SIZE, MAGIC_SIZE, SKIPPABLE_FRAME_HEADER_SIZE,
        ZSTD_MAGIC_NUMBER, is_skippable_magic, read_block_header, read_u8, read_u16, read_u32,
        read_u64,
    },
};

#[derive(Debug, Clone)]
pub enum FrameKind {
    Data,
    Skippable,
}

impl FrameKind {
    /// Determine frame kind from a magic number.
    pub fn from_magic(magic: u32) -> Result<Self> {
        if magic == ZSTD_MAGIC_NUMBER {
            Ok(FrameKind::Data)
        } else if is_skippable_magic(magic) {
            Ok(FrameKind::Skippable)
        } else {
            Err(PzstdError::InvalidMagic {
                offset: 0,
                found: magic,
            })
        }
    }
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub offset: usize,
    pub len: usize,
    pub kind: FrameKind,
    /// Decompressed size of this frame, if known from the frame header.
    /// Only present for Data frames that have Frame_Content_Size set.
    pub decompressed_size: Option<u64>,
}

/// Controls which frame types are returned by the scanner.
#[derive(Debug, Clone, PartialEq)]
pub enum FrameScanMode {
    /// Return all frames (data + skippable).
    All,
    /// Return only data frames, skip metadata frames.
    DataOnly,
}

impl Frame {
    pub fn scan_frames(input: &[u8], mode: FrameScanMode) -> Result<Vec<Frame>> {
        let mut frames = Vec::new();
        let mut pos = 0;

        while pos < input.len() {
            let frame_start = pos;

            let magic = read_u32(input, pos)?;
            pos += MAGIC_SIZE;
            let kind = FrameKind::from_magic(magic)?;

            let mut decompressed_size = None;

            match kind {
                FrameKind::Skippable => {
                    let frame_size = read_u32(input, pos)? as usize;
                    pos += SKIPPABLE_FRAME_HEADER_SIZE + frame_size;
                }
                FrameKind::Data => {
                    let desc_byte = read_u8(input, pos)?;
                    let desc = FrameDescriptor::parse(desc_byte);

                    // parse Frame_Content_Size before skipping the header
                    // FCS field sits at: descriptor(1) + window + did
                    let window_size = if desc.single_segment { 0 } else { 1 };
                    let did_size = desc.did_field_size();
                    let fcs_offset = pos + 1 + window_size + did_size;
                    decompressed_size = desc.parse_fcs(input, fcs_offset)?;

                    pos += desc.header_size();

                    loop {
                        let raw = read_block_header(input, pos)?;
                        let block = BlockHeader::parse(raw, pos)?;
                        pos += BLOCK_HEADER_SIZE;

                        pos += match block.block_type {
                            BlockType::Rle => 1,
                            _ => block.size as usize,
                        };

                        if block.last {
                            break;
                        }
                    }

                    if desc.has_checksum {
                        pos += CHECKSUM_SIZE;
                    }
                }
            }

            let should_record = match mode {
                FrameScanMode::All => true,
                FrameScanMode::DataOnly => matches!(kind, FrameKind::Data),
            };

            if should_record {
                frames.push(Frame {
                    offset: frame_start,
                    len: pos - frame_start,
                    kind,
                    decompressed_size,
                });
            }
        }

        if frames.is_empty() {
            return Err(PzstdError::NoFrames);
        }

        Ok(frames)
    }

    /// Extract this frame's raw bytes from the input buffer.
    ///
    /// # Errors
    /// Returns [`PzstdError::UnexpectedEof`] if the frame's range
    /// exceeds the input buffer bounds.
    pub fn bytes<'a>(&self, input: &'a [u8]) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(self.len)
            .ok_or(PzstdError::UnexpectedEof {
                offset: self.offset,
                needed: self.len,
                available: input.len().saturating_sub(self.offset),
            })?;

        input
            .get(self.offset..end)
            .ok_or(PzstdError::UnexpectedEof {
                offset: self.offset,
                needed: self.len,
                available: input.len().saturating_sub(self.offset),
            })
    }
}

/// Parsed flags from the Frame_Header_Descriptor byte.
#[derive(Debug, Clone)]
pub struct FrameDescriptor {
    /// Frame_Content_Size_Flag (bits 7-6). Determines FCS field size.
    pub fcs_flag: u8,
    /// Single_Segment_Flag (bit 5). If set, no Window_Descriptor field.
    pub single_segment: bool,
    /// Content_Checksum_Flag (bit 2). If set, 4-byte checksum after last block.
    pub has_checksum: bool,
    /// Dictionary_ID_Flag (bits 1-0). Determines DID field size.
    pub did_flag: u8,
}

impl FrameDescriptor {
    /// Parse the Frame_Header_Descriptor byte.
    ///
    /// Layout:
    ///   bits 7-6: FCS_Flag
    ///   bit 5:    Single_Segment_Flag
    ///   bit 4:    unused
    ///   bit 3:    unused
    ///   bit 2:    Content_Checksum_Flag
    ///   bits 1-0: DID_Flag
    pub fn parse(byte: u8) -> Self {
        Self {
            fcs_flag: (byte >> 6) & 0x3,
            single_segment: (byte >> 5) & 1 == 1,
            has_checksum: (byte >> 2) & 1 == 1,
            did_flag: byte & 0x3,
        }
    }

    /// Size of the Dictionary_ID field in bytes.
    pub fn did_field_size(&self) -> usize {
        match self.did_flag {
            0 => 0,
            1 => 1,
            2 => 2,
            3 => 4,
            _ => unreachable!(),
        }
    }

    /// Size of the Frame_Content_Size field in bytes.
    pub fn fcs_field_size(&self) -> usize {
        match self.fcs_flag {
            0 => {
                if self.single_segment {
                    1
                } else {
                    0
                }
            }
            1 => 2,
            2 => 4,
            3 => 8,
            _ => unreachable!(),
        }
    }

    /// Size of the full frame header including the descriptor byte.
    /// This is needed to skip past the header to reach the first block.
    pub fn header_size(&self) -> usize {
        let window_size = if self.single_segment { 0 } else { 1 };
        1 + window_size + self.did_field_size() + self.fcs_field_size()
    }

    /// Parse the Frame_Content_Size value from the input.
    ///
    /// Note: the 2-byte case adds 256 to the value per the zstd spec (RFC 8878).
    /// Returns None if FCS field is not present in this frame.
    pub fn parse_fcs(&self, input: &[u8], offset: usize) -> Result<Option<u64>> {
        match self.fcs_field_size() {
            0 => Ok(None),
            1 => Ok(Some(read_u8(input, offset)? as u64)),
            2 => Ok(Some(read_u16(input, offset)? as u64 + 256)),
            4 => Ok(Some(read_u32(input, offset)? as u64)),
            8 => Ok(Some(read_u64(input, offset)?)),
            _ => unreachable!(),
        }
    }
}
