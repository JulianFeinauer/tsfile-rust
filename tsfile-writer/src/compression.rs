//! Contains the compression algorithms
use crate::CompressionType::{SNAPPY, UNCOMPRESSED};

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum CompressionType {
    UNCOMPRESSED,
    SNAPPY,
}

impl TryFrom<u8> for CompressionType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(UNCOMPRESSED),
            0x01 => Ok(SNAPPY),
            _ => Err(()),
        }
    }
}

impl CompressionType {
    pub fn serialize(&self) -> u8 {
        match self {
            CompressionType::UNCOMPRESSED => 0x00,
            CompressionType::SNAPPY => 0x01,
        }
    }
}
