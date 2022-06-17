#[derive(PartialEq, Copy, Clone)]
pub enum CompressionType {
    UNCOMPRESSED,
    SNAPPY,
}

impl CompressionType {
    pub fn serialize(&self) -> u8 {
        match self {
            CompressionType::UNCOMPRESSED => 0x00,
            CompressionType::SNAPPY => 0x01,
        }
    }
}
