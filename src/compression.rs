#[derive(PartialEq, Copy, Clone)]
pub enum CompressionType {
    UNCOMPRESSED
}

impl CompressionType {
    pub(crate) fn serialize(&self) -> u8 {
        match self {
            CompressionType::UNCOMPRESSED => 0
        }
    }
}
