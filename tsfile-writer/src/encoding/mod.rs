use crate::{IoTDBValue, TSDataType, TsFileError};

pub mod plain;
pub mod time_encoder;

pub use time_encoder::TimeEncoder;
use crate::encoding::plain::PlainEncoder;

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum TSEncoding {
    PLAIN,
}

impl TSEncoding {
    pub fn serialize(&self) -> u8 {
        match self {
            TSEncoding::PLAIN => 0,
        }
    }
}

pub trait Encoder {
    fn write(&mut self, value: &IoTDBValue) -> Result<(), TsFileError>;
    fn size(&mut self) -> u32;
    fn get_max_byte_size(&self) -> u32;
    fn serialize(&mut self, buffer: &mut Vec<u8>);
    fn reset(&mut self);
}

impl dyn Encoder {
    pub(crate) fn new(data_type: TSDataType, encoding: TSEncoding) -> Box<dyn Encoder> {
        match encoding {
            TSEncoding::PLAIN => Box::new(PlainEncoder::new(data_type)),
        }
    }
}
