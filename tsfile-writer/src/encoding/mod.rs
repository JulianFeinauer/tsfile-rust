use crate::{IoTDBValue, TSDataType, TsFileError};

pub mod plain;
pub mod time_encoder;
mod ts2diff;

use crate::encoding::plain::PlainEncoder;
pub use time_encoder::Ts2DiffEncoder;
use crate::encoding::ts2diff::Ts2DiffEncoder2;

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum TSEncoding {
    PLAIN,
    TS2DIFF
}

impl TSEncoding {
    pub fn serialize(&self) -> u8 {
        match self {
            TSEncoding::PLAIN => 0,
            TSEncoding::TS2DIFF => 4
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
    pub(crate) fn new(data_type: TSDataType, encoding: TSEncoding) -> Result<Box<dyn Encoder>, TsFileError> {
        Ok(match encoding {
            TSEncoding::PLAIN => Box::new(PlainEncoder::new(data_type)),
            TSEncoding::TS2DIFF => ts2diff::new(data_type)?,
        })
    }
}
