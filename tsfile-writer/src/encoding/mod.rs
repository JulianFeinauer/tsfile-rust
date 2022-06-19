use crate::{IoTDBValue, TSDataType, TsFileError};

pub mod plain;
pub mod time_encoder;

use crate::encoding::plain::PlainEncoder;
use crate::encoding::time_encoder::{IntTs2DiffEncoder, LongTs2DiffEncoder};
use crate::TSEncoding::{PLAIN, TS2DIFF};

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum TSEncoding {
    PLAIN,
    TS2DIFF,
}

impl TryFrom<u8> for TSEncoding {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PLAIN),
            4 => Ok(TS2DIFF),
            _ => Err(())
        }
    }
}

impl TSEncoding {
    pub fn serialize(&self) -> u8 {
        match self {
            TSEncoding::PLAIN => 0,
            TSEncoding::TS2DIFF => 4,
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
    pub(crate) fn new(
        data_type: TSDataType,
        encoding: TSEncoding,
    ) -> Result<Box<dyn Encoder>, TsFileError> {
        match (data_type, encoding) {
            (_, TSEncoding::PLAIN) => Ok(Box::new(PlainEncoder::new(data_type))),
            (TSDataType::INT64, TSEncoding::TS2DIFF) => Ok(Box::new(LongTs2DiffEncoder::new())),
            (TSDataType::INT32, TSEncoding::TS2DIFF) => Ok(Box::new(IntTs2DiffEncoder::new())),
            (_, TSEncoding::TS2DIFF) => Err(TsFileError::Encoding),
        }
    }
}
