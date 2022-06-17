use std::io::Write;
use std::marker::PhantomData;

use crate::encoding::Encoder;
use crate::{utils, IoTDBValue, PositionedWrite};
use crate::{TSDataType, TsFileError};

pub struct PlainEncoder {
    data_type: TSDataType,
    pub(crate) buffer: Vec<u8>,
}

impl PlainEncoder {
    pub(crate) fn new(data_type: TSDataType) -> PlainEncoder {
        Self {
            data_type,
            buffer: Vec::new(),
        }
    }
}

impl Encoder for PlainEncoder {
    fn write(&mut self, value: &IoTDBValue) -> Result<(), TsFileError> {
        match value {
            IoTDBValue::DOUBLE(v) => {
                self.buffer.write_all(&v.to_be_bytes())?;
            }
            IoTDBValue::FLOAT(v) => {
                self.buffer.write_all(&v.to_be_bytes())?;
            }
            IoTDBValue::INT(v) => {
                utils::write_var_i32(*v, &mut self.buffer)?;
            }
            IoTDBValue::LONG(v) => {
                self.buffer.write_all(&v.to_be_bytes())?;
            }
        };
        Ok(())
    }

    fn size(&mut self) -> u32 {
        self.buffer.len() as u32
    }

    fn get_max_byte_size(&self) -> u32 {
        match self.data_type {
            TSDataType::INT32 => (24 + self.buffer.len()) as u32,
            TSDataType::INT64 => 0,
            TSDataType::FLOAT => 0,
        }
    }
    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        buffer.write(&self.buffer);
    }

    fn reset(&mut self) {
        self.buffer.clear();
    }
}
