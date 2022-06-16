use crate::encoding::Encoder;
use crate::TSDataType;
use crate::{utils, IoTDBValue, PositionedWrite};

use std::io::Write;
use std::marker::PhantomData;

pub struct PlainEncoder<T> {
    // pub(crate) values: Vec<T>,
    pub(crate) buffer: Vec<u8>,
    phantom_data: PhantomData<T>,
}

pub(crate) fn new(data_type: TSDataType) -> Box<dyn Encoder> {
    match data_type {
        TSDataType::INT32 => Box::new(PlainEncoder::<i32>::_new()),
        TSDataType::FLOAT => Box::new(PlainEncoder::<f32>::_new()),
        TSDataType::INT64 => Box::new(PlainEncoder::<i64>::_new()),
    }
}

impl<T> PlainEncoder<T> {
    fn _new() -> PlainEncoder<T> {
        Self {
            buffer: Vec::new(),
            phantom_data: PhantomData::default(),
        }
    }
}

impl Encoder for PlainEncoder<f32> {
    fn write(&mut self, value: &IoTDBValue) {
        match value {
            IoTDBValue::FLOAT(v) => {
                self.buffer.write(&(v.to_be_bytes()));
            }
            _ => panic!("Something went wrong!"),
        }
    }
    fn size(&mut self) -> u32 {
        self.buffer.len() as u32
    }
    fn get_max_byte_size(&self) -> u32 {
        // The meaning of 24 is: index(4)+width(4)+minDeltaBase(8)+firstValue(8)
        // (24 + self.buffer.len()) as u32
        0
    }
    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        buffer.write(&self.buffer);
    }

    fn reset(&mut self) {
        self.buffer.clear();
    }
}

impl Encoder for PlainEncoder<i32> {
    fn write(&mut self, value: &IoTDBValue) {
        match value {
            IoTDBValue::INT(v) => {
                utils::write_var_i32(*v, &mut self.buffer);
            }
            _ => panic!("Something went wrong!"),
        }
    }
    fn size(&mut self) -> u32 {
        self.buffer.len() as u32
    }

    fn get_max_byte_size(&self) -> u32 {
        // The meaning of 24 is: index(4)+width(4)+minDeltaBase(8)+firstValue(8)
        (24 + self.buffer.len()) as u32
    }
    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        buffer.write(&self.buffer);
    }

    fn reset(&mut self) {
        self.buffer.clear();
    }
}

impl Encoder for PlainEncoder<i64> {
    fn write(&mut self, value: &IoTDBValue) {
        match value {
            IoTDBValue::LONG(v) => {
                self.buffer.write(&v.to_be_bytes());
            } // self.values.push(*v),
            _ => panic!("Something went wrong!"),
        }
    }

    fn size(&mut self) -> u32 {
        // (&self.values.len() * 8) as u32
        self.buffer.len() as u32
    }
    fn get_max_byte_size(&self) -> u32 {
        // The meaning of 24 is: index(4)+width(4)+minDeltaBase(8)+firstValue(8)
        // (24 + self.values.len() * 8) as u32
        // TODO why is this?
        0
    }
    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        // for val in &self.values {
        //     buffer.write_all(&val.to_be_bytes());
        // }
        buffer.write(&self.buffer);
    }

    fn reset(&mut self) {
        self.buffer.clear();
    }
}

impl PositionedWrite for Vec<u8> {
    fn get_position(&self) -> u64 {
        self.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use crate::encoding::TimeEncoder;

    #[test]
    fn test_long_to_bytes() {
        let mut result = vec![];
        let width = 4;
        TimeEncoder::long_to_bytes(1, &mut result, width * 0, width as u32);
        TimeEncoder::long_to_bytes(1, &mut result, width * 1, width as u32);
        TimeEncoder::long_to_bytes(1, &mut result, width * 2, width as u32);

        assert_eq!(result, [0b00010001, 0b00010000])
    }

    #[test]
    fn test_long_to_bytes_2() {
        let mut result = vec![];
        let width = 7;
        TimeEncoder::long_to_bytes(0b0000001, &mut result, width * 0, width as u32);
        TimeEncoder::long_to_bytes(0b0000001, &mut result, width * 1, width as u32);
        TimeEncoder::long_to_bytes(0b0000001, &mut result, width * 2, width as u32);

        assert_eq!(result, [0b00000010, 0b00000100, 0b00001000])
    }

    #[test]
    fn test_long_to_bytes_3() {
        let mut result = vec![];
        let width = 7;
        TimeEncoder::long_to_bytes(0, &mut result, width * 0, width as u32);
        TimeEncoder::long_to_bytes(81, &mut result, width * 1, width as u32);

        assert_eq!(result, [1, 68])
    }
}
