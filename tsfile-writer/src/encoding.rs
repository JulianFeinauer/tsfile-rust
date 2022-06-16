use crate::TSDataType;
use crate::{utils, IoTDBValue, PositionedWrite};
use std::cmp::max;
use std::io::Write;
use std::marker::PhantomData;
use crate::utils::{size_var_i32};

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
    fn write(&mut self, value: &IoTDBValue);
    fn size(&mut self) -> u32;
    fn get_max_byte_size(&self) -> u32;
    fn serialize(&mut self, buffer: &mut Vec<u8>);
    fn reset(&mut self);
}

impl dyn Encoder {
    pub(crate) fn new(data_type: TSDataType, encoding: TSEncoding) -> Box<dyn Encoder> {
        match (data_type, encoding) {
            (TSDataType::INT32, TSEncoding::PLAIN) => Box::new(PlainIntEncoder::<i32>::new()),
            (TSDataType::FLOAT, TSEncoding::PLAIN) => Box::new(PlainIntEncoder::<f32>::new()),
            (TSDataType::INT64, TSEncoding::PLAIN) => Box::new(PlainIntEncoder::<i64>::new()),
            _ => panic!(
                "No Encoder implemented for ({:?}, {:?})",
                data_type, encoding
            ),
        }
    }
}

pub struct PlainIntEncoder<T> {
    // pub(crate) values: Vec<T>,
    pub(crate) buffer: Vec<u8>,
    phantom_data: PhantomData<T>
}

impl<T> PlainIntEncoder<T> {
    pub(crate) fn reset(&mut self) {
        self.buffer.clear()
    }
}

impl Encoder for PlainIntEncoder<f32> {
    fn write(&mut self, value: &IoTDBValue) {
        match value {
            IoTDBValue::FLOAT(v) => {
                self.buffer.write(&(v.to_be_bytes()));
            },
            _ => panic!("Something went wrong!"),
        }
    }
    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        buffer.write(&self.buffer);
    }
    fn get_max_byte_size(&self) -> u32 {
        // The meaning of 24 is: index(4)+width(4)+minDeltaBase(8)+firstValue(8)
        // (24 + self.buffer.len()) as u32
        0
    }
    fn reset(&mut self) {
        self.buffer.clear();
    }

    fn size(&mut self) -> u32 {
        self.buffer.len() as u32
    }
}

impl Encoder for PlainIntEncoder<i32> {
    fn write(&mut self, value: &IoTDBValue) {
        match value {
            IoTDBValue::INT(v) => {
                utils::write_var_i32(*v, &mut self.buffer);
            },
            _ => panic!("Something went wrong!"),
        }
    }
    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        buffer.write(&self.buffer);
    }

    fn get_max_byte_size(&self) -> u32 {
        // The meaning of 24 is: index(4)+width(4)+minDeltaBase(8)+firstValue(8)
        (24 + self.buffer.len()) as u32
    }
    fn reset(&mut self) {
        self.buffer.clear();
    }

    fn size(&mut self) -> u32 {
        return self.buffer.len() as u32;
    }
}

impl Encoder for PlainIntEncoder<i64> {
    fn write(&mut self, value: &IoTDBValue) {
        match value {
            IoTDBValue::LONG(v) => {
                self.buffer.write(&v.to_be_bytes());
            }, // self.values.push(*v),
            _ => panic!("Something went wrong!"),
        }
    }

    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        // for val in &self.values {
        //     buffer.write_all(&val.to_be_bytes());
        // }
        buffer.write(&self.buffer);
    }
    fn get_max_byte_size(&self) -> u32 {
        // The meaning of 24 is: index(4)+width(4)+minDeltaBase(8)+firstValue(8)
        // (24 + self.values.len() * 8) as u32
        // TODO why is this?
        0
    }
    fn reset(&mut self) {
        self.buffer.clear();
    }

    fn size(&mut self) -> u32 {
        // (&self.values.len() * 8) as u32
        self.buffer.len() as u32
    }
}

impl PositionedWrite for Vec<u8> {
    fn get_position(&self) -> u64 {
        self.len() as u64
    }
}

impl<T> PlainIntEncoder<T> {
    pub(crate) fn new() -> PlainIntEncoder<T> {
        Self {
            buffer: Vec::new(),
            phantom_data: PhantomData::default()
        }
    }
}

pub struct TimeEncoder {
    first_value: Option<i64>,
    min_delta: i64,
    previous_value: i64,
    values: Vec<i64>,
    buffer: Vec<u8>,
}

impl TimeEncoder {
    pub(crate) fn reset(&mut self) {
        self.first_value = None;
        self.min_delta = i64::MAX;
        self.previous_value = i64::MAX;
        self.values.clear();
        self.buffer.clear();
    }
}

impl TimeEncoder {
    pub(crate) fn size(&self) -> u32 {
        self.buffer.len() as u32
    }
    pub(crate) fn get_max_byte_size(&self) -> u32 {
        // The meaning of 24 is: index(4)+width(4)+minDeltaBase(8)+firstValue(8)
        (24 + self.values.len() * 8) as u32
    }
}

impl TimeEncoder {
    fn get_value_width(v: i64) -> u32 {
        return 64 - v.leading_zeros();
    }

    fn calculate_bit_widths_for_delta_block_buffer(
        &mut self,
        delta_block_buffer: &Vec<i64>,
    ) -> u32 {
        let mut width = 0;

        for i in 0..delta_block_buffer.len() {
            let v = *delta_block_buffer.get(i).expect("");
            let value_width = Self::get_value_width(v);
            width = max(width, value_width)
        }

        return width;
    }

    fn long_to_bytes(number: i64, result: &mut Vec<u8>, pos: usize, width: u32) {
        let mut cnt = (pos & 0x07) as u8;
        let mut index = pos >> 3;

        let mut my_width = width as u8;
        let mut my_number = number;
        while my_width > 0 {
            let m = match my_width + cnt >= 8 {
                true => 8 - cnt,
                false => my_width,
            };
            my_width = my_width - m;
            let old_count = cnt;
            cnt = cnt + m;
            let y = (number >> my_width) as u8;
            let y = y << (8 - cnt);

            // We need a mask like that
            // 0...0 (old-cnt-times) 1...1 (8-old-cnt-times)
            let mut new_mask: u8 = 0;
            for i in 0..(8 - old_count) {
                new_mask = new_mask | (1 << i);
            }
            new_mask = !new_mask;

            if index <= result.len() {
                result.resize(index + 1, 0);
            }
            let masked_input = result[index] & new_mask;
            let new_input = masked_input | y;
            result[index] = new_input;
            // Remove the written numbers
            let mut mask: i64 = 0;
            for i in 0..my_width {
                mask = mask | (1 << i);
            }
            my_number = my_number & mask;

            if cnt == 8 {
                index = index + 1;
                cnt = 0;
            }
        }
    }
}

impl TimeEncoder {
    pub(crate) fn new() -> TimeEncoder {
        TimeEncoder {
            first_value: None,
            min_delta: i64::MAX,
            previous_value: i64::MAX,
            values: vec![],
            buffer: vec![],
        }
    }

    fn flush(&mut self) {
        if self.first_value == None {
            return;
        }
        // Preliminary calculations
        let mut delta_block_buffer: Vec<i64> = vec![];

        for delta in &self.values {
            delta_block_buffer.push(delta - self.min_delta);
        }

        let write_width = self.calculate_bit_widths_for_delta_block_buffer(&delta_block_buffer);

        // Write Header
        // Write number of entries
        let number_of_entries: u32 = self.values.len() as u32;
        self.buffer.write_all(&number_of_entries.to_be_bytes());
        // Write "write-width"
        self.buffer.write_all(&write_width.to_be_bytes());

        // Min Delta Base
        self.buffer.write_all(&self.min_delta.to_be_bytes());
        // First Value
        self.buffer
            .write_all(&self.first_value.expect("").to_be_bytes());
        // End Header

        // now we can drop the long-to-bytes values here
        let mut payload_buffer = vec![];
        for i in 0..delta_block_buffer.len() {
            Self::long_to_bytes(
                delta_block_buffer[i],
                &mut payload_buffer,
                (i * write_width as usize) as usize,
                write_width,
            );
        }

        let a = (delta_block_buffer.len() * write_width as usize) as f64;
        let b = a / 8.0;
        let encoding_length = b.ceil() as usize;

        // Copy over to "real" buffer
        self.buffer.write_all(payload_buffer.as_slice());

        // Now reset everything
        self.values.clear();
        self.first_value = None;
        self.previous_value = 0;
        self.min_delta = i64::MAX;
    }
}

impl TimeEncoder {
    pub(crate) fn encode(&mut self, value: i64) {
        match self.first_value {
            None => {
                self.first_value = Some(value);
                self.previous_value = value;
            }
            Some(_) => {
                // calc delta
                let delta = value - self.previous_value;
                // If delta is min, store it
                if delta < self.min_delta {
                    self.min_delta = delta;
                }
                // store delta
                self.values.push(delta);
                self.previous_value = value;
            }
        }
        if self.values.len() == 128 {
            self.flush();
        }
    }

    #[allow(unused_variables)]
    pub(crate) fn serialize(&mut self, buffer: &mut Vec<u8>) {
        // Flush
        self.flush();
        // Copy internal buffer to out buffer
        buffer.write_all(&self.buffer);
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
