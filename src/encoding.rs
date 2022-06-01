use std::cmp::max;
use std::io::Write;
use crate::{PositionedWrite, utils};

#[derive(Copy, Clone)]
pub enum TSEncoding {
    PLAIN
}

impl TSEncoding {
    pub fn serialize(&self) -> u8 {
        match self {
            TSEncoding::PLAIN => 0
        }
    }
}

pub trait Encoder<DataType> {
    fn encode(&mut self, value: DataType);
    fn serialize(&mut self, buffer: &mut Vec<u8>);
}

pub struct PlainIntEncoder<T> {
    values: Vec<T>,
}

impl PositionedWrite for Vec<u8> {
    fn get_position(&self) -> u64 {
        todo!()
    }
}

impl Encoder<i32> for PlainIntEncoder<i32> {
    fn encode(&mut self, value: i32) {
        self.values.push(value)
    }
    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        for val in &self.values {
            // Do the encoding into writeVarInt
            utils::write_var_i32(*val, buffer);
        }
    }
}


impl Encoder<i64> for PlainIntEncoder<i64> {
    fn encode(&mut self, value: i64) {
        self.values.push(value)
    }
    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        for val in &self.values {
            // FIXME implement this
            // utils::write_var_i32(*val, buffer);
            panic!("Not implemented yet!")
        }
    }
}

impl Encoder<f32> for PlainIntEncoder<f32> {
    fn encode(&mut self, value: f32) {
        self.values.push(value)
    }
    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        for val in &self.values {
            // FIXME implement this
            // utils::write_var_i32(*val, buffer);
            panic!("Not implemented yet!")
        }
    }
}

impl<T> PlainIntEncoder<T> {
    pub(crate) fn new() -> PlainIntEncoder<T> {
        Self {
            values: vec![]
        }
    }
}

pub struct TimeEncoder {
    first_value: Option<i64>,
    min_delta: i64,
    previous_value: i64,
    values: Vec<i64>,
}

impl TimeEncoder {
    fn get_value_width(v: i64) -> u32 {
        return 64 - v.leading_zeros();
    }

    fn calculate_bit_widths_for_delta_block_buffer(&mut self, delta_block_buffer: &Vec<i64>) -> u32 {
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
                true => { 8 - cnt }
                false => { my_width }
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
        }
    }
}

impl Encoder<i64> for TimeEncoder {
    fn encode(&mut self, value: i64) {
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
    }

    #[allow(unused_variables)]
    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        // Preliminary calculations
        let mut delta_block_buffer: Vec<i64> = vec![];

        for delta in &self.values {
            delta_block_buffer.push(delta - self.min_delta);
        }

        let write_width = self.calculate_bit_widths_for_delta_block_buffer(&delta_block_buffer);

        // Write Header
        // Write number of entries
        let number_of_entries: u32 = self.values.len() as u32;
        buffer.write_all(&number_of_entries.to_be_bytes());
        // Write "write-width"
        buffer.write_all(&write_width.to_be_bytes());

        // Min Delta Base
        buffer.write_all(&self.min_delta.to_be_bytes());
        // First Value
        buffer.write_all(&self.first_value.expect("").to_be_bytes());
        // End Header

        // FIXME continue here...
        // now we can drop the long-to-bytes values here
        let mut payload_buffer = vec![];
        for i in 0..delta_block_buffer.len() {
            Self::long_to_bytes(delta_block_buffer[i], &mut payload_buffer, (i * write_width as usize) as usize, write_width);
        }

        let a = (delta_block_buffer.len() * write_width as usize) as f64;
        let b = a / 8.0;
        let encoding_length = b.ceil() as usize;

        // Copy over to "real" buffer
        buffer.write_all(payload_buffer.as_slice());


        // TODO needs to be done right
        // for val in &self.values {
        //     buffer.write(&val.to_be_bytes());
        // }
    }
}

#[cfg(test)]
mod tests {
    use crate::TimeEncoder;

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
