use crate::encoding::Encoder;
use crate::{IoTDBValue, TsFileError};
use std::cmp::max;
use std::io::Write;

impl TryFrom<&IoTDBValue> for i64 {
    type Error = TsFileError;

    fn try_from(value: &IoTDBValue) -> Result<Self, Self::Error> {
        match value {
            IoTDBValue::LONG(v) => Ok(*v),
            _ => Err(TsFileError::WrongTypeForSeries),
        }
    }
}

impl TryFrom<&IoTDBValue> for i32 {
    type Error = TsFileError;

    fn try_from(value: &IoTDBValue) -> Result<Self, Self::Error> {
        match value {
            IoTDBValue::INT(v) => Ok(*v),
            _ => Err(TsFileError::WrongTypeForSeries),
        }
    }
}

#[macro_export]
macro_rules! ts2diff_encoder {
    ( $name:ident, $type:ty, $num_bits:expr ) => {
        pub struct $name {
            first_value: Option<$type>,
            min_delta: $type,
            previous_value: $type,
            values: Vec<$type>,
            buffer: Vec<u8>,
        }

        impl Encoder for $name {
            fn write(&mut self, value: &IoTDBValue) -> Result<(), TsFileError> {
                let value = value.try_into()?;
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
                Ok(())
            }

            fn size(&mut self) -> u32 {
                self.buffer.len() as u32
            }
            fn get_max_byte_size(&self) -> u32 {
                // The meaning of 24 is: index(4)+width(4)+minDeltaBase(8)+firstValue(8)
                (24 + self.values.len() * $num_bits / 8) as u32
            }

            fn serialize(&mut self, buffer: &mut Vec<u8>) {
                // Flush
                self.flush();
                // Copy internal buffer to out buffer
                buffer.write_all(&self.buffer);
            }

            fn reset(&mut self) {
                // Now reset everything
                self.first_value = None;
                self.min_delta = <$type>::MAX;
                self.previous_value = <$type>::MAX;
                self.values.clear();
                self.buffer.clear();
            }
        }

        impl $name {
            pub(crate) fn new() -> $name {
                Self {
                    first_value: None,
                    min_delta: <$type>::MAX,
                    previous_value: <$type>::MAX,
                    values: vec![],
                    buffer: vec![],
                }
            }

            fn get_value_width(v: $type) -> u32 {
                $num_bits - v.leading_zeros()
            }

            fn calculate_bit_widths_for_delta_block_buffer(
                &mut self,
                delta_block_buffer: &[$type],
            ) -> u32 {
                let mut width = 0;

                for i in 0..delta_block_buffer.len() {
                    let v = *delta_block_buffer.get(i).expect("");
                    let value_width = Self::get_value_width(v);
                    width = max(width, value_width)
                }

                width
            }

            fn flush(&mut self) {
                if self.first_value == None {
                    return;
                }
                // Preliminary calculations
                let mut delta_block_buffer: Vec<$type> = vec![];

                for delta in &self.values {
                    delta_block_buffer.push(delta - self.min_delta);
                }

                let write_width =
                    self.calculate_bit_widths_for_delta_block_buffer(&delta_block_buffer);

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
                for (i, buffer) in delta_block_buffer.iter().enumerate() {
                    Self::to_bytes(
                        *buffer,
                        &mut payload_buffer,
                        (i * write_width as usize) as usize,
                        write_width,
                    );
                }

                // Copy over to "real" buffer
                self.buffer.write_all(payload_buffer.as_slice());

                // Now reset everything
                self.values.clear();
                self.first_value = None;
                self.previous_value = 0;
                self.min_delta = <$type>::MAX;
            }
        }
    };
}

ts2diff_encoder!(LongTs2DiffEncoder, i64, 64);
ts2diff_encoder!(IntTs2DiffEncoder, i32, 32);

impl LongTs2DiffEncoder {
    #[allow(unused_variables)]
    pub(crate) fn to_bytes(number: i64, result: &mut Vec<u8>, pos: usize, width: u32) {
        let mut cnt = (pos & 0x07) as u8;
        let mut index = pos >> 3;

        let mut my_width = width as u8;
        let mut my_number = number;
        while my_width > 0 {
            let m = match my_width + cnt >= 8 {
                true => 8 - cnt,
                false => my_width,
            };
            my_width -= m;
            let old_count = cnt;
            cnt += m;
            let y = (number >> my_width) as u8;
            let y = y << (8 - cnt);

            // We need a mask like that
            // 0...0 (old-cnt-times) 1...1 (8-old-cnt-times)
            let mut new_mask: u8 = 0;
            for i in 0..(8 - old_count) {
                new_mask |= 1 << i;
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
                mask |= 1 << i;
            }
            my_number &= mask;

            if cnt == 8 {
                index += 1;
                cnt = 0;
            }
        }
    }
}

impl IntTs2DiffEncoder {
    pub(crate) fn to_bytes(number: i32, result: &mut Vec<u8>, pos: usize, width: u32) {
        let mut cnt = (pos & 0x07) as u8;
        let mut index = pos >> 3;

        let mut my_width = width as u8;
        let mut my_number = number;

        while my_width > 0 {
            let m = if my_width + cnt >= 8 {
                8 - cnt
            } else {
                my_width
            };
            my_width -= m;
            let mut mask = (1 << (8 - cnt)) as i32;
            cnt += m;
            let mut y = (my_number >> my_width) as u8;
            y <<= 8 - cnt;
            mask = !(mask - (1 << (8 - cnt)) as i32);

            if index <= result.len() {
                result.resize(index + 1, 0);
            }

            result[index] = result[index] & (mask as u8) | y;
            my_number &= !(-1 << my_width);
            if cnt == 8 {
                index += 1;
                cnt = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::encoding::time_encoder::{IntTs2DiffEncoder, LongTs2DiffEncoder};

    #[test]
    fn test_long_to_bytes() {
        let mut result = vec![];
        let width = 4;
        LongTs2DiffEncoder::to_bytes(1, &mut result, width * 0, width as u32);
        LongTs2DiffEncoder::to_bytes(1, &mut result, width * 1, width as u32);
        LongTs2DiffEncoder::to_bytes(1, &mut result, width * 2, width as u32);

        assert_eq!(result, [0b00010001, 0b00010000])
    }

    #[test]
    fn test_long_to_bytes_2() {
        let mut result = vec![];
        let width = 7;
        LongTs2DiffEncoder::to_bytes(0b0000001, &mut result, width * 0, width as u32);
        LongTs2DiffEncoder::to_bytes(0b0000001, &mut result, width * 1, width as u32);
        LongTs2DiffEncoder::to_bytes(0b0000001, &mut result, width * 2, width as u32);

        assert_eq!(result, [0b00000010, 0b00000100, 0b00001000])
    }

    #[test]
    fn test_long_to_bytes_3() {
        let mut result = vec![];
        let width = 7;
        LongTs2DiffEncoder::to_bytes(0, &mut result, width * 0, width as u32);
        LongTs2DiffEncoder::to_bytes(81, &mut result, width * 1, width as u32);

        assert_eq!(result, [1, 68])
    }

    #[test]
    fn test_int_to_bytes() {
        let mut result = vec![];
        let width = 4;
        IntTs2DiffEncoder::to_bytes(1, &mut result, width * 0, width as u32);
        IntTs2DiffEncoder::to_bytes(1, &mut result, width * 1, width as u32);
        IntTs2DiffEncoder::to_bytes(1, &mut result, width * 2, width as u32);

        assert_eq!(result, [0b00010001, 0b00010000])
    }
}
