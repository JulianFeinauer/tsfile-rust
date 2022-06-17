use std::cmp::{max, Ordering};
use std::io::Write;
use std::mem;
use std::ops::{Deref, Sub};
use crate::encoding::Encoder;
use crate::{IoTDBValue, TSDataType, TsFileError};

pub trait Numeric: Sized + Copy + Sub<Output=Self> + Ord {
    const MAX: Self;
    const ZERO: Self;
    const NUM_BITS: u8;

    fn leading_zeros(&self) -> u32;
    fn write_be_bytes<T: Write>(&self, writer: &mut T) -> Result<(), TsFileError>;
    fn x_to_bytes(number: Self, result: &mut Vec<u8>, pos: usize, width: u32);
    fn from_iotdb_value(value: &IoTDBValue) -> Result<Self, TsFileError>;
}

impl Numeric for i64 {
    const MAX: Self = i64::MAX;
    const ZERO: Self = 0;
    const NUM_BITS: u8 = 64;

    fn leading_zeros(&self) -> u32 {
        self.leading_zeros()
    }

    fn write_be_bytes<T: Write>(&self, writer: &mut T) -> Result<(), TsFileError> {
        writer.write_all(&self.to_be_bytes())?;
        Ok(())
    }

    #[allow(unused_variables)]
    fn x_to_bytes(number: Self, result: &mut Vec<u8>, pos: usize, width: u32) {
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

    fn from_iotdb_value(value: &IoTDBValue) -> Result<Self, TsFileError> {
        match value {
            IoTDBValue::LONG(v) => {
                Ok(*v)
            },
            _ => {
                Err(TsFileError::WrongTypeForSeries)
            }
        }
    }
}

impl Numeric for i32 {
    const MAX: Self = i32::MAX;
    const ZERO: Self = 0;
    const NUM_BITS: u8 = 32;

    fn leading_zeros(&self) -> u32 {
        self.leading_zeros()
    }

    fn write_be_bytes<T: Write>(&self, writer: &mut T) -> Result<(), TsFileError> {
        writer.write_all(&self.to_be_bytes())?;
        Ok(())
    }

    fn x_to_bytes(number: Self, result: &mut Vec<u8>, pos: usize, width: u32) {
        todo!()
    }

    fn from_iotdb_value(value: &IoTDBValue) -> Result<Self, TsFileError> {
        match value {
            IoTDBValue::INT(v) => {
                Ok(*v)
            },
            _ => {
                Err(TsFileError::WrongTypeForSeries)
            }
        }
    }
}


pub struct Ts2DiffEncoder2<T: Numeric> {
    first_value: Option<T>,
    min_delta: T,
    previous_value: T,
    values: Vec<T>,
    buffer: Vec<u8>,
}

impl<T: Numeric> Ts2DiffEncoder2<T> {
    pub(crate) fn reset(&mut self) {
        self.first_value = None;
        self.min_delta = T::MAX;
        self.previous_value = T::MAX;
        self.values.clear();
        self.buffer.clear();
    }
}

impl<T: Numeric> Encoder for Ts2DiffEncoder2<T> {
    fn write(&mut self, value: &IoTDBValue) -> Result<(), TsFileError> {
        // TODO renove unwrap
        let value: T = T::from_iotdb_value(value)?;
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
        (24 + self.values.len() * 8) as u32
    }

    fn serialize(&mut self, buffer: &mut Vec<u8>) {
        // Flush
        self.flush();
        // Copy internal buffer to out buffer
        buffer.write_all(&self.buffer);
    }

    fn reset(&mut self) {
        // Now reset everything
        self.values.clear();
        self.first_value = None;
        self.previous_value = T::ZERO;
        self.min_delta = T::MAX;
    }
}

pub(crate) fn new(data_type: TSDataType) -> Result<Box<dyn Encoder>, TsFileError> {
        match data_type {
            TSDataType::INT32 => {
                Ok(Box::new(Ts2DiffEncoder2::<i32>::_new()))
            }
            TSDataType::INT64 => {
                Ok(Box::new(Ts2DiffEncoder2::<i64>::_new()))
            }
            _ => {
                Err(TsFileError::Encoding)
            }
        }
    }

impl<T: Numeric> Ts2DiffEncoder2<T> {

    fn _new() -> Ts2DiffEncoder2<T> {
        Ts2DiffEncoder2 {
            first_value: None,
            min_delta: T::MAX,
            previous_value: T::MAX,
            values: vec![],
            buffer: vec![],
        }
    }

    fn get_value_width(v: T) -> u32 {
        T::NUM_BITS as u32 - v.leading_zeros()
    }

    fn calculate_bit_widths_for_delta_block_buffer(&mut self, delta_block_buffer: &[T]) -> u32 {
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
        let mut delta_block_buffer: Vec<T> = vec![];

        for delta in &self.values {
            delta_block_buffer.push(*delta - self.min_delta);
        }

        let write_width = self.calculate_bit_widths_for_delta_block_buffer(&delta_block_buffer);

        // Write Header
        // Write number of entries
        let number_of_entries: u32 = self.values.len() as u32;
        self.buffer.write_all(&number_of_entries.to_be_bytes());
        // Write "write-width"
        self.buffer.write_all(&write_width.to_be_bytes());

        // Min Delta Base
        self.min_delta.write_be_bytes(&mut self.buffer);
        // First Value
        self.first_value.expect("").write_be_bytes(&mut self.buffer);
        // End Header

        // now we can drop the long-to-bytes values here
        let mut payload_buffer = vec![];
        for (i, buffer) in delta_block_buffer.iter().enumerate() {
            T::x_to_bytes(
                *buffer,
                &mut payload_buffer,
                (i * write_width as usize) as usize,
                write_width,
            );
        }

        // Copy over to "real" buffer
        self.buffer.write_all(payload_buffer.as_slice());

        // Now reset everything
        self.reset();
    }
}

#[cfg(test)]
mod tests {
    use crate::encoding::ts2diff::Numeric;
    use crate::encoding::Ts2DiffEncoder2;

    #[test]
    fn test_long_to_bytes() {
        let mut result = vec![];
        let width = 4;
        i64::x_to_bytes(1, &mut result, width * 0, width as u32);
        i64::x_to_bytes(1, &mut result, width * 1, width as u32);
        i64::x_to_bytes(1, &mut result, width * 2, width as u32);

        assert_eq!(result, [0b00010001, 0b00010000])
    }

    #[test]
    fn test_long_to_bytes_2() {
        let mut result = vec![];
        let width = 7;
        i64::x_to_bytes(0b0000001, &mut result, width * 0, width as u32);
        i64::x_to_bytes(0b0000001, &mut result, width * 1, width as u32);
        i64::x_to_bytes(0b0000001, &mut result, width * 2, width as u32);

        assert_eq!(result, [0b00000010, 0b00000100, 0b00001000])
    }

    #[test]
    fn test_long_to_bytes_3() {
        let mut result = vec![];
        let width = 7;
        i64::x_to_bytes(0, &mut result, width * 0, width as u32);
        i64::x_to_bytes(81, &mut result, width * 1, width as u32);

        assert_eq!(result, [1, 68])
    }
}
