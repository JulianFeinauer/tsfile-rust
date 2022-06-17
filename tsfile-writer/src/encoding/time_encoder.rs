use std::cmp::max;
use std::io::Write;

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
        64 - v.leading_zeros()
    }

    fn calculate_bit_widths_for_delta_block_buffer(&mut self, delta_block_buffer: &[i64]) -> u32 {
        let mut width = 0;

        for i in 0..delta_block_buffer.len() {
            let v = *delta_block_buffer.get(i).expect("");
            let value_width = Self::get_value_width(v);
            width = max(width, value_width)
        }

        width
    }

    #[allow(unused_variables)]
    pub(crate) fn long_to_bytes(number: i64, result: &mut Vec<u8>, pos: usize, width: u32) {
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
        for (i, buffer) in delta_block_buffer.iter().enumerate() {
            Self::long_to_bytes(
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
