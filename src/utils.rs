use std::io::{Read};
use crate::PositionedWrite;

pub fn write_var_u32(num: u32, buffer: &mut dyn PositionedWrite) -> u8 {
    let mut number = num.clone();

    // Now compress them
    let mut position: u8 = 1;

    while (number & 0xFFFFFF80) != 0 {
        buffer.write_all(&[((number & 0x7F) | 0x80) as u8]);
        number = number >> 7;
        position = position + 1;
    }

    buffer.write_all(&[(number & 0x7F) as u8]);

    return position;
}

pub fn write_var_i32(num: i32, buffer: &mut dyn PositionedWrite) -> u8 {
    let mut u_value = num << 1;
    if num < 0 {
        u_value = !u_value;
    }
    return write_var_u32(u_value as u32, buffer);
}

fn read_byte(buffer: &mut dyn Read) -> u8 {
    let mut read_buffer: [u8; 1] = [0];
    buffer.read(&mut read_buffer).expect("Prblem");
    return read_buffer[0];
}

pub fn read_var_u32(buffer: &mut dyn Read) -> u32 {
    let mut value: u32 = 0;
    let mut i: u8 = 0;
    let mut b = read_byte(buffer);
    while b != u8::MAX && (b & 0x80) != 0 {
        value = value | (((b & 0x7F) as u32) << i);
        i = i + 7;
        b = read_byte(buffer);
    }
    return value | ((b as u32) << i);
}
