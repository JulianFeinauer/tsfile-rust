use crate::{PositionedWrite, TsFileError};
use std::io::Read;

pub fn write_var_u32(num: u32, buffer: &mut dyn PositionedWrite) -> Result<u8, TsFileError> {
    let mut number = num;

    // Now compress them
    let mut position: u8 = 1;

    while (number & 0xFFFFFF80) != 0 {
        buffer.write_all(&[((number & 0x7F) | 0x80) as u8])?;
        number >>= 7;
        position += 1;
    }

    buffer.write_all(&[(number & 0x7F) as u8])?;

    Ok(position)
}

pub fn size_var_i32(num: i32) -> u8 {
    let mut u_value = num << 1;
    if num < 0 {
        u_value = !u_value;
    }
    size_var_u32(u_value as u32)
}

pub fn size_var_u32(num: u32) -> u8 {
    let mut position = 1;
    let mut value = num;
    while (value & 0xFFFFFF80) != 0 {
        value >>= 7;
        position += 1;
    }
    position
}

pub fn write_var_i32(num: i32, buffer: &mut dyn PositionedWrite) -> Result<u8, TsFileError> {
    let mut u_value = num << 1;
    if num < 0 {
        u_value = !u_value;
    }
    write_var_u32(u_value as u32, buffer)
}

#[allow(dead_code)]
fn read_byte(buffer: &mut dyn Read) -> Result<u8, TsFileError> {
    let mut read_buffer: [u8; 1] = [0];
    buffer.read_exact(&mut read_buffer)?;
    Ok(read_buffer[0])
}

#[allow(dead_code)]
pub fn read_var_u32(buffer: &mut dyn Read) -> Result<u32, TsFileError> {
    let mut value: u32 = 0;
    let mut i: u8 = 0;
    let mut b = read_byte(buffer)?;
    while b != u8::MAX && (b & 0x80) != 0 {
        value |= ((b & 0x7F) as u32) << i;
        i += 7;
        b = read_byte(buffer)?;
    }
    Ok(value | ((b as u32) << i))
}
