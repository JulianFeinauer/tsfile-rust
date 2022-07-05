extern crate core;
extern crate libc;

use libc::c_char;
use std::ffi::CStr;
use std::fs::File;
use tsfile_writer::writer::tsfile_writer::TsFileWriter;
use tsfile_writer::writer::{IoTDBValue, Schema, WriteWrapper};

/// # Safety
/// this function is intended for C usage, so unsafe is part of it....
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn schema_simple<'a>(
    device_id: *const c_char,
    measurement_id: *const c_char,
    data_type: u8,
    encoding: u8,
    compression: u8,
) -> *mut Schema<'a> {
    let device_id = unsafe {
        assert!(!device_id.is_null());

        CStr::from_ptr(device_id)
    };
    let measurement_id = unsafe {
        assert!(!measurement_id.is_null());

        CStr::from_ptr(measurement_id)
    };
    let device_id = device_id.to_str().unwrap();
    let measurement_id = measurement_id.to_str().unwrap();

    let data_type = data_type
        .try_into()
        .expect("Unable to deserialize data type");
    let encoding = encoding.try_into().expect("Unable to deserialize encoding");
    let compression = compression
        .try_into()
        .expect("Unable to deserialize compression");

    let schema = Schema::simple(device_id, measurement_id, data_type, encoding, compression);

    let b = Box::new(schema);

    Box::into_raw(b)
}

/// # Safety
/// this function is intended for C usage, so unsafe is part of it....
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn schema_free(schema: *mut Schema) {
    if !schema.is_null() {
        let _b = unsafe { Box::from_raw(schema) };
    }
}

/// # Safety
/// this function is intended for C usage, so unsafe is part of it....
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn file_writer_new(
    filename: *const c_char,
    schema: *mut Schema,
) -> *mut TsFileWriter<WriteWrapper<File>> {
    let filename = unsafe {
        assert!(!filename.is_null());

        CStr::from_ptr(filename)
    };
    let schema = unsafe { Box::from_raw(schema) };
    let b = Box::new(
        TsFileWriter::new(
            filename.to_str().expect(""),
            *schema.clone(),
            Default::default(),
        )
        .expect(""),
    );

    // Important, forget about the pointer to not clean up schema here
    Box::leak(schema);

    Box::into_raw(b)
}

/// # Safety
/// this function is intended for C usage, so unsafe is part of it....
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn file_writer_write_int32(
    writer: *mut TsFileWriter<WriteWrapper<File>>,
    device_id: *const c_char,
    measurement_id: *const c_char,
    timestamp: i64,
    number: i32,
) -> *mut TsFileWriter<WriteWrapper<File>> {
    if writer.is_null() {
        panic!("Null writer given!")
    }
    let device_id = unsafe {
        assert!(!device_id.is_null());

        CStr::from_ptr(device_id)
    };
    let measurement_id = unsafe {
        assert!(!measurement_id.is_null());

        CStr::from_ptr(measurement_id)
    };
    let mut writer = unsafe { Box::from_raw(writer) };

    writer
        .write(
            device_id.to_str().unwrap(),
            measurement_id.to_str().unwrap(),
            timestamp,
            IoTDBValue::INT(number),
        )
        .unwrap();

    // Return ref back
    Box::into_raw(writer)
}

/// # Safety
/// this function is intended for C usage, so unsafe is part of it....
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn file_writer_close(writer: *mut TsFileWriter<WriteWrapper<File>>) {
    if !writer.is_null() {
        let mut _b = unsafe { Box::from_raw(writer) };
        _b.close();
    }
}
