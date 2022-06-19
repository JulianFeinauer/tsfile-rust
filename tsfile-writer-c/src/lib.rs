extern crate core;
extern crate libc;

use std::ffi::{CStr, CString};
use std::fs::File;
use libc::c_char;
use tsfile_writer::{IoTDBValue, PositionedWrite, Schema, TSDataType, WriteWrapper};
use tsfile_writer::tsfile_writer::TsFileWriter;

#[no_mangle]
pub extern "C" fn schema_simple<'a>(device_id: *const c_char, measurement_id: *const c_char, data_type: u8, encoding: u8, compression: u8) -> *mut Schema<'a> {
    let device_id = unsafe {
        assert!(!device_id.is_null());

        CStr::from_ptr(device_id)
    };
    let measurement_id = unsafe {
        assert!(!measurement_id.is_null());

        CStr::from_ptr(measurement_id)
    };
    let device_id = device_id.to_str().expect("Unable to parse Device ID");
    let measurement_id = measurement_id.to_str().expect("Unable to parse Measurement ID");
    let data_type = data_type.try_into().expect("Unable to deserialize data type");
    let encoding = encoding.try_into().expect("Unable to deserialize encoding");
    let compression = compression.try_into().expect("Unable to deserialize compression");

    println!("Constructing Schema {} - {} - {:?} - {:?} - {:?}", device_id, measurement_id, data_type, encoding, compression);

    let schema = Schema::simple(device_id, measurement_id, data_type, encoding, compression);

    let b = Box::new(schema);


    Box::into_raw(b)
}


#[no_mangle]
pub extern "C" fn schema_free(schema: *mut Schema) {
    if !schema.is_null() {
        let b = unsafe {
            Box::from_raw(schema)
        };
        println!("Freeing schema {}", &b);
    }
}

#[no_mangle]
pub extern "C" fn file_writer_new<'a>(filename: *const c_char, schema: *mut Schema<'a>) -> *mut TsFileWriter<'a, WriteWrapper<File>> {
    println!("Hello world!");
    let filename = unsafe {
        assert!(!filename.is_null());

        CStr::from_ptr(filename)
    };
    let schema = unsafe {
        *Box::from_raw(schema)
    };
    println!("Generate File Writer {} with Schema {}", filename.to_str().expect(""), schema);
    let b = Box::new(TsFileWriter::new(filename.to_str().expect(""), schema, Default::default()).expect(""));

    Box::into_raw(b)
}

#[no_mangle]
pub extern "C" fn file_writer_write_int32<'a>(writer: *mut TsFileWriter<'a, WriteWrapper<File>>, device_id: *const c_char, measurement_id: *const c_char, timestamp: i64, number: i32) -> *mut TsFileWriter<'a, WriteWrapper<File>> {
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
    let mut writer = unsafe {
        Box::from_raw(writer)
    };

    writer.write(device_id.to_str().unwrap(), measurement_id.to_str().unwrap(), timestamp, IoTDBValue::INT(number)).unwrap();

    // Return ref back
    Box::into_raw(writer)
}

#[no_mangle]
pub extern "C" fn file_writer_close<'a>(writer: *mut TsFileWriter<'a, WriteWrapper<File>>) {
    println!("Freeing writer...");
    if !writer.is_null() {
        let mut _b = unsafe {
            Box::from_raw(writer)
        };
        println!("Closing writer");
        _b.close();
    }
}
