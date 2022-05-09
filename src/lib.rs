use std::{io, vec};
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Error;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::iter::Map;

fn write_var_u32(num: u32, buffer: &mut dyn Write) -> u8 {
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

fn read_byte(buffer: &mut dyn Read) -> u8 {
    let mut read_buffer: [u8; 1] = [0];
    buffer.read(&mut read_buffer).expect("Prblem");
    return read_buffer[0];
}

fn read_var_u32(buffer: &mut dyn Read) -> u32 {
    let mut value: u32 = 0;
    let mut i: u8 = 0;
    let mut b = read_byte(buffer);
    while (b != u8::MAX && (b & 0x80) != 0) {
        value = value | (((b & 0x7F) as u32) << i);
        i = i + 7;
        b = read_byte(buffer);
    }
    return value | ((b as u32) << i);
}

#[derive(Copy, Clone)]
enum TSDataType {
    INT32
}


impl TSDataType {
    fn serialize(&self) -> u8 {
        match self {
            TSDataType::INT32 => 1
        }
    }
}

#[derive(Copy, Clone)]
enum TSEncoding {
    PLAIN
}

impl TSEncoding {
    pub(crate) fn serialize(&self) -> u8 {
        match self {
            TSEncoding::PLAIN => 0
        }
    }
}

#[derive(PartialEq, Copy, Clone)]
enum CompressionType {
    UNCOMPRESSED
}

impl CompressionType {
    pub(crate) fn serialize(&self) -> u8 {
        match self {
            CompressionType::UNCOMPRESSED => 0
        }
    }
}

struct MeasurementSchema<'a> {
    measurement_id: &'a str,
    data_type: TSDataType,
    encoding: TSEncoding,
    compression: CompressionType,
}

#[derive(PartialEq, Eq, Hash)]
struct Path<'a> {
    path: &'a str,
}

impl MeasurementSchema<'_> {
    fn new(measurement_id: &str,
           data_type: TSDataType,
           encoding: TSEncoding,
           compression: CompressionType) -> MeasurementSchema {
        MeasurementSchema {
            measurement_id,
            data_type,
            encoding,
            compression,
        }
    }
}

struct MeasurementGroup<'a> {
    measurement_schemas: HashMap<&'a str, MeasurementSchema<'a>>,
}

struct Schema<'a> {
    measurement_groups: HashMap<&'a Path<'a>, MeasurementGroup<'a>>,
}

struct PageWriter {
    time_encoder: TimeEncoder,
    value_encoder: PlainInt32Encoder,
    // Necessary for writing
    buffer: Vec<u8>,
}

impl PageWriter {
    pub(crate) fn serialize(&self, file: &mut File, compression: CompressionType) {
        if compression != CompressionType::UNCOMPRESSED {
            panic!("Only uncompressed is supported now!")
        }
        // Write header
        // Write uncompressed size
        let len_as_bytes = (self.buffer.len() as i32).to_be_bytes();
        file.write_all(&len_as_bytes);
        // Write compressed size (same for now)
        file.write_all(&len_as_bytes);
        // End of Header
        // Write statistic ???
        // Write data
        file.write_all(self.buffer.as_slice());
    }
}

impl PageWriter {
    fn new() -> PageWriter {
        PageWriter {
            time_encoder: TimeEncoder::new(),
            value_encoder: PlainInt32Encoder::new(),
            buffer: vec![],
        }
    }

    pub(crate) fn write(&mut self, timestamp: i64, value: i32) -> Result<(), &str> {
        self.time_encoder.encode(timestamp);
        self.value_encoder.encode(value);
        Ok(())
    }

    pub(crate) fn prepare_buffer(&mut self) {
        // serialize time_encoder and value encoder
        self.time_encoder.serialize(&mut self.buffer);
        self.value_encoder.serialize(&mut self.buffer);
    }
}

struct ChunkWriter<'a> {
    data_type: TSDataType,
    compression: CompressionType,
    encoding: TSEncoding,
    measurement_id: &'a str,
    current_page_writer: Option<PageWriter>,
}

impl ChunkWriter<'_> {
    pub(crate) fn new(measurement_id: &str, data_type: TSDataType, compression: CompressionType, encoding: TSEncoding) -> ChunkWriter {
        ChunkWriter {
            data_type,
            compression,
            encoding,
            measurement_id,
            current_page_writer: None,
        }
    }

    pub(crate) fn write(&mut self, timestamp: i64, value: i32) -> Result<(), &str> {
        match &mut self.current_page_writer {
            None => {
                // Create a page
                self.current_page_writer = Some(PageWriter::new())
            }
            Some(_) => {
                // do nothing
            }
        }
        let page_writer = self.current_page_writer.as_mut().unwrap();
        page_writer.write(timestamp, value)
    }

    pub(crate) fn serialize(&mut self, file: &mut File) {
        // Before we can write the header we have to serialize the current page
        let buffer_size: u8 = match self.current_page_writer.as_mut() {
            Some(page_writer) => {
                page_writer.prepare_buffer();
                page_writer.buffer.len()
            }
            None => {
                0
            }
        } as u8;

        // Chunk Header
        file.write(&[5]).expect("write failed");   // Marker
        write_str(file, self.measurement_id);
        // Data Lenght
        file.write(&[buffer_size]).expect("write failed");
        // Data Type INT32 -> 1
        file.write(&[self.data_type.serialize()]).expect("write failed");
        // Compression Type UNCOMPRESSED -> 0
        file.write(&[self.compression.serialize()]).expect("write failed");
        // Encoding PLAIN -> 0
        file.write(&[self.encoding.serialize()]).expect("write failed");
        // End Chunk Header

        // Iterate all pages (only one now)
        match self.current_page_writer.as_mut() {
            Some(page_writer) => {
                page_writer.serialize(file, self.compression);
            }
            _ => {
                // Dont do nothing here?
            }
        }
    }
}

struct GroupWriter<'a> {
    path: &'a Path<'a>,
    measurement_group: MeasurementGroup<'a>,
    chunk_writers: HashMap<&'a str, ChunkWriter<'a>>,
}

impl<'a> GroupWriter<'a> {
    pub(crate) fn write(&mut self, measurement_id: &'a str, timestamp: i64, value: i32) -> Result<(), &str> {
        match &mut self.chunk_writers.get_mut(measurement_id) {
            Some(chunk_writer) => {
                chunk_writer.write(timestamp, value);
                Ok(())
            }
            None => {
                Err("Unknown measurement id")
            }
        }
    }

    fn serialize(&mut self, file: &mut File) -> Result<(), &str> {
        // Marker
        file.write(&[0]);
        // Chunk Group Header
        write_str(file, self.path.path);
        // End Group Header
        for (&measurement_id, chunk_writer) in self.chunk_writers.iter_mut() {
            chunk_writer.serialize(file);
        }
        // TODO Footer?
        Ok(())
    }
}

struct TsFileWriter<'a> {
    filename: &'a str,
    group_writers: HashMap<&'a Path<'a>, GroupWriter<'a>>,
}

impl<'a> TsFileWriter<'a> {
    pub(crate) fn write<'b>(&'b mut self, device: &'a Path, measurement_id: &'a str, timestamp: i64, value: i32) -> Result<(), &'b str> {
        match self.group_writers.get_mut(device) {
            Some(group) => {
                return group.write(measurement_id, timestamp, value);
            }
            None => {
                return Err("Unable to find group writer");
            }
        }
    }

    pub(crate) fn flush(&mut self) -> Result<(), &str> {
        // Start to write to file
        // Header
        let mut file = File::create(self.filename).expect("create failed");
        let version: [u8; 1] = [3];

        // Header
        file.write("TsFile".as_bytes()).expect("write failed");
        file.write(&version).expect("write failed");
        // End of Header

        // Now iterate the
        for (&path, group_writer) in self.group_writers.iter_mut() {
            // Write the group
            group_writer.serialize(&mut file);
        }

        // TODO Write the Footer
        Ok(())
    }
}

impl TsFileWriter<'_> {
    fn new<'a>(filename: &'a str, schema: Schema<'a>) -> TsFileWriter<'a> {
        let group_writers = schema.measurement_groups.into_iter().map(|(path, v)| {
            (path, GroupWriter {
                path,
                chunk_writers: v.measurement_schemas.iter().map(|(&measurement_id, measurement_schema)| {
                    (measurement_id, ChunkWriter::new(measurement_id, measurement_schema.data_type, measurement_schema.compression, measurement_schema.encoding))
                }).collect(),
                measurement_group: v,
            })
        }).collect();

        TsFileWriter {
            filename,
            group_writers,
        }
    }
}

trait Encoder<DataType> {
    fn encode(&mut self, value: DataType);
}

struct PlainInt32Encoder {
    values: Vec<i32>,
}

impl PlainInt32Encoder {
    pub(crate) fn serialize(&self, buffer: &mut Vec<u8>) {
        for val in &self.values {
            buffer.write(&val.to_be_bytes());
        }
    }
}

impl PlainInt32Encoder {
    fn new() -> PlainInt32Encoder {
        PlainInt32Encoder {
            values: vec![]
        }
    }
}

impl Encoder<i32> for PlainInt32Encoder {
    fn encode(&mut self, value: i32) {
        self.values.push(value)
    }
}

struct TimeEncoder {
    first_value: Option<i64>,
    min_delta: i64,
    previous_value: i64,
    values: Vec<i64>,
}

impl TimeEncoder {

    fn get_value_width(&mut self, v: i64) -> u32 {
      return 64 - v.leading_zeros()
    }

    fn calculate_bit_widths_for_delta_block_buffer(&mut self, delta_block_buffer: &Vec<i64>) -> u32 {
        let mut width = 0;

        for i in 0..delta_block_buffer.len() {
            let v = *delta_block_buffer.get(i).expect("");
            let value_width = self.get_value_width(v);
            width = max(width, value_width)
        }

        return width;
    }

    fn long_to_bytes(number: i64, width: u32) -> Vec<u8> {

    }

    fn write_data_with_min_width(&mut self, buffer: &mut Vec<u8>) {

    }

    pub(crate) fn serialize(&mut self, buffer: &mut Vec<u8>) {
        // Preliminary calculations
        let mut delta_block_buffer: Vec<i64> = vec![];

        for delta in &self.values {
            delta_block_buffer.push(delta - self.min_delta);
        }

        let write_width = self.calculate_bit_widths_for_delta_block_buffer(&delta_block_buffer);

        // Write Header
        // Write number of entries
        buffer.write_all(&self.values.len().to_be_bytes());
        // Write "write-width"
        buffer.write_all(&write_width.to_be_bytes());

        // Min Delta Base
        buffer.write_all(&self.min_delta.to_be_bytes());
        // First Value
        buffer.write_all(&self.first_value.expect("").to_be_bytes());
        // End Header

        // FIXME continue here...

        // TODO needs to be done right
        // for val in &self.values {
        //     buffer.write(&val.to_be_bytes());
        // }
    }
}

impl TimeEncoder {
    fn new() -> TimeEncoder {
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
}

struct Int32Page {
    times: Vec<i64>,
    values: Vec<i32>,
}

impl Int32Page {
    fn flush_to_buffer(&self) {}
}

struct ChunkGroupHeader<'a> {
    device_id: &'a str,
}

impl Serializable for ChunkGroupHeader<'_> {
    fn serialize(&self, file: &mut File) -> io::Result<()> {
        file.write_all(&[0])?;
        write_str(file, &self.device_id);
        Ok(())
    }
}

struct ChunkGroup<'a> {
    header: ChunkGroupHeader<'a>,
    pages: Vec<Int32Page>,
}

impl Serializable for ChunkGroup<'_> {
    fn serialize(&self, file: &mut File) -> io::Result<()> {
        self.header.serialize(file)
    }
}

struct ChunkHeader<'a> {
    measurement_id: &'a str,
    data_size: u8,
    data_type: u8,
    compression: u8,
    encoding: u8,
}

impl ChunkHeader<'_> {
    fn new<'a>(measurement_id: &str) -> ChunkHeader {
        return ChunkHeader {
            measurement_id,
            data_size: 28,
            data_type: 1,
            compression: 0,
            encoding: 0,
        };
    }
}

trait Serializable {
    fn serialize(&self, file: &mut File) -> io::Result<()>;
}

struct Chunk<'a> {
    header: ChunkHeader<'a>,
    num_pages: u8,
}

impl Serializable for Chunk<'_> {
    fn serialize(&self, file: &mut File) -> io::Result<()> {
        self.header.serialize(file)
    }
}

fn write_str(file: &mut File, s: &str) -> io::Result<()> {
    let len = s.len() as u8 + 2;
    file.write(&[len]).expect("write failed");   // lenght (?)
    let bytes = s.as_bytes();
    file.write(bytes);   // measurement id
    Ok(())
}

impl Serializable for ChunkHeader<'_> {
    fn serialize(&self, file: &mut File) -> io::Result<()> {
        // Chunk Header
        file.write(&[5]).expect("write failed");   // Marker
        write_str(file, &self.measurement_id);
        // Data Lenght
        file.write(&[self.data_size]).expect("write failed");
        // Data Type INT32 -> 1
        file.write(&[1]).expect("write failed");
        // Compression Type UNCOMPRESSED -> 0
        file.write(&[0]).expect("write failed");
        // Encoding PLAIN -> 0
        file.write(&[0]).expect("write failed");
        Ok(())
    }
}

#[warn(dead_code)]
fn write_file_3() {
    let measurement_schema = MeasurementSchema {
        measurement_id: "s1",
        data_type: TSDataType::INT32,
        encoding: TSEncoding::PLAIN,
        compression: CompressionType::UNCOMPRESSED,
    };

    let mut measurement_schema_map = HashMap::new();
    measurement_schema_map.insert("s1", measurement_schema);
    let measurement_group = MeasurementGroup {
        measurement_schemas: measurement_schema_map
    };
    let mut measurement_groups_map = HashMap::new();
    let d1 = Path { path: "d1" };
    measurement_groups_map.insert(&d1, measurement_group);
    let schema = Schema {
        measurement_groups: measurement_groups_map
    };
    let mut writer = TsFileWriter::new("data3.tsfile", schema);

    TsFileWriter::write(&mut writer, &d1, "s1", 0, 13);

    TsFileWriter::flush(&mut writer);

    ()
}

#[warn(dead_code)]
fn write_file_2() {
    std::fs::remove_file("data2.tsfile");

    let zero: [u8; 1] = [0];
    let mut file = File::create("data2.tsfile").expect("create failed");
    let version: [u8; 1] = [3];

    // Header
    file.write("TsFile".as_bytes()).expect("write failed");
    file.write(&version).expect("write failed");
    // End of Header

    let cg = ChunkGroup {
        header: ChunkGroupHeader {
            device_id: "d1"
        },
        pages: vec![
            Int32Page {
                times: vec![0],
                values: vec![13],
            }
        ],
    };

    &cg.serialize(&mut file);

    // Create ChunkHeader
    let header = ChunkHeader::new("s1");
    header.serialize(&mut file).expect("")
}

#[warn(dead_code)]
fn write_file() {
    std::fs::remove_file("data.tsfile");

    let zero: [u8; 1] = [0];
    let mut file = File::create("data.tsfile").expect("create failed");
    let version: [u8; 1] = [3];

    // Header
    file.write("TsFile".as_bytes()).expect("write failed");
    file.write(&version).expect("write failed");
    // End of Header
    file.write(&zero).expect("write failed");
    // First Channel Group
    // Chunk Group Header
    file.write(&[4]).expect("write failed");   // lenght (?)
    file.write("d1".as_bytes()).expect("write failed");   // device id
    // First Chunk
    // Chunk Header
    file.write(&[5]).expect("write failed");   // Marker
    file.write(&[4]).expect("write failed");   // lenght (?)
    file.write("s1".as_bytes()).expect("write failed");   // measurement id
    // Data Lenght
    file.write(&[28]).expect("write failed");
    // Data Type INT32 -> 1
    file.write(&[1]).expect("write failed");
    // Compression Type UNCOMPRESSED -> 0
    file.write(&[0]).expect("write failed");
    // Encoding PLAIN -> 0
    file.write(&[0]).expect("write failed");

    println!("data written to file");
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use crate::{read_var_u32, write_file, write_file_2, write_file_3, write_var_u32};

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }

    #[test]
    fn write_file_test() {
        write_file()
    }

    #[test]
    fn write_file_test_2() {
        write_file_2()
    }

    #[test]
    fn write_file_test_3() {
        write_file_3()
    }

    #[test]
    fn read_var_int() {
        for number in [1, 12, 123, 1234, 12345, 123456, 1234567, 12345678, 123456789] {
            let mut result: Vec<u8> = vec![];

            // Write it
            write_var_u32(number, &mut result);
            // Read it back
            let result: u32 = read_var_u32(&mut result.as_slice());

            assert_eq!(number, result);
        }
    }

    #[test]
    fn write_var_int() {
        let number: u32 = 123456789;
        let mut result: Vec<u8> = vec![];
        let position = write_var_u32(number, &mut result);

        assert_eq!(position, 4);
        assert_eq!(result.as_slice(), [0b10010101, 0b10011010, 0b11101111, 0b00111010]);
    }

    #[test]
    fn write_var_int_2() {
        let number: u32 = 128;
        let mut result: Vec<u8> = vec![];
        let position = write_var_u32(number, &mut result);

        assert_eq!(position, 2);
        assert_eq!(result.as_slice(), [128, 1]);
    }

    #[test]
    fn write_var_int_3() {
        let number: u32 = 13;
        let mut result: Vec<u8> = vec![];
        let position = write_var_u32(number, &mut result);

        assert_eq!(position, 1);
        assert_eq!(result.as_slice(), [13]);
    }

    #[test]
    fn pre_write_var_int() {
        let mut number: u32 = 123456789;
        let bytes: [u8; 4] = number.to_be_bytes();
        assert_eq!(bytes, [0b00000111, 0b01011011, 0b11001101, 0b00010101]);

        let mut buffer: Vec<u8> = vec![];

        // Now compress them
        let mut position: u8 = 1;

        while (number & 0xFFFFFF80) != 0 {
            buffer.push(((number & 0x7F) | 0x80) as u8);
            number = number >> 7;
            position = position + 1;
        }

        buffer.push((number & 0x7F) as u8);

        assert_eq!(buffer, [0b10010101, 0b10011010, 0b11101111, 0b00111010])
    }
}
