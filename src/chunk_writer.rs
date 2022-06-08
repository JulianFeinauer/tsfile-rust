use std::fmt::{Display, Formatter};
use std::io;
use std::io::Write;
use crate::{CompressionType, IoTDBValue, PositionedWrite, Serializable, TSDataType, TSEncoding, utils, write_str};
use crate::chunk_writer::TypedEncoder::{Float, Int, Long};
use crate::encoding::{PlainIntEncoder, TimeEncoder};
use crate::statistics::Statistics;
use crate::encoding::Encoder;
use crate::TSDataType::FLOAT;

pub enum TypedEncoder {
    Int(IntEncoder),
    Long(LongEncoder),
    Float(FloatEncoder)
}

impl TypedEncoder {
    pub(crate) fn serialize(&mut self, buffer: &mut Vec<u8>) {
        match self {
            TypedEncoder::Int(encoder) => encoder.serialize(buffer),
            TypedEncoder::Long(encoder) => encoder.serialize(buffer),
            TypedEncoder::Float(encoder) => encoder.serialize(buffer)
        }
    }

    pub(crate) fn write(&mut self, value: &IoTDBValue) -> Result<(), &str> {
        match (self, value) {
            (Int(encoder), IoTDBValue::INT(i)) => encoder.write(*i),
            (Long(encoder), IoTDBValue::LONG(i)) => encoder.write(*i),
            (Float(encoder), IoTDBValue::FLOAT(i)) => encoder.write(*i),
            (_, _) => panic!("Something went terribly wrong here!")
        }
    }

    fn new(data_type: TSDataType, encoding: TSEncoding) -> TypedEncoder {
        match data_type {
            TSDataType::INT32 => TypedEncoder::Int(IntEncoder::new(encoding)),
            TSDataType::INT64 => TypedEncoder::Long(LongEncoder::new(encoding)),
            TSDataType::FLOAT => TypedEncoder::Float(FloatEncoder::new(encoding)),
            _ => panic!("No Encoder for data type {:?}", data_type)
        }
    }
}

pub enum IntEncoder {
    Plain(PlainIntEncoder<i32>)
}

impl IntEncoder {
    pub(crate) fn write(&mut self, value: i32) -> Result<(), &str> {
        match self {
            IntEncoder::Plain(encoder) => {
                encoder.encode(value);
                Ok(())
            }
        }
    }

    pub(crate) fn serialize(&mut self, buffer: &mut Vec<u8>) {
        match self {
            IntEncoder::Plain(encoder) => {
                encoder.serialize(buffer);
            }
        }
    }

    fn new(encoding: TSEncoding) -> IntEncoder {
        match encoding {
            TSEncoding::PLAIN => {
                IntEncoder::Plain(PlainIntEncoder::<i32>::new())
            }
        }
    }
}

pub enum LongEncoder {
    Plain(PlainIntEncoder<i64>)
}

impl LongEncoder {
    pub(crate) fn write(&mut self, value: i64) -> Result<(), &str> {
        match self {
            LongEncoder::Plain(encoder) => {
                encoder.encode(value);
                Ok(())
            }
        }
    }

    pub(crate) fn serialize(&mut self, buffer: &mut Vec<u8>) {
        match self {
            LongEncoder::Plain(encoder) => {
                encoder.serialize(buffer);
            }
        }
    }

    fn new(encoding: TSEncoding) -> LongEncoder {
        match encoding {
            TSEncoding::PLAIN => {
                LongEncoder::Plain(PlainIntEncoder::<i64>::new())
            }
        }
    }
}

pub enum FloatEncoder {
    Plain(PlainIntEncoder<f32>)
}

impl FloatEncoder {
    fn new(encoding: TSEncoding) -> FloatEncoder {
        match encoding {
            TSEncoding::PLAIN => FloatEncoder::Plain(PlainIntEncoder::new())
        }
    }

    pub(crate) fn write(&mut self, value: f32) -> Result<(), &str> {
        match self {
            FloatEncoder::Plain(encoder) => {
                encoder.encode(value);
                Ok(())
            }
        }
    }

    pub(crate) fn serialize(&mut self, buffer: &mut Vec<u8>) {
        match self {
            FloatEncoder::Plain(encoder) => {
                encoder.serialize(buffer);
            }
        }
    }
}


struct PageWriter {
    time_encoder: TimeEncoder,
    value_encoder: TypedEncoder,
    // Necessary for writing
    buffer: Vec<u8>,
}

impl PageWriter {
    fn new(data_type: TSDataType, encoding: TSEncoding) -> PageWriter {
        PageWriter {
            time_encoder: TimeEncoder::new(),
            value_encoder: TypedEncoder::new(data_type, encoding),
            buffer: vec![]
        }
    }

    fn write(&mut self, timestamp: i64, value: &IoTDBValue) -> Result<(), &str> {
        self.time_encoder.encode(timestamp);
        self.value_encoder.write(value);
        Ok(())
    }

    pub(crate) fn prepare_buffer(&mut self) {
        // serialize time_encoder and value encoder
        let mut time_buffer = vec![];
        self.time_encoder.serialize(&mut time_buffer);
        crate::write_var_u32(time_buffer.len() as u32, &mut self.buffer);
        self.buffer.write_all(time_buffer.as_slice());
        self.value_encoder.serialize(&mut self.buffer);
    }
}

pub struct ChunkWriter {
    pub(crate) measurement_id: String,
    pub(crate) data_type: TSDataType,
    pub compression_type: CompressionType,
    pub encoding: TSEncoding,
    pub(crate) mask: u8,
    offset_of_chunk_header: Option<u64>,
    pub(crate) statistics: Statistics,
    current_page_writer: Option<PageWriter>,
}

impl ChunkWriter {
    pub fn write(&mut self, timestamp: i64, value: IoTDBValue) -> Result<(), &str> {
        self.statistics.update(timestamp, &value);

        match &mut self.current_page_writer {
            None => {
                // Create a page
                self.current_page_writer = Some(PageWriter::new(self.data_type, self.encoding))
            }
            Some(_) => {
                // do nothing
            }
        }
        let page_writer = self.current_page_writer.as_mut().unwrap();
        page_writer.write(timestamp, &value)
    }
}

impl ChunkWriter {
    pub fn new(measurement_id: String, data_type: TSDataType, compression_type: CompressionType, encoding: TSEncoding) -> ChunkWriter {
        ChunkWriter {
            measurement_id,
            data_type,
            compression_type,
            encoding,
            mask: 0,
            offset_of_chunk_header: None,
            statistics: Statistics::new(data_type),
            current_page_writer: None
        }
    }

    pub(crate) fn serialize(&mut self, file: &mut dyn PositionedWrite) {
        // Before we can write the header we have to serialize the current page
        let buffer_size: u8 = match self.current_page_writer.as_mut() {
            Some(page_writer) => {
                page_writer.prepare_buffer();
                page_writer.buffer.len()
            }
            None => 0,
        } as u8;

        let uncompressed_bytes = buffer_size;
        // We have no compression!
        let compressed_bytes = uncompressed_bytes;

        let mut page_buffer: Vec<u8> = vec![];

        // Uncompressed size
        utils::write_var_u32(uncompressed_bytes as u32, &mut page_buffer);
        // Compressed size
        utils::write_var_u32(compressed_bytes as u32, &mut page_buffer);

        // page data
        match self.current_page_writer.as_mut() {
            Some(page_writer) => {
                page_buffer.write_all(&page_writer.buffer);
            }
            None => {
                panic!("Unable to flush without page writer!")
            }
        }

        // Chunk Header

        // store offset for metadata
        self.offset_of_chunk_header = Some(file.get_position());

        file.write(&[5]).expect("write failed"); // Marker

        write_str(file, self.measurement_id.as_str());
        // Data Lenght
        utils::write_var_u32(page_buffer.len() as u32, file);
        // Data Type INT32 -> 1
        file.write(&[self.data_type.serialize()])
            .expect("write failed");
        // Compression Type UNCOMPRESSED -> 0
        file.write(&[self.compression_type.serialize()])
            .expect("write failed");
        // Encoding PLAIN -> 0
        file.write(&[self.encoding.serialize()])
            .expect("write failed");
        // End Chunk Header

        // Write the full page
        file.write_all(&page_buffer);
    }

    pub(crate) fn get_metadata(&self) -> ChunkMetadata {
        ChunkMetadata {
            measurement_id: self.measurement_id.clone(),
            data_type: self.data_type,
            // FIXME add this
            mask: 0,
            offset_of_chunk_header: match self.offset_of_chunk_header {
                None => {
                    panic!("get_metadata called before offset is defined");
                }
                Some(offset) => offset,
            } as i64,
            statistics: self.statistics.clone(),
        }
    }
}


#[derive(Clone)]
pub struct ChunkMetadata {
    pub(crate) measurement_id: String,
    pub(crate) data_type: TSDataType,
    pub(crate) mask: u8,
    offset_of_chunk_header: i64,
    pub(crate) statistics: Statistics,
}

impl Display for ChunkMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (...)", self.measurement_id)
    }
}

impl ChunkMetadata {
    pub(crate) fn serialize(
        &self,
        file: &mut dyn PositionedWrite,
        serialize_statistics: bool,
    ) -> io::Result<()> {
        let result = file.write_all(&self.offset_of_chunk_header.to_be_bytes());
        if serialize_statistics {
            self.statistics.serialize(file);
        }
        return result;
    }
}
