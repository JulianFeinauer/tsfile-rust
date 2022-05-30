use std::fmt::{Display, Formatter};
use std::io;
use std::io::Write;
use crate::{CompressionType, IoTDBValue, PageWriter, PositionedWrite, Serializable, Statistics, StatisticsStruct, TSDataType, TSEncoding, utils, write_str};
use crate::encoding::PlainIntEncoder;

// #[derive(Clone)]
pub struct ChunkMetadata {
    pub(crate) measurement_id: String,
    pub(crate) data_type: TSDataType,
    pub(crate) mask: u8,
    offset_of_chunk_header: i64,
    // statistics: Box<dyn Statistics>,
    pub(crate) statistics: Box<dyn Statistics>,
}

impl Clone for ChunkMetadata {
    fn clone(&self) -> Self {
        let cloned: Box<dyn Statistics> = match self.data_type {
            TSDataType::INT32 => {
                match self.statistics.as_any().downcast_ref::<StatisticsStruct<i32>>() {
                    None => {
                        panic!("Whaaa")
                    }
                    Some(statistic) => {
                        Box::new(statistic.clone())
                    }
                }
            }
            TSDataType::INT64 => {
                match self.statistics.as_any().downcast_ref::<StatisticsStruct<i64>>() {
                    None => {
                        panic!("Whaaa")
                    }
                    Some(statistic) => {
                        Box::new(statistic.clone())
                    }
                }
            }
            TSDataType::FLOAT => {
                match self.statistics.as_any().downcast_ref::<StatisticsStruct<f32>>() {
                    None => {
                        panic!("Whaaa")
                    }
                    Some(statistic) => {
                        Box::new(statistic.clone())
                    }
                }
            }
        };
        Self {
            measurement_id: self.measurement_id.clone(),
            data_type: self.data_type,
            mask: self.mask,
            offset_of_chunk_header: self.offset_of_chunk_header,
            statistics: cloned
        }
    }
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

pub trait Chunkeable {
    fn write(&mut self, timestamp: i64, value: IoTDBValue) -> Result<(), &str>;

    fn serialize(&mut self, file: &mut dyn PositionedWrite);

    fn get_metadata(&self) -> ChunkMetadata;
}

pub struct ChunkWriter<T> {
    data_type: TSDataType,
    compression: CompressionType,
    encoding: TSEncoding,
    measurement_id: String,
    current_page_writer: Option<PageWriter<T>>,
    offset_of_chunk_header: Option<u64>,
    // Statistics
    statistics: StatisticsStruct<T>,
}

impl Chunkeable for ChunkWriter<i32> {
    fn write(&mut self, timestamp: i64, value: IoTDBValue) -> Result<(), &str> {
        let value = match value {
            IoTDBValue::INT(val) => val,
            _ => {
                return Err("wrong type!");
            }
        };
        // Update statistics
        self.statistics.update(timestamp, value);

        match &mut self.current_page_writer {
            None => {
                // Create a page
                self.current_page_writer = Some(PageWriter::new(PlainIntEncoder::<i32>::new()))
            }
            Some(_) => {
                // do nothing
            }
        }
        let page_writer = self.current_page_writer.as_mut().unwrap();
        page_writer.write(timestamp, value)
    }

    fn serialize(&mut self, file: &mut dyn PositionedWrite) {
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
        file.write(&[self.compression.serialize()])
            .expect("write failed");
        // Encoding PLAIN -> 0
        file.write(&[self.encoding.serialize()])
            .expect("write failed");
        // End Chunk Header

        // Write the full page
        file.write_all(&page_buffer);
    }

    fn get_metadata(&self) -> ChunkMetadata {
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
            statistics: Box::new(self.statistics.clone()),
        }
    }
}

impl Chunkeable for ChunkWriter<i64> {
    fn write(&mut self, timestamp: i64, value: IoTDBValue) -> Result<(), &str> {
        let value = match value {
            IoTDBValue::LONG(val) => val,
            _ => {
                return Err("wrong type!");
            }
        };
        // Update statistics
        self.statistics.update(timestamp, value);

        match &mut self.current_page_writer {
            None => {
                // Create a page
                self.current_page_writer = Some(PageWriter::new(PlainIntEncoder::<i64>::new()))
            }
            Some(_) => {
                // do nothing
            }
        }
        let page_writer = self.current_page_writer.as_mut().unwrap();
        page_writer.write(timestamp, value)
    }

    fn serialize(&mut self, file: &mut dyn PositionedWrite) {
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
        file.write(&[self.compression.serialize()])
            .expect("write failed");
        // Encoding PLAIN -> 0
        file.write(&[self.encoding.serialize()])
            .expect("write failed");
        // End Chunk Header

        // Write the full page
        file.write_all(&page_buffer);
    }

    fn get_metadata(&self) -> ChunkMetadata {
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
            statistics: Box::new(self.statistics.clone()),
        }
    }
}

impl Chunkeable for ChunkWriter<f32> {
    fn write(&mut self, timestamp: i64, value: IoTDBValue) -> Result<(), &str> {
        let value = match value {
            IoTDBValue::FLOAT(val) => val,
            _ => {
                return Err("wrong type!");
            }
        };
        // Update statistics
        self.statistics.update(timestamp, value);

        match &mut self.current_page_writer {
            None => {
                // Create a page
                self.current_page_writer = Some(PageWriter::new(PlainIntEncoder::<f32>::new()))
            }
            Some(_) => {
                // do nothing
            }
        }
        let page_writer = self.current_page_writer.as_mut().unwrap();
        page_writer.write(timestamp, value)
    }

    fn serialize(&mut self, file: &mut dyn PositionedWrite) {
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
        file.write(&[self.compression.serialize()])
            .expect("write failed");
        // Encoding PLAIN -> 0
        file.write(&[self.encoding.serialize()])
            .expect("write failed");
        // End Chunk Header

        // Write the full page
        file.write_all(&page_buffer);
    }

    fn get_metadata(&self) -> ChunkMetadata {
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
            statistics: Box::new(self.statistics.clone()),
        }
    }
}

impl<'a, 'b, T> ChunkWriter<T> {
    pub(crate) fn new(
        measurement_id: String,
        data_type: TSDataType,
        compression: CompressionType,
        encoding: TSEncoding,
    ) -> Box<dyn Chunkeable> {
        match data_type {
            TSDataType::INT32 => {
                let writer: ChunkWriter<i32> = ChunkWriter {
                    data_type,
                    compression,
                    encoding,
                    measurement_id: measurement_id.to_owned(),
                    current_page_writer: None,
                    offset_of_chunk_header: None,
                    statistics: StatisticsStruct::<i32>::new(),
                };
                Box::new(writer)
            }
            TSDataType::INT64 => {
                let writer: ChunkWriter<i64> = ChunkWriter {
                    data_type,
                    compression,
                    encoding,
                    measurement_id: measurement_id.to_owned(),
                    current_page_writer: None,
                    offset_of_chunk_header: None,
                    statistics: StatisticsStruct::<i64>::new(),
                };
                Box::new(writer)
            }
            TSDataType::FLOAT => {
                let writer: ChunkWriter<f32> = ChunkWriter {
                    data_type,
                    compression,
                    encoding,
                    measurement_id: measurement_id.to_owned(),
                    current_page_writer: None,
                    offset_of_chunk_header: None,
                    statistics: StatisticsStruct::<f32>::new(),
                };
                Box::new(writer)
            }
        }
    }
}