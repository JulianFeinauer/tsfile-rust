#![allow(dead_code)]
#![allow(unused_must_use)]
use std::{io, vec};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::hash::Hash;
use std::io::{Write};

use compression::CompressionType;
use encoding::{PlainInt32Encoder, TimeEncoder, TSEncoding};
use statistics::{Statistics, StatisticsStruct};

use crate::utils::write_var_u32;

mod test;
mod utils;
mod encoding;
mod compression;
mod statistics;

const GET_MAX_DEGREE_OF_INDEX_NODE: usize = 256;

pub trait PositionedWrite: Write {

    fn get_position(&self) -> u64;

}

struct WriteWrapper<T: Write> {
    position: u64,
    writer: T
}

impl<T: Write> Write for WriteWrapper<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.writer.write(buf) {
            Ok(size) => {
                self.position += size as u64;
                Ok(size)
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<T: Write> PositionedWrite for WriteWrapper<T> {

    fn get_position(&self) -> u64 {
        self.position
    }
}

impl<T: Write> WriteWrapper<T> {

    fn new(writer: T) -> WriteWrapper<T> {
        WriteWrapper {
            position: 0,
            writer
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
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

struct MeasurementSchema<'a> {
    measurement_id: &'a str,
    data_type: TSDataType,
    encoding: TSEncoding,
    compression: CompressionType,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Path {
    path: String,
}

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path)
    }
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
    measurement_groups: HashMap<&'a Path, MeasurementGroup<'a>>,
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
        let mut time_buffer = vec![];
        self.time_encoder.serialize(&mut time_buffer);
        utils::write_var_u32(time_buffer.len() as u32, &mut self.buffer);
        self.buffer.write_all(time_buffer.as_slice());
        self.value_encoder.serialize(&mut self.buffer);
    }
}

struct ChunkWriter<'a> {
    data_type: TSDataType,
    compression: CompressionType,
    encoding: TSEncoding,
    measurement_id: &'a str,
    current_page_writer: Option<PageWriter>,
    offset_of_chunk_header: Option<u64>,
    // Statistics
    statistics: StatisticsStruct<i32>,
}

impl<'a> ChunkWriter<'a> {
    pub(crate) fn new(measurement_id: &'a str, data_type: TSDataType, compression: CompressionType, encoding: TSEncoding) -> ChunkWriter {
        ChunkWriter {
            data_type,
            compression,
            encoding,
            measurement_id,
            current_page_writer: None,
            offset_of_chunk_header: None,
            statistics: StatisticsStruct::new(),
        }
    }

    pub(crate) fn write(&mut self, timestamp: i64, value: i32) -> Result<(), &str> {
        // Update statistics
        self.statistics.update(timestamp, value);

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

    pub(crate) fn serialize(&mut self, file: &mut dyn PositionedWrite) {
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

        let uncompressed_bytes = buffer_size;
        // We have no compression!
        let compressed_bytes = uncompressed_bytes;

        let mut page_buffer: Vec<u8> = vec![];
        // Uncompressed size
        utils::write_var_u32(uncompressed_bytes as u32, &mut page_buffer);
        // Compressed size
        utils::write_var_u32(compressed_bytes as u32, &mut page_buffer);
        // TODO serialize statistics
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

        file.write(&[5]).expect("write failed");   // Marker
        write_str(file, self.measurement_id);
        // Data Lenght
        utils::write_var_u32(page_buffer.len() as u32, file);
        // Data Type INT32 -> 1
        file.write(&[self.data_type.serialize()]).expect("write failed");
        // Compression Type UNCOMPRESSED -> 0
        file.write(&[self.compression.serialize()]).expect("write failed");
        // Encoding PLAIN -> 0
        file.write(&[self.encoding.serialize()]).expect("write failed");
        // End Chunk Header

        // Write the full page
        file.write_all(&page_buffer);
    }

    fn get_metadata(&self) -> ChunkMetadata<'a> {
        ChunkMetadata {
            measurement_id: self.measurement_id,
            data_type: self.data_type,
            // FIXME add this
            mask: 0,
            offset_of_chunk_header: match self.offset_of_chunk_header {
                None => {
                    panic!("get_metadata called before offset is defined");
                }
                Some(offset) => {
                    offset
                }
            } as i64,
            statistics: self.statistics.clone(),
        }
    }
}

struct GroupWriter<'a> {
    path: &'a Path,
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

    fn serialize(&mut self, file: &mut dyn PositionedWrite) -> Result<(), &str> {
        // Marker
        file.write(&[0]);
        // Chunk Group Header
        write_str(file, self.path.path.as_str());
        // End Group Header
        for (_, chunk_writer) in self.chunk_writers.iter_mut() {
            chunk_writer.serialize(file);
        }
        // TODO Footer?
        Ok(())
    }

    fn get_metadata(&self) -> ChunkGroupMetadata<'a> {
        ChunkGroupMetadata {
            device_id: &self.path.path,
            chunk_metadata: self.chunk_writers.iter().map(|(_, cw)| {
                cw.get_metadata()
            }).collect(),
        }
    }
}

#[derive(Copy, Clone)]
struct ChunkMetadata<'a> {
    measurement_id: &'a str,
    data_type: TSDataType,
    mask: u8,
    offset_of_chunk_header: i64,
    // statistics: Box<dyn Statistics>,
    statistics: StatisticsStruct<i32>,
}

impl Display for ChunkMetadata<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (...)", self.measurement_id)
    }
}

impl Serializable for ChunkMetadata<'_> {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        file.write_all(&self.offset_of_chunk_header.to_be_bytes());
        self.statistics.serialize(file)
    }
}

impl ChunkMetadata<'_> {
    fn serialize(&self, file: &mut dyn PositionedWrite, serialize_statistics: bool) -> io::Result<()> {
        let result = file.write_all(&self.offset_of_chunk_header.to_be_bytes());
        if serialize_statistics {
            self.statistics.serialize(file);
        }
        return result;
    }
}

struct ChunkGroupMetadata<'a> {
    device_id: &'a str,
    chunk_metadata: Vec<ChunkMetadata<'a>>,
}

impl ChunkGroupMetadata<'_> {
    fn new<'a>(device_id: &'a str) -> ChunkGroupMetadata {
        return ChunkGroupMetadata {
            device_id,
            chunk_metadata: vec![],
        };
    }
}


#[derive(Clone)]
enum MetadataIndexNodeType {
    LeafMeasurement,
    InternalMeasurement
}

struct TimeseriesMetadata {

}

#[derive(Clone)]
struct MetadataIndexEntry {
    name: String,
    offset: usize,
}

#[derive(Clone)]
struct MetadataIndexNode {
    children: Vec<MetadataIndexEntry>,
    end_offset: usize,
    node_type: MetadataIndexNodeType
}

impl MetadataIndexNode {
    fn new(node_type: MetadataIndexNodeType) -> MetadataIndexNode {
        MetadataIndexNode {
            children: vec![],
            end_offset: 0,
            node_type
        }
    }

    fn add_current_index_node_to_queue(current_index_node: &mut MetadataIndexNode, measurement_metadata_index_queue: &mut Vec<MetadataIndexNode>, file: &mut dyn PositionedWrite) {
        // TODO file.getPosition
        // currentIndexNode.setEndOffset(out.getPosition());
        current_index_node.end_offset = file.get_position() as usize;
        // metadataIndexNodeQueue.add(currentIndexNode);
        measurement_metadata_index_queue.push(current_index_node.clone());
    }

    fn generate_root_node(measurement_metadata_index_queue: &Vec<MetadataIndexNode>, file: &mut dyn PositionedWrite, node_type: MetadataIndexNodeType) -> MetadataIndexNode {
        // TODO
        MetadataIndexNode {
            children: vec![],
            end_offset: 0,
            node_type
        }
    }

    #[allow(unused_variables)]
    fn construct_metadata_index(device_timeseries_metadata_map: &HashMap<String, Vec<TimeSeriesMetadata>>, file: &mut dyn PositionedWrite) -> MetadataIndexNode {
        let mut device_metadata_index_map: HashMap<String, MetadataIndexNode> = HashMap::new();

        for (device, list_metadata) in device_timeseries_metadata_map.iter() {
            if list_metadata.is_empty() {
                continue;
            }

            let mut measurement_metadata_index_queue: Vec<MetadataIndexNode> = vec![];

            let timeseries_metadata: TimeseriesMetadata;
            let mut current_index_node: MetadataIndexNode = MetadataIndexNode::new(MetadataIndexNodeType::LeafMeasurement);

            // for (int i = 0; i < entry.getValue().size(); i++) {
            for i in 0..list_metadata.len() {
                let timeseries_metadata = list_metadata.get(i).unwrap();
                if i % GET_MAX_DEGREE_OF_INDEX_NODE == 0 {
                    if current_index_node.is_full() {
                        Self::add_current_index_node_to_queue(&mut current_index_node, &mut measurement_metadata_index_queue, file);
                        current_index_node = MetadataIndexNode::new(MetadataIndexNodeType::LeafMeasurement);
                    }
                }
                if current_index_node.is_full() {
                    // private static void addCurrentIndexNodeToQueue(
                    //       MetadataIndexNode currentIndexNode,
                    //       Queue<MetadataIndexNode> metadataIndexNodeQueue,
                    //       TsFileOutput out)
                    //       throws IOException {
                    //     currentIndexNode.setEndOffset(out.getPosition());
                    //     metadataIndexNodeQueue.add(currentIndexNode);
                    //   }
                    //     addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
                    //     currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LeafMeasurement);
                    current_index_node.end_offset = file.get_position() as usize;
                    measurement_metadata_index_queue.push(current_index_node.clone());

                    current_index_node = MetadataIndexNode::new(MetadataIndexNodeType::LeafMeasurement);

                }
                current_index_node.children.push(
                    MetadataIndexEntry {
                        name: timeseries_metadata.measurement_id.clone().to_owned(),
                        offset: file.get_position() as usize
                    }
                );
                timeseries_metadata.serialize(file);
            }
            // addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
            // deviceMetadataIndexMap.put(
            //       entry.getKey(),
            //       generateRootNode(
            //           measurementMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
            current_index_node.end_offset = file.get_position() as usize;
            measurement_metadata_index_queue.push(current_index_node.clone());

            device_metadata_index_map.insert(device.clone(), Self::generate_root_node(&measurement_metadata_index_queue, file, MetadataIndexNodeType::InternalMeasurement));

        }

        // // if not exceed the max child nodes num, ignore the device index and directly point to the
        // // measurement
        // if (deviceMetadataIndexMap.size() <= config.getMaxDegreeOfIndexNode()) {
        //   MetadataIndexNode metadataIndexNode =
        //       new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
        //   for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
        //     metadataIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition()));
        //     entry.getValue().serializeTo(out.wrapAsStream());
        //   }
        //   metadataIndexNode.setEndOffset(out.getPosition());
        //   return metadataIndexNode;
        // }
        //
        // // else, build level index for devices
        // Queue<MetadataIndexNode> deviceMetadataIndexQueue = new ArrayDeque<>();
        // MetadataIndexNode currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
        //
        // for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
        //   // when constructing from internal node, each node is related to an entry
        //   if (currentIndexNode.isFull()) {
        //     addCurrentIndexNodeToQueue(currentIndexNode, deviceMetadataIndexQueue, out);
        //     currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
        //   }
        //   currentIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition()));
        //   entry.getValue().serializeTo(out.wrapAsStream());
        // }
        // addCurrentIndexNodeToQueue(currentIndexNode, deviceMetadataIndexQueue, out);
        // MetadataIndexNode deviceMetadataIndexNode =
        //     generateRootNode(deviceMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_DEVICE);
        // deviceMetadataIndexNode.setEndOffset(out.getPosition());
        // return deviceMetadataIndexNode;

        // TODO remove
        MetadataIndexNode {
            children: vec![],
            end_offset: 0,
            node_type: MetadataIndexNodeType::LeafMeasurement
        }
    }
    fn is_full(&self) -> bool {
        return self.children.len() >= GET_MAX_DEGREE_OF_INDEX_NODE;
    }
}

struct TimeSeriesMetadata {
    time_series_metadata_type: u8,
    chunk_meta_data_list_data_size: usize,
    measurement_id: String,
    data_type: TSDataType,
    statistics: Box<dyn Statistics>,
    buffer: Vec<u8>,
}

impl Serializable for TimeSeriesMetadata {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        file.write_all(&[self.time_series_metadata_type]);
        write_str(file, self.measurement_id.as_str());
        file.write_all(&[self.data_type.serialize()]);
        write_var_u32(self.chunk_meta_data_list_data_size as u32, file);
        self.statistics.serialize(file);
        file.write_all(&self.buffer);
        Ok(())
    }
}

struct TsFileWriter<'a> {
    filename: &'a str,
    group_writers: HashMap<&'a Path, GroupWriter<'a>>,
    chunk_group_metadata: Vec<ChunkGroupMetadata<'a>>,
    timeseries_metadata_map: HashMap<String, Vec<TimeSeriesMetadata>>,
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

    fn flush_metadata_index(&mut self, file: &mut dyn PositionedWrite, chunk_metadata_list: &HashMap<Path, Vec<ChunkMetadata>>) -> MetadataIndexNode {
        for (path, metadata) in chunk_metadata_list {
            let data_type = metadata.get(0).unwrap().data_type;
            let serialize_statistic = metadata.len() > 1;
            let mut statistics: StatisticsStruct<i32> = StatisticsStruct::new();

            let mut buffer: Vec<u8> = vec![];

            for &m in metadata {
                if m.data_type != data_type {
                    continue;
                }
                // Serialize
                m.serialize(&mut buffer, serialize_statistic);
                statistics.merge(&m.statistics.to_struct_i32());
            }

            // Build Timeseries Index
            let timeseries_metadata = TimeSeriesMetadata {
                time_series_metadata_type: match serialize_statistic {
                    true => {
                        1
                    }
                    false => {
                        0
                    }
                } | &metadata.get(0).unwrap().mask,
                chunk_meta_data_list_data_size: buffer.len(),
                measurement_id: metadata.get(0).unwrap().measurement_id.to_owned(),
                data_type,
                statistics: Box::new(statistics),
                buffer,
            };

            // Add to the global struct
            let split = path.path.split(".").collect::<Vec<&str>>();
            let device_id = *split.get(0).unwrap();

            if !self.timeseries_metadata_map.contains_key(device_id) {
                self.timeseries_metadata_map.insert(device_id.to_owned(), vec![]);
            }

            self.timeseries_metadata_map.get_mut(device_id).unwrap().push(timeseries_metadata);
        }

        return MetadataIndexNode::construct_metadata_index(&self.timeseries_metadata_map, file);
    }

    #[allow(unused_variables)]
    pub(crate) fn _flush<'b>(&mut self, file: &'b mut dyn PositionedWrite) -> Result<(), &str> {
        // Start to write to file
        // Header
        // let mut file = File::create(self.filename).expect("create failed");
        let version: [u8; 1] = [3];

        // Header
        file.write("TsFile".as_bytes()).expect("write failed");
        file.write(&version).expect("write failed");
        // End of Header

        // Now iterate the
        for (_, group_writer) in self.group_writers.iter_mut() {
            // Write the group
            group_writer.serialize(file);
        }
        // Statistics
        // Fetch all metadata
        self.chunk_group_metadata = self.group_writers.iter().map(|(_, gw)| gw.get_metadata()).collect();

        // Create metadata list
        let mut chunk_metadata_map: HashMap<Path, Vec<ChunkMetadata>> = HashMap::new();
        for group_metadata in &self.chunk_group_metadata {
            for chunk_metadata in &group_metadata.chunk_metadata {
                let device_path = format!("{}.{}", &group_metadata.device_id, &chunk_metadata.measurement_id);
                let path = Path {
                    path: device_path.clone()
                };
                if !&chunk_metadata_map.contains_key(&path) {
                    &chunk_metadata_map.insert(path.clone(), vec![]);
                }
                &chunk_metadata_map.get_mut(&path).unwrap().push(chunk_metadata.clone());
            }
        }

        // Write Marker 0x02
        file.write_all(&[0x02]);

        let metadata_index_node = self.flush_metadata_index(file, &chunk_metadata_map);

        // TODO Write "real" statistics
        let statistics = [0x01, 0x04, 0x73, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6B, 0x03, 0x01, 0x04, 0x64, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x1F, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x80, 0x02, 0x05, 0x00, 0x00, 0x00];
        file.write_all(&statistics);

        // Footer
        file.write_all("@TsFile".as_bytes());
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<(), &str> {
        let mut file = WriteWrapper::new(File::create(self.filename).expect("create failed"));
        self._flush(&mut file)
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
            chunk_group_metadata: vec![],
            timeseries_metadata_map: HashMap::new(),
        }
    }
}

trait Encoder<DataType> {
    fn encode(&mut self, value: DataType);
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
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
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
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
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
            data_size: 0x20,
            data_type: 1,
            compression: 0,
            encoding: 0,
        };
    }
}

pub trait Serializable {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()>;
}

struct Chunk<'a> {
    header: ChunkHeader<'a>,
    num_pages: u8,
}

impl Serializable for Chunk<'_> {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        self.header.serialize(file)
    }
}

fn write_str(file: &mut dyn PositionedWrite, s: &str) -> io::Result<()> {
    let len = s.len() as u8 + 2;
    file.write(&[len]).expect("write failed");   // lenght (?)
    let bytes = s.as_bytes();
    file.write(bytes);   // measurement id
    Ok(())
}

impl Serializable for ChunkHeader<'_> {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
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
    let d1 = Path { path: "d1".to_owned() };
    measurement_groups_map.insert(&d1, measurement_group);
    let schema = Schema {
        measurement_groups: measurement_groups_map
    };
    let mut writer = TsFileWriter::new("data3.tsfile", schema);

    TsFileWriter::write(&mut writer, &d1, "s1", 1, 13);
    TsFileWriter::write(&mut writer, &d1, "s1", 10, 14);
    TsFileWriter::write(&mut writer, &d1, "s1", 100, 15);

    TsFileWriter::flush(&mut writer);

    ()
}

#[warn(dead_code)]
fn write_file_2() {
    std::fs::remove_file("data2.tsfile");

    let mut file = WriteWrapper::new(File::create("data2.tsfile").expect("create failed"));
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
    use std::collections::HashMap;

    use crate::{MeasurementGroup, MeasurementSchema, Path, Schema, TSDataType, TsFileWriter, write_file, write_file_2, write_file_3, WriteWrapper};
    use crate::compression::CompressionType;
    use crate::encoding::{TSEncoding};
    use crate::utils::{read_var_u32, write_var_u32};

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
    fn write_file_test_4() {
        let expectation = [0x54, 0x73, 0x46, 0x69, 0x6C, 0x65, 0x03, 0x00, 0x04, 0x64, 0x31, 0x05, 0x04, 0x73, 0x31, 0x20, 0x01, 0x00, 0x00, 0x1E, 0x1E, 0x1A, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x44, 0x1A, 0x1C, 0x1E,
            // TODO make this in HEX
            2, 0, 4, 115, 49, 1, 8, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 13, 0, 0, 0, 15, 0, 0, 0, 13, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 11, 1, 4, 115, 49, 0, 0, 0, 0, 0, 0, 0, 52, 0, 0, 0, 0, 0, 0, 0, 107, 3, 1, 4, 100, 49, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 0, 0, 0, 0, 128, 1, 0, 0, 0, 0, 0, 0, 0, 51, 31, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 128, 2, 5, 0, 0, 0, 64, 84, 115, 70, 105, 108, 101];

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
        let d1 = Path { path: "d1".to_owned() };
        measurement_groups_map.insert(&d1, measurement_group);
        let schema = Schema {
            measurement_groups: measurement_groups_map
        };
        let mut writer = TsFileWriter::new("data3.tsfile", schema);

        TsFileWriter::write(&mut writer, &d1, "s1", 1, 13);
        TsFileWriter::write(&mut writer, &d1, "s1", 10, 14);
        TsFileWriter::write(&mut writer, &d1, "s1", 100, 15);

        let buffer: Vec<u8> = vec![];

        let mut buffer_writer = WriteWrapper::new(buffer);

        writer._flush(&mut buffer_writer);

        assert_eq!(buffer_writer.writer, expectation);
        assert_eq!(buffer_writer.position, 202);
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
