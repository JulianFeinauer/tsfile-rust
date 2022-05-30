#![allow(unused_must_use)]

use std::{io, vec};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::hash::Hash;
use std::io::Write;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use compression::CompressionType;
use encoding::{TimeEncoder, TSEncoding};
use statistics::{Statistics, StatisticsStruct};
use crate::chunk_writer::{Chunkeable, ChunkMetadata, ChunkWriter};
use crate::encoding::Encoder;

use crate::MetadataIndexNodeType::LeafDevice;
use crate::murmur128::Murmur128;
use crate::statistics::StatisticsEnum;
use crate::utils::write_var_u32;

mod compression;
mod encoding;
mod statistics;
mod test;
mod utils;
mod murmur128;
mod schema;
mod chunk_writer;

const GET_MAX_DEGREE_OF_INDEX_NODE: usize = 256;
const GET_BLOOM_FILTER_ERROR_RATE: f64 = 0.05;

const MIN_BLOOM_FILTER_ERROR_RATE: f64 = 0.01;
const MAX_BLOOM_FILTER_ERROR_RATE: f64 = 0.1;
const MINIMAL_SIZE: i32 = 256;
const MAXIMAL_HASH_FUNCTION_SIZE: i32 = 8;
const SEEDS: [u8; 8] = [5, 7, 11, 19, 31, 37, 43, 59];

#[allow(dead_code)]
pub enum IoTDBValue {
    DOUBLE(f64),
    FLOAT(f32),
    INT(i32),
    LONG(i64),
}

pub trait PositionedWrite: Write {
    fn get_position(&self) -> u64;
}

struct WriteWrapper<T: Write> {
    position: u64,
    writer: T,
}

impl<T: Write> Write for WriteWrapper<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.writer.write(buf) {
            Ok(size) => {
                self.position += size as u64;
                Ok(size)
            }
            Err(e) => Err(e),
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
            writer,
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum TSDataType {
    INT32,
    INT64,
    FLOAT,
}

impl TSDataType {
    fn serialize(&self) -> u8 {
        match self {
            TSDataType::INT32 => 1,
            TSDataType::INT64 => 2,
            TSDataType::FLOAT => 3,
        }
    }
}

#[derive(Clone)]
struct MeasurementSchema {
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

impl MeasurementSchema {
    fn new(
        data_type: TSDataType,
        encoding: TSEncoding,
        compression: CompressionType,
    ) -> MeasurementSchema {
        MeasurementSchema {
            data_type,
            encoding,
            compression,
        }
    }
}

#[derive(Clone)]
pub struct MeasurementGroup {
    measurement_schemas: HashMap<String, MeasurementSchema>,
}

pub struct Schema {
    measurement_groups: HashMap<String, MeasurementGroup>,
}

struct PageWriter<T> {
    time_encoder: TimeEncoder,
    value_encoder: Box<dyn Encoder<T>>,
    // Necessary for writing
    buffer: Vec<u8>,
    phantom: PhantomData<T>
}


impl<T> PageWriter<T> {
    // TODO why is this never called?
    #[allow(dead_code)]
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

impl<T> PageWriter<T> {
    fn new<E: Encoder<T> + 'static>(encoder: E) -> PageWriter<T> {
        PageWriter {
            time_encoder: TimeEncoder::new(),
            value_encoder: Box::new(encoder),
            buffer: vec![],
            phantom: PhantomData::default()
        }
    }

    pub(crate) fn write(&mut self, timestamp: i64, value: T) -> Result<(), &str> {
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

struct GroupWriter {
    path: Path,
    chunk_writers: HashMap<String, Box<dyn Chunkeable>>,
}

impl GroupWriter {
    pub(crate) fn write(
        &mut self,
        measurement_id: String,
        timestamp: i64,
        value: IoTDBValue,
    ) -> Result<(), &str> {
        match &mut self.chunk_writers.get_mut(&measurement_id) {
            Some(chunk_writer) => {
                chunk_writer.write(timestamp, value);
                Ok(())
            }
            None => Err("Unknown measurement id"),
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

    fn get_metadata(&self) -> ChunkGroupMetadata {
        ChunkGroupMetadata::new(
            self.path.path.clone(),
            self
                .chunk_writers
                .iter()
                .map(|(_, cw)| cw.get_metadata())
                .collect(),
        )
    }
}

struct ChunkGroupMetadata {
    device_id: String,
    chunk_metadata: Vec<ChunkMetadata>,
}

impl ChunkGroupMetadata {
    fn new(device_id: String, chunk_metadata: Vec<ChunkMetadata>) -> ChunkGroupMetadata {
        return ChunkGroupMetadata {
            device_id,
            chunk_metadata,
        };
    }
}

#[derive(Clone)]
enum MetadataIndexNodeType {
    LeafMeasurement,
    InternalMeasurement,
    LeafDevice,
}

impl Serializable for MetadataIndexNodeType {
    //   /** INTERNAL_DEVICE */
    //   INTERNAL_DEVICE((byte) 0),
    //
    //   /** LEAF_DEVICE */
    //   LEAF_DEVICE((byte) 1),
    //
    //   /** INTERNAL_MEASUREMENT */
    //   INTERNAL_MEASUREMENT((byte) 2),
    //
    //   /** INTERNAL_MEASUREMENT */
    //   LEAF_MEASUREMENT((byte) 3);
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        let byte: u8 = match self {
            MetadataIndexNodeType::LeafMeasurement => {
                0x03
            }
            MetadataIndexNodeType::InternalMeasurement => {
                0x02
            }
            LeafDevice => {
                0x01
            }
        };
        file.write(&[byte]);

        Ok(())
    }
}

struct TimeseriesMetadata {}

#[derive(Clone)]
struct MetadataIndexEntry {
    name: String,
    offset: usize,
}

impl Serializable for MetadataIndexEntry {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        // int byteLen = 0;
        // byteLen += ReadWriteIOUtils.writeVar(name, outputStream);
        // byteLen += ReadWriteIOUtils.write(offset, outputStream);
        // return byteLen;
        write_str(file, self.name.as_str());
        file.write(&self.offset.to_be_bytes());

        Ok(())
    }
}

#[derive(Clone)]
struct MetadataIndexNode {
    children: Vec<MetadataIndexEntry>,
    end_offset: usize,
    node_type: MetadataIndexNodeType,
}

impl Serializable for MetadataIndexNode {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        // int byteLen = 0;
        // byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(children.size(), outputStream);
        // for (MetadataIndexEntry metadataIndexEntry : children) {
        //   byteLen += metadataIndexEntry.serializeTo(outputStream);
        // }
        // byteLen += ReadWriteIOUtils.write(endOffset, outputStream);
        // byteLen += ReadWriteIOUtils.write(nodeType.serialize(), outputStream);
        // return byteLen;
        write_var_u32(self.children.len() as u32, file);

        for metadata_index_entry in self.children.iter() {
            metadata_index_entry.serialize(file);
        }

        file.write(&self.end_offset.to_be_bytes());
        self.node_type.serialize(file);

        Ok(())
    }
}

impl MetadataIndexNode {
    fn new(node_type: MetadataIndexNodeType) -> MetadataIndexNode {
        MetadataIndexNode {
            children: vec![],
            end_offset: 0,
            node_type,
        }
    }

    fn add_current_index_node_to_queue(
        current_index_node: &mut MetadataIndexNode,
        measurement_metadata_index_queue: &mut Vec<MetadataIndexNode>,
        file: &mut dyn PositionedWrite,
    ) {
        // TODO file.getPosition
        // currentIndexNode.setEndOffset(out.getPosition());
        current_index_node.end_offset = file.get_position() as usize;
        // metadataIndexNodeQueue.add(currentIndexNode);
        measurement_metadata_index_queue.push(current_index_node.clone());
    }

    #[allow(unused_variables)]
    fn generate_root_node(
        mut measurement_metadata_index_queue: Vec<MetadataIndexNode>,
        file: &mut dyn PositionedWrite,
        node_type: MetadataIndexNodeType,
    ) -> MetadataIndexNode {
        // int queueSize = metadataIndexNodeQueue.size();
        // MetadataIndexNode metadataIndexNode;
        // MetadataIndexNode currentIndexNode = new MetadataIndexNode(type);
        // while (queueSize != 1) {
        //   for (int i = 0; i < queueSize; i++) {
        //     metadataIndexNode = metadataIndexNodeQueue.poll();
        //     // when constructing from internal node, each node is related to an entry
        //     if (currentIndexNode.isFull()) {
        //       addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
        //       currentIndexNode = new MetadataIndexNode(type);
        //     }
        //     currentIndexNode.addEntry(
        //         new MetadataIndexEntry(metadataIndexNode.peek().getName(), out.getPosition()));
        //     metadataIndexNode.serializeTo(out.wrapAsStream());
        //   }
        //   addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
        //   currentIndexNode = new MetadataIndexNode(type);
        //   queueSize = metadataIndexNodeQueue.size();
        // }
        // return metadataIndexNodeQueue.poll();
        // TODO
        let mut queue_size = measurement_metadata_index_queue.len();
        let mut metadata_index_node;
        let mut current_index_metadata = MetadataIndexNode::new(node_type.clone());

        while queue_size != 1 {
            for i in 0..queue_size {
                metadata_index_node = measurement_metadata_index_queue.get(measurement_metadata_index_queue.len() - 1).unwrap().clone();
                let device = match metadata_index_node.children.get(0) {
                    None => {
                        panic!("...")
                    }
                    Some(inner) => {
                        inner.name.clone()
                    }
                };
                measurement_metadata_index_queue.remove(measurement_metadata_index_queue.len() - 1);
                if current_index_metadata.is_full() {
                    current_index_metadata.end_offset = file.get_position() as usize;
                    measurement_metadata_index_queue.push(current_index_metadata.clone());
                }
                // ...
                let name = match metadata_index_node.children.get(0) {
                    None => {
                        panic!("This should not happen!")
                    }
                    Some(node) => {
                        node.name.clone()
                    }
                };
                current_index_metadata.children.push(MetadataIndexEntry {
                    name: name,
                    offset: file.get_position() as usize,
                });
            }
            // ...
            Self::add_current_index_node_to_queue(&mut current_index_metadata, &mut measurement_metadata_index_queue, file);
            current_index_metadata = MetadataIndexNode {
                children: vec![],
                end_offset: 0,
                node_type: node_type.clone(),
            };
            queue_size = measurement_metadata_index_queue.len();
        }
        return measurement_metadata_index_queue.get(0).unwrap().clone();
    }

    #[allow(unused_variables)]
    fn construct_metadata_index(
        device_timeseries_metadata_map: &HashMap<String, Vec<Box<dyn TimeSeriesMetadatable>>>,
        file: &mut dyn PositionedWrite,
    ) -> MetadataIndexNode {
        let mut device_metadata_index_map: HashMap<String, MetadataIndexNode> = HashMap::new();

        for (device, list_metadata) in device_timeseries_metadata_map.iter() {
            if list_metadata.is_empty() {
                continue;
            }

            let mut measurement_metadata_index_queue: Vec<MetadataIndexNode> = vec![];

            let timeseries_metadata: TimeseriesMetadata;
            let mut current_index_node: MetadataIndexNode =
                MetadataIndexNode::new(MetadataIndexNodeType::LeafMeasurement);

            // for (int i = 0; i < entry.getValue().size(); i++) {
            for i in 0..list_metadata.len() {
                let timeseries_metadata = list_metadata.get(i).unwrap();
                if i % GET_MAX_DEGREE_OF_INDEX_NODE == 0 {
                    if current_index_node.is_full() {
                        Self::add_current_index_node_to_queue(
                            &mut current_index_node,
                            &mut measurement_metadata_index_queue,
                            file,
                        );
                        current_index_node =
                            MetadataIndexNode::new(MetadataIndexNodeType::LeafMeasurement);
                    }
                    current_index_node.children.push(MetadataIndexEntry {
                        name: timeseries_metadata.get_measurement_id(),
                        offset: file.get_position() as usize,
                    });
                }
                timeseries_metadata.serialize(file);
            }
            // addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
            // deviceMetadataIndexMap.put(
            //       entry.getKey(),
            //       generateRootNode(
            //           measurementMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
            current_index_node.end_offset = file.get_position() as usize;
            measurement_metadata_index_queue.push(current_index_node.clone());

            let root_node = Self::generate_root_node(
                measurement_metadata_index_queue,
                file,
                MetadataIndexNodeType::InternalMeasurement,
            );
            device_metadata_index_map.insert(
                device.clone(),
                root_node,
            );
        }

        if device_metadata_index_map.len() <= GET_MAX_DEGREE_OF_INDEX_NODE {
            let mut metadata_index_node = MetadataIndexNode::new(LeafDevice);

            for (s, value) in device_metadata_index_map {
                metadata_index_node.children.push(MetadataIndexEntry {
                    name: s.clone(),
                    offset: file.get_position() as usize,
                });
                value.serialize(file);
            }
            metadata_index_node.end_offset = file.get_position() as usize;
            return metadata_index_node;
        }

        panic!("This is not yet implemented!");

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
    }
    fn is_full(&self) -> bool {
        return self.children.len() >= GET_MAX_DEGREE_OF_INDEX_NODE;
    }
}

trait TimeSeriesMetadatable {
    fn get_measurement_id(&self) -> String;
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()>;
}

impl TimeSeriesMetadatable for TimeSeriesMetadata {
    fn get_measurement_id(&self) -> String {
        self.measurement_id.clone()
    }

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


struct TimeSeriesMetadata {
    time_series_metadata_type: u8,
    chunk_meta_data_list_data_size: usize,
    measurement_id: String,
    data_type: TSDataType,
    statistics: StatisticsEnum,
    buffer: Vec<u8>,
}
//
// impl<T> Serializable for TimeSeriesMetadata<T> {
//     fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
//         file.write_all(&[self.time_series_metadata_type]);
//         write_str(file, self.measurement_id.as_str());
//         file.write_all(&[self.data_type.serialize()]);
//         write_var_u32(self.chunk_meta_data_list_data_size as u32, file);
//         self.statistics.serialize(file);
//         file.write_all(&self.buffer);
//         Ok(())
//     }
// }

struct HashFunction {
    cap: i32,
    seed: i32,
}

impl HashFunction {
    fn new(cap: i32, seed: i32) -> HashFunction {
        HashFunction {
            cap,
            seed,
        }
    }

    fn _murmur_hash(&self, s: &String, seed: i32) -> i32 {
        Murmur128::hash(s, seed)
    }

    fn hash(&self, value: &String) -> usize {
        // return Math.abs(Murmur128Hash.hash(value, seed)) % cap;
        (self._murmur_hash(value, self.seed).abs() % self.cap) as usize
    }
}

struct BloomFilter {
    size: i32,
    hash_function_size: i32,
    func: Vec<HashFunction>,
    bit_set: Vec<bool>,
}

impl BloomFilter {
    fn add(&mut self, path: String) {
        for f in self.func.iter() {
            let bit_id = f.hash(&path);
            // println!("{path} - {} -> {}", f.seed, bit_id);
            self.bit_set[bit_id] = true;
        }
    }

    fn new(size: i32, hash_function_size: i32) -> BloomFilter {
        let mut func = vec![];

        for i in 0..hash_function_size {
            func.push(HashFunction::new(size, SEEDS[i as usize] as i32));
        }

        let bit_set = vec![false; size as usize];

        BloomFilter {
            size,
            hash_function_size,
            func,
            bit_set,
        }
    }

    fn build(paths: Vec<Path>) -> BloomFilter {
        let mut filter = BloomFilter::empty_filter(GET_BLOOM_FILTER_ERROR_RATE, paths.len() as i32);

        for path in paths {
            filter.add(path.path);
        }

        filter
    }

    fn empty_filter(error_percent: f64, num_of_string: i32) -> BloomFilter {
        let mut error = error_percent;
        error = error.max(MIN_BLOOM_FILTER_ERROR_RATE);
        error = error.min(MAX_BLOOM_FILTER_ERROR_RATE);

        let ln2 = 2.0_f64.ln();

        let size = ((-1 * num_of_string) as f64 * error.ln() / ln2 / ln2) as i32 + 1;
        let hash_function_size = ((-1.0 * error.ln() / ln2) + 1.0) as i32;

        BloomFilter::new(
            size.max(MINIMAL_SIZE),
            hash_function_size.min(MAXIMAL_HASH_FUNCTION_SIZE),
        )
    }

    fn serialize_bits(&self) -> Vec<u8> {
        let number_of_bytes = if self.bit_set.len() % 8 == 0 {
            self.bit_set.len() / 8
        } else {
            (self.bit_set.len() + (self.bit_set.len() % 8)) / 8
        };

        let mut result = vec![0 as u8; number_of_bytes];

        for i in 0..self.bit_set.len() {
            let byte_index = (i - (i % 8)) / 8;
            let bit_index = i % 8;

            // println!("{} - {} / {} -> {}", i, byte_index, bit_index, self.bit_set[i]);

            // TODO why is this necessary?
            // if byte_index >= result.len() {
            //     println!("Skipped!!!!");
            //     continue;
            // }

            let value = *result.get(byte_index).unwrap();

            // See https://stackoverflow.com/questions/57449264/how-to-get-replace-a-value-in-rust-vec
            let bit: u8 = if self.bit_set[i] {
                0x01
            } else {
                0x00
            };
            std::mem::replace(&mut result[byte_index], value | (bit << bit_index));
        }

        return result;
    }
}

impl Serializable for BloomFilter {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        // Real
        let bytes = self.serialize_bits();

        write_var_u32(bytes.len() as u32, file);
        file.write_all(bytes.as_slice());
        write_var_u32(self.size as u32, file);
        write_var_u32(self.hash_function_size as u32, file);

        Ok(())
    }
}

struct TsFileMetadata {
    metadata_index: Option<MetadataIndexNode>,
    meta_offset: u64,
}

impl TsFileMetadata {
    pub fn new(metadata_index: Option<MetadataIndexNode>, meta_offset: u64) -> TsFileMetadata {
        TsFileMetadata {
            metadata_index,
            meta_offset,
        }
    }
}

impl Serializable for TsFileMetadata {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        match self.metadata_index.clone() {
            Some(index) => {
                index.serialize(file);
            }
            None => {
                // Write 0 as 4 bytes (u32)
                file.write_all(&(0x00 as u32).to_be_bytes());
            }
        }
        // Meta Offset
        file.write_all(&self.meta_offset.to_be_bytes());

        Ok(())
    }
}

struct TsFileWriter {
    filename: String,
    group_writers: HashMap<Path, GroupWriter>,
    chunk_group_metadata: Vec<ChunkGroupMetadata>,
    timeseries_metadata_map: HashMap<String, Vec<Box<dyn TimeSeriesMetadatable>>>,
}

impl TsFileWriter {
    pub(crate) fn write(
        &mut self,
        device: &str,
        measurement_id: &str,
        timestamp: i64,
        value: IoTDBValue,
    ) -> Result<(), &str> {
        let device = Path {
            path: String::from(device)
        };
        match self.group_writers.get_mut(&device) {
            Some(group) => {
                return group.write(String::from(measurement_id), timestamp, value);
            }
            None => {
                return Err("Unable to find group writer");
            }
        }
    }

    fn flush_metadata_index(
        &mut self,
        file: &mut dyn PositionedWrite,
        chunk_metadata_list: &HashMap<Path, Vec<ChunkMetadata>>,
    ) -> MetadataIndexNode {
        for (path, metadata) in chunk_metadata_list {
            let data_type = metadata.get(0).unwrap().data_type;
            let serialize_statistic = metadata.len() > 1;
            let mut statistics = StatisticsEnum::new(data_type);
            let mut buffer: Vec<u8> = vec![];

            for m in metadata {
                if m.data_type != data_type {
                    continue;
                }
                // Serialize
                m.serialize(&mut buffer, serialize_statistic);

                let statistic = &m.statistics;
                // Update the statistics
                statistics.merge(statistic);
            }

            // Build Timeseries Index
            let timeseries_metadata = TimeSeriesMetadata {
                time_series_metadata_type: match serialize_statistic {
                    true => 1,
                    false => 0,
                } | &metadata.get(0).unwrap().mask,
                chunk_meta_data_list_data_size: buffer.len(),
                measurement_id: metadata.get(0).unwrap().measurement_id.to_owned(),
                data_type,
                statistics: statistics,
                buffer,
            };

            // Add to the global struct
            let split = path.path.split(".").collect::<Vec<&str>>();
            let device_id = *split.get(0).unwrap();

            if !self.timeseries_metadata_map.contains_key(device_id) {
                self.timeseries_metadata_map
                    .insert(device_id.to_owned(), vec![]);
            }

            self.timeseries_metadata_map
                .get_mut(device_id)
                .unwrap()
                .push(Box::new(timeseries_metadata));
        }

        return MetadataIndexNode::construct_metadata_index(&self.timeseries_metadata_map, file);
    }

    #[allow(unused_variables)]
    fn _flush<'b>(&mut self, file: &'b mut dyn PositionedWrite) -> Result<(), &str> {
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
        self.chunk_group_metadata = self
            .group_writers
            .iter()
            .map(|(_, gw)| gw.get_metadata())
            .collect();

        // Create metadata list
        let mut chunk_metadata_map: HashMap<Path, Vec<ChunkMetadata>> = HashMap::new();
        for group_metadata in &self.chunk_group_metadata {
            for chunk_metadata in &group_metadata.chunk_metadata {
                let device_path = format!(
                    "{}.{}",
                    &group_metadata.device_id, &chunk_metadata.measurement_id
                );
                let path = Path {
                    path: device_path.clone(),
                };
                if !&chunk_metadata_map.contains_key(&path) {
                    &chunk_metadata_map.insert(path.clone(), vec![]);
                }
                &chunk_metadata_map
                    .get_mut(&path)
                    .unwrap()
                    .push(chunk_metadata.clone());
            }
        }

        // Get meta offset
        let meta_offset = file.get_position();

        // Write Marker 0x02
        file.write_all(&[0x02]);

        let metadata_index_node = self.flush_metadata_index(file, &chunk_metadata_map);

        let ts_file_metadata = TsFileMetadata::new(Some(metadata_index_node), meta_offset);

        let footer_index = file.get_position();

        ts_file_metadata.serialize(file);

        // Now serialize the Bloom Filter ?!

        let paths = chunk_metadata_map.keys().into_iter().map(|path| { path.clone() }).collect();

        let bloom_filter = BloomFilter::build(paths);

        bloom_filter.serialize(file);


        let size_of_footer = (file.get_position() - footer_index) as u32;

        file.write_all(&size_of_footer.to_be_bytes());

        // Footer
        file.write_all("TsFile".as_bytes());
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<(), &str> {
        let mut file = WriteWrapper::new(File::create(self.filename.clone()).expect("create failed"));
        self._flush(&mut file)
    }
}

impl TsFileWriter {
    fn new(filename: &str, schema: Schema) -> TsFileWriter {
        let group_writers = schema
            .measurement_groups
            .into_iter()
            .map(|(path, v)| {
                (
                    Path { path: path.clone() },
                    GroupWriter {
                        path: Path { path: path.clone() },
                        chunk_writers: v
                            .measurement_schemas
                            .iter()
                            .map(|(measurement_id, measurement_schema)| {
                                (
                                    measurement_id.clone(),
                                    ChunkWriter::<i32>::new(
                                        measurement_id.clone(),
                                        measurement_schema.data_type,
                                        measurement_schema.compression,
                                        measurement_schema.encoding,
                                    ),
                                )
                            })
                            .collect(),
                    },
                )
            })
            .collect();

        TsFileWriter {
            filename: String::from(filename),
            group_writers,
            chunk_group_metadata: vec![],
            timeseries_metadata_map: HashMap::new(),
        }
    }
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
}

impl Serializable for ChunkGroup<'_> {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        self.header.serialize(file)
    }
}

struct ChunkHeader<'a> {
    measurement_id: &'a str,
    data_size: u8,
}

impl ChunkHeader<'_> {
    fn new<'a>(measurement_id: &str) -> ChunkHeader {
        return ChunkHeader {
            measurement_id,
            data_size: 0x20,
        };
    }
}

pub trait Serializable {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()>;
}

struct Chunk<'a> {
    header: ChunkHeader<'a>,
}

impl Serializable for Chunk<'_> {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        self.header.serialize(file)
    }
}

fn write_str(file: &mut dyn PositionedWrite, s: &str) -> io::Result<()> {
    let len = s.len() as u8 + 2;
    file.write(&[len]).expect("write failed"); // lenght (?)
    let bytes = s.as_bytes();
    file.write(bytes); // measurement id
    Ok(())
}

impl Serializable for ChunkHeader<'_> {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
        // Chunk Header
        file.write(&[5]).expect("write failed"); // Marker
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
pub fn write_file_3() {
    let measurement_schema = MeasurementSchema::new(
        TSDataType::INT32,
        TSEncoding::PLAIN,
        CompressionType::UNCOMPRESSED,
    );

    let mut measurement_schema_map = HashMap::new();
    measurement_schema_map.insert(String::from("s1"), measurement_schema);
    let measurement_group = MeasurementGroup {
        measurement_schemas: measurement_schema_map,
    };
    let mut measurement_groups_map = HashMap::new();
    let d1 = Path {
        path: "d1".to_owned(),
    };
    measurement_groups_map.insert(d1.path.clone(), measurement_group);
    let schema = Schema {
        measurement_groups: measurement_groups_map,
    };
    let mut writer = TsFileWriter::new("data3.tsfile", schema);

    TsFileWriter::write(&mut writer, "d1", "s1", 1, IoTDBValue::INT(13));
    TsFileWriter::write(&mut writer, "d1", "s1", 10, IoTDBValue::INT(14));
    TsFileWriter::write(&mut writer, "d1", "s1", 100, IoTDBValue::INT(15));
    TsFileWriter::write(&mut writer, "d1", "s1", 1000, IoTDBValue::INT(16));
    TsFileWriter::write(&mut writer, "d1", "s1", 10000, IoTDBValue::INT(17));

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
        header: ChunkGroupHeader { device_id: "d1" },
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
    file.write(&[4]).expect("write failed"); // lenght (?)
    file.write("d1".as_bytes()).expect("write failed"); // device id
    // First Chunk
    // Chunk Header
    file.write(&[5]).expect("write failed"); // Marker
    file.write(&[4]).expect("write failed"); // lenght (?)
    file.write("s1".as_bytes()).expect("write failed"); // measurement id
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

    use crate::{IoTDBValue, MeasurementGroup, MeasurementSchema, Path, Schema, TSDataType, TsFileWriter, write_file, write_file_2, write_file_3, WriteWrapper};
    use crate::compression::CompressionType;
    use crate::encoding::TSEncoding;
    use crate::schema::{DeviceBuilder, TsFileSchemaBuilder};
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
        let expectation = [
            0x54, 0x73, 0x46, 0x69, 0x6C, 0x65, 0x03, 0x00, 0x04, 0x64, 0x31, 0x05, 0x04, 0x73,
            0x31, 0x20, 0x01, 0x00, 0x00, 0x1E, 0x1E, 0x1A, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x01, 0x44, 0x1A, 0x1C, 0x1E, // TODO make this in HEX
            2, 0, 4, 115, 49, 1, 8, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0,
            13, 0, 0, 0, 15, 0, 0, 0, 13, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0,
            0, 11, 1, 4, 115, 49, 0, 0, 0, 0, 0, 0, 0, 52, 0, 0, 0, 0, 0, 0, 0, 107, 3, 1, 4, 100,
            49, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 0, 0, 0, 0, 128, 1, 0, 0, 0, 0, 0, 0, 0, 51, 32,
            4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 2, 0, 128, 2, 5, 0, 0, 0, 65, 84, 115, 70, 105, 108, 101,
        ];

        let measurement_schema = MeasurementSchema::new(
            TSDataType::INT32,
            TSEncoding::PLAIN,
            CompressionType::UNCOMPRESSED,
        );

        let mut measurement_schema_map = HashMap::new();
        measurement_schema_map.insert(String::from("s1"), measurement_schema);
        let measurement_group = MeasurementGroup {
            measurement_schemas: measurement_schema_map,
        };
        let mut measurement_groups_map = HashMap::new();
        let d1 = Path {
            path: String::from("d1"),
        };
        measurement_groups_map.insert(d1.path.clone(), measurement_group);
        let schema = Schema {
            measurement_groups: measurement_groups_map,
        };
        let mut writer = TsFileWriter::new("data3.tsfile", schema);

        TsFileWriter::write(&mut writer, "d1", "s1", 1, IoTDBValue::INT(13));
        TsFileWriter::write(&mut writer, "d1", "s1", 10, IoTDBValue::INT(14));
        TsFileWriter::write(&mut writer, "d1", "s1", 100, IoTDBValue::INT(15));

        let buffer: Vec<u8> = vec![];

        let mut buffer_writer = WriteWrapper::new(buffer);

        writer._flush(&mut buffer_writer);

        assert_eq!(buffer_writer.writer, expectation);
        assert_eq!(buffer_writer.position, 203);
    }

    #[test]
    fn write_file_5() {
        let schema = TsFileSchemaBuilder::new()
            .add("d1", DeviceBuilder::new()
                .add("s1", TSDataType::INT32, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED)
                .add("s2", TSDataType::INT32, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED)
                .build(),
            )
            .build();

        let mut writer = TsFileWriter::new("data5.tsfile", schema);

        for i in 0..100 {
            writer.write("d1", "s1", i, IoTDBValue::INT(i as i32));
            writer.write("d1", "s2", i, IoTDBValue::INT(i as i32));
        }

        writer.flush();

        ()
    }

    #[test]
    fn write_i64() {
        let schema = TsFileSchemaBuilder::new()
            .add("d1", DeviceBuilder::new()
                .add("s1", TSDataType::INT64, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED)
                .build()
            )
            .build();

        let mut writer = TsFileWriter::new("write_long.tsfile", schema);

        let result = writer.write("d1", "s1", 0, IoTDBValue::LONG(0));

        match result {
            Ok(_) => {}
            Err(_) => {
                assert!(false);
            }
        }

        writer.flush();

        ()
    }

    #[test]
    fn write_float() {
        let schema = TsFileSchemaBuilder::new()
            .add("d1", DeviceBuilder::new()
                .add("s1", TSDataType::FLOAT, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED)
                .build()
            )
            .build();

        let mut writer = TsFileWriter::new("write_float.tsfile", schema);

        let result = writer.write("d1", "s1", 0, IoTDBValue::FLOAT(3.141));

        match result {
            Ok(_) => {}
            Err(_) => {
                assert!(false);
            }
        }

        writer.flush();

        ()
    }

    #[test]
    fn read_var_int() {
        for number in [
            1, 12, 123, 1234, 12345, 123456, 1234567, 12345678, 123456789,
        ] {
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
        assert_eq!(
            result.as_slice(),
            [0b10010101, 0b10011010, 0b11101111, 0b00111010]
        );
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
