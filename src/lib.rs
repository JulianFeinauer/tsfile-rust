use std::{io, vec};
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::hash::{Hash};
use std::io::{Read, Write};
use arraydeque::ArrayDeque;

const GET_MAX_DEGREE_OF_INDEX_NODE: usize = 256;

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

fn write_var_i32(num: i32, buffer: &mut dyn Write) -> u8 {
    let mut uValue = num << 1;
    if num < 0 {
        uValue = !uValue;
    }
    return write_var_u32(uValue as u32, buffer);
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
    while b != u8::MAX && (b & 0x80) != 0 {
        value = value | (((b & 0x7F) as u32) << i);
        i = i + 7;
        b = read_byte(buffer);
    }
    return value | ((b as u32) << i);
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
        write_var_u32(time_buffer.len() as u32, &mut self.buffer);
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

    pub(crate) fn serialize(&mut self, file: &mut dyn Write) {
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
        write_var_u32(uncompressed_bytes as u32, &mut page_buffer);
        // Compressed size
        write_var_u32(compressed_bytes as u32, &mut page_buffer);
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
        file.write(&[5]).expect("write failed");   // Marker
        write_str(file, self.measurement_id);
        // Data Lenght
        write_var_u32(page_buffer.len() as u32, file);
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
            // FIXME add this!
            offset_of_chunk_header: 0,
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

    fn serialize(&mut self, file: &mut dyn Write) -> Result<(), &str> {
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

trait Statistics: Serializable {
    fn to_struct_i32(&self) -> StatisticsStruct<i32>;
}

#[derive(Copy, Clone)]
struct StatisticsStruct<T> {
    ts_first: i64,
    ts_last: i64,

    min_value: T,
    max_value: T,
    first_value: T,
    last_value: T,
    sum_value: i64,
}

impl StatisticsStruct<i32> {
    fn new() -> StatisticsStruct<i32> {
        StatisticsStruct {
            ts_first: i64::MAX,
            ts_last: i64::MIN,
            min_value: i32::MAX,
            max_value: i32::MIN,
            first_value: 0,
            last_value: 0,
            sum_value: 0,
        }
    }

    pub(crate) fn merge(&mut self, statistics: &StatisticsStruct<i32>) {
        if statistics.ts_first < self.ts_first {
            self.ts_first = statistics.ts_first;
            self.first_value = statistics.first_value;
        }
        if statistics.ts_last > self.ts_last {
            self.ts_last = statistics.ts_first;
            self.last_value = statistics.last_value;
        }
        if statistics.max_value > self.max_value {
            self.max_value = statistics.max_value;
        }
        if statistics.min_value < self.min_value {
            self.min_value = statistics.min_value;
        }
        self.sum_value = self.sum_value + statistics.sum_value;
    }

    fn update(&mut self, timestamp: i64, value: i32) {
        if timestamp < self.ts_first {
            self.ts_first = timestamp;
            self.first_value = value;
        }
        if timestamp > self.ts_last {
            self.ts_last = timestamp;
            self.last_value = value;
        }
        if value < self.min_value {
            self.min_value = value;
        }
        if value > self.max_value {
            self.max_value = value;
        }
        self.sum_value += value as i64;
    }
}

impl Serializable for StatisticsStruct<i32> {
    fn serialize(&self, file: &mut dyn Write) -> io::Result<()> {
        file.write_all(&self.min_value.to_be_bytes());
        file.write_all(&self.max_value.to_be_bytes());
        file.write_all(&self.first_value.to_be_bytes());
        file.write_all(&self.last_value.to_be_bytes());
        file.write_all(&self.sum_value.to_be_bytes())
    }
}

impl Statistics for StatisticsStruct<i32> {
    fn to_struct_i32(&self) -> StatisticsStruct<i32> {
        self.clone()
    }
}

#[derive(Copy, Clone)]
struct LongStatistics {
    ts_first: i64,
    ts_last: i64,

    min_value: i64,
    max_value: i64,
    first_value: i64,
    last_value: i64,
    sum_value: i64,
}

impl LongStatistics {
    fn new() -> LongStatistics {
        LongStatistics {
            ts_first: i64::MAX,
            ts_last: i64::MIN,
            min_value: i64::MAX,
            max_value: i64::MIN,
            first_value: 0,
            last_value: 0,
            sum_value: 0,
        }
    }

    fn update(&mut self, timestamp: i64, value: i64) {
        if timestamp < self.ts_first {
            self.ts_first = timestamp;
            self.first_value = value;
        }
        if timestamp > self.ts_last {
            self.ts_last = timestamp;
            self.last_value = value;
        }
        if value < self.min_value {
            self.min_value = value;
        }
        if value > self.max_value {
            self.max_value = value;
        }
        self.sum_value += value;
    }
}

impl Serializable for LongStatistics {
    fn serialize(&self, file: &mut dyn Write) -> io::Result<()> {
        file.write_all(&self.min_value.to_be_bytes());
        file.write_all(&self.max_value.to_be_bytes());
        file.write_all(&self.first_value.to_be_bytes());
        file.write_all(&self.last_value.to_be_bytes());
        file.write_all(&self.sum_value.to_be_bytes())
    }
}

impl Statistics for LongStatistics {
    fn to_struct_i32(&self) -> StatisticsStruct<i32> {
        // this should not work!
        todo!()
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
    fn serialize(&self, file: &mut dyn Write) -> io::Result<()> {
        file.write_all(&self.offset_of_chunk_header.to_be_bytes());
        self.statistics.serialize(file)
    }
}

impl ChunkMetadata<'_> {
    fn serialize(&self, file: &mut dyn Write, serialize_statistics: bool) -> io::Result<()> {
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
    LEAF_MEASUREMENT
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

    fn add_current_index_node_to_queue(current_index_node: &mut MetadataIndexNode, measurement_metadata_index_queue: &mut Vec<MetadataIndexNode>, file: &mut dyn Write) {
        // TODO file.getPosition
        // currentIndexNode.setEndOffset(out.getPosition());
        current_index_node.end_offset = 0;
        // metadataIndexNodeQueue.add(currentIndexNode);
        measurement_metadata_index_queue.push(current_index_node.clone());
    }

    fn construct_metadata_index(device_timeseries_metadata_map: &HashMap<String, Vec<TimeSeriesMetadata>>, file: &mut dyn Write) -> MetadataIndexNode {
        let device_metadata_index_map: HashMap<String, MetadataIndexNode> = HashMap::new();

        for (&device, &list_metadata) in device_timeseries_metadata_map {
            if list_metadata.is_empty() {
                continue;
            }

            let mut measurement_metadata_index_queue: Vec<MetadataIndexNode> = vec![];

            let timeseries_metadata: TimeseriesMetadata;
            let mut current_index_node: MetadataIndexNode = MetadataIndexNode::new(MetadataIndexNodeType::LEAF_MEASUREMENT);

            // for (int i = 0; i < entry.getValue().size(); i++) {
            for i in 0..list_metadata.len() {
                let timeseries_metadata = list_metadata.get(i).unwrap();
                if (i % GET_MAX_DEGREE_OF_INDEX_NODE == 0) {
                    if current_index_node.is_full() {
                        Self::add_current_index_node_to_queue(&mut current_index_node, &mut measurement_metadata_index_queue, file);
                        current_index_node = MetadataIndexNode::new(MetadataIndexNodeType::LEAF_MEASUREMENT);
                    }
                }
            //   if (currentIndexNode.isFull()) {
            //     addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
            //     currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
            //   }
            //   currentIndexNode.addEntry(
            //       new MetadataIndexEntry(timeseriesMetadata.getMeasurementId(), out.getPosition()));
            // }
            // timeseriesMetadata.serializeTo(out.wrapAsStream());
            }
        }

        // TODO remove
        MetadataIndexNode {
            children: vec![],
            end_offset: 0,
            node_type: MetadataIndexNodeType::LEAF_MEASUREMENT
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

    fn flush_metadata_index(&mut self, file: &mut dyn Write, chunk_metadata_list: &HashMap<Path, Vec<ChunkMetadata>>) -> MetadataIndexNode {
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

    pub(crate) fn _flush<'b>(&mut self, file: &'b mut dyn Write) -> Result<(), &str> {
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

        let metadata_index_node = self.flush_metadata_index(file, &chunk_metadata_map);


        // TODO Write "real" statistics
        let statistics = [0x02, 0x00, 0x04, 0x73, 0x31, 0x01, 0x08, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0B, 0x01, 0x04, 0x73, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6B, 0x03, 0x01, 0x04, 0x64, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x1F, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x80, 0x02, 0x05, 0x00, 0x00, 0x00];
        file.write_all(&statistics);

        // Footer
        file.write_all("@TsFile".as_bytes());
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<(), &str> {
        let mut file = File::create(self.filename).expect("create failed");
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

struct PlainInt32Encoder {
    values: Vec<i32>,
}

impl PlainInt32Encoder {
    pub(crate) fn serialize(&self, buffer: &mut Vec<u8>) {
        for val in &self.values {
            // Do the encoding into writeVarInt
            write_var_i32(*val, buffer);
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
    fn get_value_width(v: i64) -> u32 {
        return 64 - v.leading_zeros();
    }

    fn calculate_bit_widths_for_delta_block_buffer(&mut self, delta_block_buffer: &Vec<i64>) -> u32 {
        let mut width = 0;

        for i in 0..delta_block_buffer.len() {
            let v = *delta_block_buffer.get(i).expect("");
            let value_width = Self::get_value_width(v);
            width = max(width, value_width)
        }

        return width;
    }

    fn long_to_bytes(number: i64, result: &mut Vec<u8>, pos: usize, width: u32) {
        let mut cnt = (pos & 0x07) as u8;
        let mut index = pos >> 3;

        let mut my_width = width as u8;
        let mut my_number = number;
        while my_width > 0 {
            let m = match my_width + cnt >= 8 {
                true => { 8 - cnt }
                false => { my_width }
            };
            my_width = my_width - m;
            let old_count = cnt;
            cnt = cnt + m;
            let y = (number >> my_width) as u8;
            let y = y << (8 - cnt);

            // We need a mask like that
            // 0...0 (old-cnt-times) 1...1 (8-old-cnt-times)
            let mut new_mask: u8 = 0;
            for i in 0..(8 - old_count) {
                new_mask = new_mask | (1 << i);
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
                mask = mask | (1 << i);
            }
            my_number = my_number & mask;

            if cnt == 8 {
                index = index + 1;
                cnt = 0;
            }
        }
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
        let number_of_entries: u32 = self.values.len() as u32;
        buffer.write_all(&number_of_entries.to_be_bytes());
        // Write "write-width"
        buffer.write_all(&write_width.to_be_bytes());

        // Min Delta Base
        buffer.write_all(&self.min_delta.to_be_bytes());
        // First Value
        buffer.write_all(&self.first_value.expect("").to_be_bytes());
        // End Header

        // FIXME continue here...
        // now we can drop the long-to-bytes values here
        let mut payload_buffer = vec![];
        for i in 0..delta_block_buffer.len() {
            Self::long_to_bytes(delta_block_buffer[i], &mut payload_buffer, (i * write_width as usize) as usize, write_width);
        }

        let a = (delta_block_buffer.len() * write_width as usize) as f64;
        let b = a / 8.0;
        let encoding_length = b.ceil() as usize;

        // Copy over to "real" buffer
        buffer.write_all(payload_buffer.as_slice());


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
    fn serialize(&self, file: &mut dyn Write) -> io::Result<()> {
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
    fn serialize(&self, file: &mut dyn Write) -> io::Result<()> {
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

trait Serializable {
    fn serialize(&self, file: &mut dyn Write) -> io::Result<()>;
}

struct Chunk<'a> {
    header: ChunkHeader<'a>,
    num_pages: u8,
}

impl Serializable for Chunk<'_> {
    fn serialize(&self, file: &mut dyn Write) -> io::Result<()> {
        self.header.serialize(file)
    }
}

fn write_str(file: &mut dyn Write, s: &str) -> io::Result<()> {
    let len = s.len() as u8 + 2;
    file.write(&[len]).expect("write failed");   // lenght (?)
    let bytes = s.as_bytes();
    file.write(bytes);   // measurement id
    Ok(())
}

impl Serializable for ChunkHeader<'_> {
    fn serialize(&self, file: &mut dyn Write) -> io::Result<()> {
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
    use std::collections::HashMap;

    use crate::{CompressionType, MeasurementGroup, MeasurementSchema, Path, read_var_u32, Schema, TimeEncoder, TSDataType, TSEncoding, TsFileWriter, write_file, write_file_2, write_file_3, write_var_u32};

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
        let expectation = [0x54, 0x73, 0x46, 0x69, 0x6C, 0x65, 0x03, 0x00, 0x04, 0x64, 0x31, 0x05, 0x04, 0x73, 0x31, 0x20, 0x01, 0x00, 0x00, 0x1E, 0x1E, 0x1A, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x44, 0x1A, 0x1C, 0x1E];

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

        let mut buffer: Vec<u8> = vec![];
        writer._flush(&mut buffer);

        assert_eq!(buffer, expectation);
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

    #[test]
    fn test_long_to_bytes() {
        let mut result = vec![];
        let width = 4;
        TimeEncoder::long_to_bytes(1, &mut result, width * 0, width as u32);
        TimeEncoder::long_to_bytes(1, &mut result, width * 1, width as u32);
        TimeEncoder::long_to_bytes(1, &mut result, width * 2, width as u32);

        assert_eq!(result, [0b00010001, 0b00010000])
    }

    #[test]
    fn test_long_to_bytes_2() {
        let mut result = vec![];
        let width = 7;
        TimeEncoder::long_to_bytes(0b0000001, &mut result, width * 0, width as u32);
        TimeEncoder::long_to_bytes(0b0000001, &mut result, width * 1, width as u32);
        TimeEncoder::long_to_bytes(0b0000001, &mut result, width * 2, width as u32);

        assert_eq!(result, [0b00000010, 0b00000100, 0b00001000])
    }

    #[test]
    fn test_long_to_bytes_3() {
        let mut result = vec![];
        let width = 7;
        TimeEncoder::long_to_bytes(0, &mut result, width * 0, width as u32);
        TimeEncoder::long_to_bytes(81, &mut result, width * 1, width as u32);

        assert_eq!(result, [1, 68])
    }
}
