use crate::chunk_writer::ChunkWriter;
use crate::group_writer::GroupWriter;
use crate::{
    BloomFilter, ChunkGroupMetadata, ChunkMetadata, IoTDBValue, MetadataIndexNode, Path,
    PositionedWrite, Schema, Serializable, Statistics, TimeSeriesMetadata, TimeSeriesMetadatable,
    TsFileMetadata, WriteWrapper,
};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use crate::tsfile_io_writer::TsFileIoWriter;

const CHUNK_GROUP_SIZE_THRESHOLD_BYTE: u32 = 128 * 1024 * 1024;

pub struct TsFileWriter<T: PositionedWrite> {
    filename: String,
    pub(crate) file_io_writer: TsFileIoWriter<T>,
    group_writers: HashMap<Path, GroupWriter>,
    chunk_group_metadata: Vec<ChunkGroupMetadata>,
    timeseries_metadata_map: HashMap<String, Vec<Box<dyn TimeSeriesMetadatable>>>,
    record_count: u32,
    record_count_for_next_mem_check: u32,
    non_aligned_timeseries_last_time_map: HashMap<String, HashMap<String, i64>>,
    pub schema: Schema,
}

impl<T: PositionedWrite> TsFileWriter<T> {
    pub(crate) fn close(&mut self) {
        log::info!("start close file");
        self.flush_all_chunk_groups();
        self.file_io_writer.end_file();
    }
}

impl<T: PositionedWrite> TsFileWriter<T> {
    pub(crate) fn write(
        &mut self,
        device: &str,
        measurement_id: &str,
        timestamp: i64,
        value: IoTDBValue,
    ) -> Result<(), &str> {
        let device = Path {
            path: String::from(device),
        };
        match self.group_writers.get_mut(&device) {
            Some(group) => {
                group.write(String::from(measurement_id), timestamp, value);
                // TODO fetch from write operation
                self.record_count += 1;
            }
            None => {
                panic!("Unable to find group writer");
            }
        }
        self.check_memory_size_and_may_flush_chunks();
        Ok(())
    }

    fn check_memory_size_and_may_flush_chunks(&mut self) -> bool {
        if self.record_count >= self.record_count_for_next_mem_check {
            let mem_size = self.calculate_mem_size_for_all_groups();
            println!("Memcount calculated: {}", mem_size);
            println!("{:.2?}% - {} / {} for flushing", mem_size as f64/CHUNK_GROUP_SIZE_THRESHOLD_BYTE as f64 * 100.0, mem_size, CHUNK_GROUP_SIZE_THRESHOLD_BYTE);
            if mem_size > CHUNK_GROUP_SIZE_THRESHOLD_BYTE {
                self.record_count_for_next_mem_check = (self.record_count_for_next_mem_check as u64
                    * CHUNK_GROUP_SIZE_THRESHOLD_BYTE as u64/ mem_size as u64) as u32;
                return self.flush_all_chunk_groups();
            } else {
                // println!("Record Count: {}, CHUNK_GROUP_SIZE_THRESHOLD_BYTE: {}, memsize: {}", self.record_count_for_next_mem_check, CHUNK_GROUP_SIZE_THRESHOLD_BYTE, mem_size);
                // in the java impl there can be an overflow...
                self.record_count_for_next_mem_check = (self.record_count_for_next_mem_check as u64
                    * CHUNK_GROUP_SIZE_THRESHOLD_BYTE as u64/ mem_size as u64) as u32;
                println!("Next record count for check {}", self.record_count_for_next_mem_check);
                return false;
            }
        }
        return false;
    }

    fn flush_all_chunk_groups(&mut self) -> bool {
        if self.record_count > 0 {
            for (device_id, group_writer) in self.group_writers.iter_mut() {
                // self.file_writer.start_chunk_group(device_id);
                // self.file_writer
                self.file_io_writer.start_chunk_group(device_id.path.clone());
                let pos = self.file_io_writer.out.get_position();
                let data_size = group_writer.flush_to_filewriter(&mut self.file_io_writer);

                if self.file_io_writer.out.get_position() - pos != data_size {
                    panic!("Something went wrong!");
                }

                self.file_io_writer.end_chunk_group();

                self.non_aligned_timeseries_last_time_map.insert(device_id.path.clone(), group_writer.get_last_time_map());
            }
            self.reset();
        }
        true
    }

    fn calculate_mem_size_for_all_groups(&mut self) -> u32 {
        //     long memTotalSize = 0;
        // for (IChunkGroupWriter group : groupWriters.values()) {
        //   memTotalSize += group.updateMaxGroupMemSize();
        // }
        // return memTotalSize;
        let mut mem_total_size = 0_u32;
        for (_, group) in self.group_writers.iter_mut() {
            mem_total_size += group.update_max_group_mem_size();
        }
        mem_total_size
    }

    fn flush_metadata_index(
        &mut self,
        chunk_metadata_list: &HashMap<Path, Vec<ChunkMetadata>>,
    ) -> MetadataIndexNode {
        for (path, metadata) in chunk_metadata_list {
            let data_type = metadata.get(0).unwrap().data_type;
            let serialize_statistic = metadata.len() > 1;
            let mut statistics = Statistics::new(data_type);
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
            let mut device_id = "".to_owned();
            for i in 0..split.len() - 1 {
                if i > 0 {
                    device_id.push_str(".");
                }
                device_id.push_str(*split.get(i).unwrap());
            }

            if !self.timeseries_metadata_map.contains_key(&device_id) {
                self.timeseries_metadata_map
                    .insert(device_id.to_owned(), vec![]);
            }

            self.timeseries_metadata_map
                .get_mut(&device_id)
                .unwrap()
                .push(Box::new(timeseries_metadata));
        }

        return MetadataIndexNode::construct_metadata_index(&self.timeseries_metadata_map, &mut self.file_io_writer.out);
    }

    #[allow(unused_variables)]
    pub(crate) fn flush<'b>(&mut self) -> Result<(), &str> {
        // Now iterate the
        for (device, group_writer) in self.group_writers.iter_mut() {
            self.file_io_writer.start_chunk_group(device.path.clone());
            // Write the group
            group_writer.serialize(&mut self.file_io_writer.out);
            group_writer.flush_to_filewriter(&mut self.file_io_writer);
            self.file_io_writer.end_chunk_group();
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
        let meta_offset = self.file_io_writer.out.get_position();

        // Write Marker 0x02
        self.file_io_writer.out.write_all(&[0x02]);

        let metadata_index_node = self.flush_metadata_index(&chunk_metadata_map);

        let ts_file_metadata = TsFileMetadata::new(Some(metadata_index_node), meta_offset);

        let footer_index = self.file_io_writer.out.get_position();

        ts_file_metadata.serialize(&mut self.file_io_writer.out);

        // Now serialize the Bloom Filter ?!

        let paths = chunk_metadata_map
            .keys()
            .into_iter()
            .map(|path| path.clone())
            .collect();

        let bloom_filter = BloomFilter::build(paths);

        bloom_filter.serialize(&mut self.file_io_writer.out);

        let size_of_footer = (self.file_io_writer.out.get_position() - footer_index) as u32;

        self.file_io_writer.out.write_all(&size_of_footer.to_be_bytes());

        // Footer
        self.file_io_writer.out.write_all("TsFile".as_bytes());
        Ok(())
    }
    fn reset(&mut self) {
        self.record_count = 0;
        // Reset Group Writers
        let schema = self.schema.clone();
        self.group_writers = schema
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
                                    ChunkWriter::new(
                                        measurement_id.clone(),
                                        measurement_schema.data_type,
                                        measurement_schema.compression,
                                        measurement_schema.encoding,
                                    ),
                                )
                            })
                            .collect(),
                        last_time_map: HashMap::new()
                    },
                )
            })
            .collect();
    }
}

impl TsFileWriter<WriteWrapper<File>> {
    // "Default" constructor to use... writes to a file
    pub(crate) fn new(filename: &str, schema: Schema) -> TsFileWriter<WriteWrapper<File>> {
        let mut file =
            WriteWrapper::new(File::create(filename.clone()).expect("create failed"));

        TsFileWriter::new_from_writer(filename, schema, file)
    }
}

impl<T: PositionedWrite> TsFileWriter<T> {

    pub(crate) fn new_from_writer(filename: &str, schema: Schema, file_writer: T) -> TsFileWriter<T> {
        let group_writers = schema.clone()
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
                                    ChunkWriter::new(
                                        measurement_id.clone(),
                                        measurement_schema.data_type,
                                        measurement_schema.compression,
                                        measurement_schema.encoding,
                                    ),
                                )
                            })
                            .collect(),
                        last_time_map: HashMap::new()
                    },
                )
            })
            .collect();

        TsFileWriter {
            filename: String::from(filename),
            schema: schema,
            file_io_writer: TsFileIoWriter::new(file_writer),
            group_writers,
            chunk_group_metadata: vec![],
            timeseries_metadata_map: HashMap::new(),
            record_count: 0,
            record_count_for_next_mem_check: 100,
            non_aligned_timeseries_last_time_map: HashMap::new()
        }
    }
}
