use std::borrow::Borrow;
use crate::chunk_writer::ChunkWriter;
use crate::group_writer::GroupWriter;
use crate::{
    BloomFilter, ChunkGroupMetadata, ChunkMetadata, IoTDBValue, MetadataIndexNode, Path,
    PositionedWrite, Schema, Serializable, Statistics, TimeSeriesMetadata, TimeSeriesMetadatable,
    TsFileMetadata, WriteWrapper,
};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Write;
use crate::ts_file_config::TsFileConfig;
use crate::tsfile_io_writer::TsFileIoWriter;

const CHUNK_GROUP_SIZE_THRESHOLD_BYTE: u32 = 128 * 1024 * 1024;

pub struct TsFileWriter<'a, T: PositionedWrite> {
    filename: String,
    pub(crate) file_io_writer: TsFileIoWriter<'a, T>,
    group_writers: BTreeMap<&'a str, GroupWriter<'a>>,
    chunk_group_metadata: Vec<ChunkGroupMetadata>,
    timeseries_metadata_map: HashMap<String, Vec<Box<dyn TimeSeriesMetadatable>>>,
    record_count: u32,
    record_count_for_next_mem_check: u32,
    non_aligned_timeseries_last_time_map: HashMap<&'a str, HashMap<&'a str, i64>>,
    pub schema: Schema<'a>,
    config: TsFileConfig
}

impl<'a, T: PositionedWrite> TsFileWriter<'a, T> {
    pub(crate) fn close(&mut self) {
        log::info!("start close file");
        self.flush_all_chunk_groups();
        self.file_io_writer.end_file();
    }
}

impl<'a, T: PositionedWrite> TsFileWriter<'a, T> {
    pub fn write(
        &mut self,
        device: &'a str,
        measurement_id: &'a str,
        timestamp: i64,
        value: IoTDBValue,
    ) -> Result<(), &str> {
        match self.group_writers.get_mut(device) {
            Some(group) => {
                let records_written = group.write(measurement_id, timestamp, value).unwrap();
                self.record_count += records_written;
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
            log::trace!("Memcount calculated: {}", mem_size);
            log::trace!("{:.2?}% - {} / {} for flushing", mem_size as f64/CHUNK_GROUP_SIZE_THRESHOLD_BYTE as f64 * 100.0, mem_size, CHUNK_GROUP_SIZE_THRESHOLD_BYTE);
            if mem_size > CHUNK_GROUP_SIZE_THRESHOLD_BYTE {
                self.record_count_for_next_mem_check = (self.record_count_for_next_mem_check as u64
                    * CHUNK_GROUP_SIZE_THRESHOLD_BYTE as u64/ mem_size as u64) as u32;
                return self.flush_all_chunk_groups();
            } else {
                // println!("Record Count: {}, CHUNK_GROUP_SIZE_THRESHOLD_BYTE: {}, memsize: {}", self.record_count_for_next_mem_check, CHUNK_GROUP_SIZE_THRESHOLD_BYTE, mem_size);
                // in the java impl there can be an overflow...
                self.record_count_for_next_mem_check = (self.record_count_for_next_mem_check as u64
                    * CHUNK_GROUP_SIZE_THRESHOLD_BYTE as u64/ mem_size as u64) as u32;
                log::trace!("Next record count for check {}", self.record_count_for_next_mem_check);
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
                self.file_io_writer.start_chunk_group(device_id.clone());
                let pos = self.file_io_writer.out.get_position();
                let data_size = group_writer.flush_to_filewriter(&mut self.file_io_writer);

                if self.file_io_writer.out.get_position() - pos != data_size {
                    panic!("Something went wrong!");
                }

                self.file_io_writer.end_chunk_group();

                self.non_aligned_timeseries_last_time_map.insert(device_id, group_writer.get_last_time_map());
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

    fn reset(&mut self) {
        self.record_count = 0;
        // Reset Group Writers
        let schema = self.schema.clone();
        self.group_writers = schema
            .measurement_groups
            .into_iter()
            .map(|(path, v)| {
                (
                    path,
                    GroupWriter {
                        path: path,
                        chunk_writers: v
                            .measurement_schemas
                            .iter()
                            .map(|(measurement_id, measurement_schema)| {
                                (
                                    measurement_id.clone(),
                                    ChunkWriter::new(
                                        measurement_id,
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

impl<'a> TsFileWriter<'a, WriteWrapper<File>> {
    // "Default" constructor to use... writes to a file
    pub(crate) fn new(filename: &'a str, schema: Schema<'a>, config: TsFileConfig) -> TsFileWriter<'a, WriteWrapper<File>> {
        let mut file =
            WriteWrapper::new(File::create(filename.clone()).expect("create failed"));

        TsFileWriter::new_from_writer(filename, schema, file, config)
    }
}

impl<'a, T: PositionedWrite> TsFileWriter<'a, T> {

    pub(crate) fn new_from_writer(filename: &'a str, schema: Schema<'a>, file_writer: T, config: TsFileConfig) -> TsFileWriter<'a, T> {
        let group_writers = schema.clone()
            .measurement_groups
            .into_iter()
            .map(|(path, v)| {
                (
                    path.borrow(),
                    GroupWriter {
                        path: path,
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
            group_writers,
            chunk_group_metadata: vec![],
            timeseries_metadata_map: HashMap::new(),
            record_count: 0,
            record_count_for_next_mem_check: 100,
            non_aligned_timeseries_last_time_map: HashMap::new(),
            config: config,
            file_io_writer: TsFileIoWriter::new(file_writer, config),
        }
    }
}
