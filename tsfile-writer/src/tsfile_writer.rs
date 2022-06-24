//! Contains the TsFileWriter as central class to write tsfiles
use crate::chunk_writer::ChunkWriter;
use crate::errors::TsFileError;
use crate::group_writer::GroupWriter;
use crate::ts_file_config::TsFileConfig;
use crate::tsfile_io_writer::TsFileIoWriter;
use crate::{
    ChunkGroupMetadata, IoTDBValue, PositionedWrite, Schema, TimeSeriesMetadatable, WriteWrapper,
};
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::fs::{create_dir_all, File};

const CHUNK_GROUP_SIZE_THRESHOLD_BYTE: u32 = 128 * 1024 * 1024;

/// Class defined to hold a datapoint for writing into a given device.
/// Consists of a measurement_id and a value
pub struct DataPoint<'a> {
    pub(crate) measurement_id: &'a str,
    pub(crate) value: IoTDBValue,
}

impl<'a> DataPoint<'a> {
    pub fn new(measurement_id: &'a str, value: IoTDBValue) -> DataPoint<'a> {
        Self {
            measurement_id,
            value,
        }
    }
}

/// Central class to write TsFiles
/// a TsFileWriter always produces one file.
/// The file is opened on creation of the TsFileWriter and finished on closing.
/// It is not possible to append to files, always a new file has to be started.
pub struct TsFileWriter<'a, T: PositionedWrite> {
    #[allow(dead_code)]
    filename: String,
    pub(crate) file_io_writer: TsFileIoWriter<'a, T>,
    group_writers: BTreeMap<&'a str, GroupWriter<'a>>,
    #[allow(dead_code)]
    chunk_group_metadata: Vec<ChunkGroupMetadata>,
    #[allow(dead_code)]
    timeseries_metadata_map: HashMap<String, Vec<Box<dyn TimeSeriesMetadatable>>>,
    record_count: u32,
    record_count_for_next_mem_check: u32,
    non_aligned_timeseries_last_time_map: BTreeMap<&'a str, BTreeMap<&'a str, i64>>,
    pub schema: Schema<'a>,
    #[allow(dead_code)]
    config: TsFileConfig,
}

impl<'a, T: PositionedWrite> TsFileWriter<'a, T> {
    pub fn close(&mut self) {
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
    ) -> Result<(), TsFileError> {
        match self.group_writers.get_mut(device) {
            Some(group) => {
                let records_written = group.write(measurement_id, timestamp, value)?;
                self.record_count += records_written;
            }
            None => {
                return Err(TsFileError::IllegalState {
                    source: Some("No Group Writer found".to_owned()),
                });
            }
        }
        self.check_memory_size_and_may_flush_chunks();
        Ok(())
    }

    pub fn write_many(
        &mut self,
        device: &'a str,
        timestamp: i64,
        values: impl IntoIterator<Item = DataPoint<'a>>,
    ) -> Result<(), TsFileError> {
        match self.group_writers.get_mut(device) {
            Some(group) => {
                let records_written = group.write_many(timestamp, values)?;
                self.record_count += records_written;
            }
            None => return Err(TsFileError::IllegalState { source: None }),
        }
        self.check_memory_size_and_may_flush_chunks();
        Ok(())
    }

    fn check_memory_size_and_may_flush_chunks(&mut self) -> Result<bool, TsFileError> {
        if self.record_count >= self.record_count_for_next_mem_check {
            let mem_size = self.calculate_mem_size_for_all_groups();
            log::trace!("Memcount calculated: {}", mem_size);
            log::trace!(
                "{:.2?}% - {} / {} for flushing",
                mem_size as f64 / CHUNK_GROUP_SIZE_THRESHOLD_BYTE as f64 * 100.0,
                mem_size,
                CHUNK_GROUP_SIZE_THRESHOLD_BYTE
            );
            if mem_size > CHUNK_GROUP_SIZE_THRESHOLD_BYTE {
                self.record_count_for_next_mem_check = (self.record_count_for_next_mem_check as u64
                    * CHUNK_GROUP_SIZE_THRESHOLD_BYTE as u64
                    / mem_size as u64)
                    as u32;
                return self.flush_all_chunk_groups();
            } else {
                // println!("Record Count: {}, CHUNK_GROUP_SIZE_THRESHOLD_BYTE: {}, memsize: {}", self.record_count_for_next_mem_check, CHUNK_GROUP_SIZE_THRESHOLD_BYTE, mem_size);
                // in the java impl there can be an overflow...
                self.record_count_for_next_mem_check = (self.record_count_for_next_mem_check as u64
                    * CHUNK_GROUP_SIZE_THRESHOLD_BYTE as u64
                    / mem_size as u64)
                    as u32;
                log::trace!(
                    "Next record count for check {}",
                    self.record_count_for_next_mem_check
                );
                return Ok(false);
            }
        }
        Ok(false)
    }

    fn flush_all_chunk_groups(&mut self) -> Result<bool, TsFileError> {
        if self.record_count > 0 {
            for (&device_id, group_writer) in self.group_writers.iter_mut() {
                // self.file_writer.start_chunk_group(device_id);
                // self.file_writer
                self.file_io_writer.start_chunk_group(device_id)?;
                let pos = self.file_io_writer.out.get_position();
                let data_size = group_writer.flush_to_filewriter(&mut self.file_io_writer);

                if self.file_io_writer.out.get_position() - pos != data_size {
                    return Err(TsFileError::IllegalState {
                        source: Some("Bytes written are not as expected!".to_owned()),
                    });
                }

                self.file_io_writer.end_chunk_group();

                self.non_aligned_timeseries_last_time_map
                    .insert(device_id, group_writer.get_last_time_map());
            }
            self.reset();
        }
        Ok(true)
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
                        path,
                        chunk_writers: v
                            .measurement_schemas
                            .iter()
                            .map(|(&measurement_id, measurement_schema)| {
                                (
                                    measurement_id,
                                    ChunkWriter::new(
                                        measurement_id,
                                        measurement_schema.data_type,
                                        measurement_schema.compression,
                                        measurement_schema.encoding,
                                    ),
                                )
                            })
                            .collect(),
                        last_time_map: BTreeMap::new(),
                    },
                )
            })
            .collect();
    }
}

impl<'a> TsFileWriter<'a, WriteWrapper<File>> {
    // "Default" constructor to use... writes to a file
    pub fn new(
        filename: &'a str,
        schema: Schema<'a>,
        config: TsFileConfig,
    ) -> Result<TsFileWriter<'a, WriteWrapper<File>>, TsFileError> {
        // Create directory, if not exists
        let folder = match std::path::Path::new(filename).parent() {
            Some(f) => f,
            None => {
                return Err(TsFileError::Error { source: None });
            }
        };
        create_dir_all(folder);
        // Create the file
        let file = WriteWrapper::new(File::create(filename).expect("create failed"));

        TsFileWriter::new_from_writer(schema, file, config)
    }
}

impl<'a, T: PositionedWrite> TsFileWriter<'a, T> {
    pub(crate) fn new_from_writer(
        schema: Schema<'a>,
        file_writer: T,
        config: TsFileConfig,
    ) -> Result<TsFileWriter<'a, T>, TsFileError> {
        let group_writers = schema
            .clone()
            .measurement_groups
            .into_iter()
            .map(|(path, v)| {
                (
                    path.borrow(),
                    GroupWriter {
                        path,
                        chunk_writers: v
                            .measurement_schemas
                            .iter()
                            .map(|(&measurement_id, measurement_schema)| {
                                (
                                    measurement_id,
                                    ChunkWriter::new(
                                        measurement_id,
                                        measurement_schema.data_type,
                                        measurement_schema.compression,
                                        measurement_schema.encoding,
                                    ),
                                )
                            })
                            .collect(),
                        last_time_map: BTreeMap::new(),
                    },
                )
            })
            .collect();

        let io_writer = TsFileIoWriter::new(file_writer, config)?;
        Ok(TsFileWriter {
            filename: String::from(""),
            schema,
            group_writers,
            chunk_group_metadata: vec![],
            timeseries_metadata_map: HashMap::new(),
            record_count: 0,
            record_count_for_next_mem_check: 100,
            non_aligned_timeseries_last_time_map: BTreeMap::new(),
            config,
            file_io_writer: io_writer,
        })
    }
}
