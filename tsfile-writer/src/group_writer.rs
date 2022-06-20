use crate::chunk_writer::ChunkWriter;
use crate::errors::TsFileError;
use crate::tsfile_io_writer::TsFileIoWriter;
use crate::tsfile_writer::DataPoint;
use crate::{IoTDBValue, PositionedWrite};
use std::collections::BTreeMap;

pub struct GroupWriter<'a> {
    pub(crate) path: &'a str,
    pub(crate) chunk_writers: BTreeMap<&'a str, ChunkWriter>,
    pub(crate) last_time_map: BTreeMap<&'a str, i64>,
}

impl<'a> GroupWriter<'a> {
    pub(crate) fn write_many(
        &mut self,
        timestamp: i64,
        values: impl IntoIterator<Item=DataPoint<'a>>,
    ) -> Result<u32, TsFileError> {
        let mut records = 0;
        for dp in values {
            records += self.write(dp.measurement_id, timestamp, dp.value)?;
        }
        Ok(records)
    }
}

impl<'a> GroupWriter<'a> {
    pub(crate) fn get_last_time_map(&mut self) -> BTreeMap<&'a str, i64> {
        self.last_time_map.clone()
    }
}

impl<'a> GroupWriter<'a> {
    pub(crate) fn flush_to_filewriter<T: PositionedWrite>(
        &mut self,
        file_writer: &mut TsFileIoWriter<T>,
    ) -> u64 {
        log::info!("Start flush device id: {}", &self.path);

        self.seal_all_chunks();

        let current_chunk_group_size = self.get_current_chunk_group_size();

        for (_, series_writer) in self.chunk_writers.iter_mut() {
            series_writer.write_to_file_writer(file_writer);
        }

        current_chunk_group_size
    }

    pub(crate) fn update_max_group_mem_size(&mut self) -> u32 {
        let mut buffer_size = 0;
        for (_, chunk_writer) in self.chunk_writers.iter_mut() {
            let chunk_writer_size = chunk_writer.estimate_max_series_mem_size();
            log::trace!(
                "Chunk Writer Size: {} for series {}",
                chunk_writer_size,
                chunk_writer.measurement_id
            );
            buffer_size += chunk_writer_size;
        }
        buffer_size
    }
    fn seal_all_chunks(&mut self) {
        for (_, writer) in self.chunk_writers.iter_mut() {
            writer.seal_current_page();
        }
    }
    fn get_current_chunk_group_size(&mut self) -> u64 {
        // long size = 0;
        // for (IChunkWriter writer : chunkWriters.values()) {
        //   size += writer.getSerializedChunkSize();
        // }
        // return size;
        let mut size = 0;
        for (_, writer) in self.chunk_writers.iter_mut() {
            size += writer.get_serialized_chunk_size();
        }
        size
    }
}

impl<'a> GroupWriter<'a> {
    pub(crate) fn write(
        &mut self,
        measurement_id: &'a str,
        timestamp: i64,
        value: IoTDBValue,
    ) -> Result<u32, TsFileError> {
        // Check is historic
        self.check_is_history_data(measurement_id, timestamp)?;

        let record_count = match &mut self.chunk_writers.get_mut(measurement_id) {
            Some(chunk_writer) => chunk_writer.write(timestamp, value).unwrap(),
            None => {
                return Err(TsFileError::IllegalState {
                    source: Some("Unknown measurement id".to_owned()),
                });
            }
        };
        self.last_time_map.insert(measurement_id, timestamp);
        Ok(record_count)
    }

    fn check_is_history_data(
        &mut self,
        measurement_id: &'a str,
        timestamp: i64,
    ) -> Result<(), TsFileError> {
        if !self.last_time_map.contains_key(measurement_id) {
            self.last_time_map.insert(measurement_id, -1);
        }
        if timestamp <= *self.last_time_map.get(measurement_id).unwrap() {
            return Err(TsFileError::OutOfOrderData);
        }
        Ok(())
    }
}
