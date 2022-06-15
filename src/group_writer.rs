use crate::chunk_writer::ChunkWriter;
use crate::{ChunkGroupMetadata, IoTDBValue, Path, PositionedWrite};
use std::collections::HashMap;
use std::io::Write;
use crate::tsfile_io_writer::TsFileIoWriter;
#[cfg(feature = "fast_hash")]
use ahash::AHashMap;


pub struct GroupWriter<'a> {
    pub(crate) path: &'a str,
    #[cfg(feature = "fast_hash")]
    pub(crate) chunk_writers: AHashMap<&'a str, ChunkWriter>,
    #[cfg(not(feature = "fast_hash"))]
    pub(crate) chunk_writers: HashMap<&'a str, ChunkWriter>,
    pub(crate) last_time_map: HashMap<&'a str, i64>
}

impl<'a> GroupWriter<'a> {
    pub(crate) fn get_last_time_map(&mut self) -> HashMap<&'a str, i64> {
        self.last_time_map.clone()
    }
}

impl<'a> GroupWriter<'a> {
    pub(crate) fn flush_to_filewriter<T: PositionedWrite>(&mut self, file_writer: &mut TsFileIoWriter<T>) -> u64 {
        log::info!("Start flush device id: {}", &self.path);

        self.seal_all_chunks();

        let current_chunk_group_size = self.get_current_chunk_group_size();

        for (_, series_writer) in self.chunk_writers.iter_mut() {
            series_writer.write_to_file_writer(file_writer);
        }

        return current_chunk_group_size;
    }

    pub(crate) fn update_max_group_mem_size(&mut self) -> u32 {
        let mut buffer_size = 0;
        for (_, chunk_writer) in self.chunk_writers.iter_mut() {
            let chunk_writer_size = chunk_writer.estimate_max_series_mem_size();
            log::trace!("Chunk Writer Size: {} for series {}", chunk_writer_size, chunk_writer.measurement_id);
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
        return size;
    }
}

impl<'a> GroupWriter<'a> {
    pub(crate) fn write(
        &mut self,
        measurement_id: &'a str,
        timestamp: i64,
        value: IoTDBValue,
    ) -> Result<u32, &str> {
        match &mut self.chunk_writers.get_mut(measurement_id) {
            Some(chunk_writer) => {
                Ok(chunk_writer.write(timestamp, value).unwrap())
            }
            None => Err("Unknown measurement id"),
        }
    }

    pub(crate) fn serialize(&mut self, file: &mut dyn PositionedWrite) -> Result<(), &str> {
        // // Marker
        // file.write(&[0]);
        // // Chunk Group Header
        // crate::write_str(file, self.path.path.as_str());
        // End Group Header
        for (_, chunk_writer) in self.chunk_writers.iter_mut() {
            chunk_writer.serialize(file);
        }
        // TODO Footer?
        Ok(())
    }

    pub(crate) fn get_metadata(&self) -> ChunkGroupMetadata {
        ChunkGroupMetadata::new(
            self.path.to_owned(),
            self.chunk_writers
                .iter()
                .map(|(_, cw)| cw.get_metadata())
                .collect(),
        )
    }
}
