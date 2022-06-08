use std::collections::HashMap;
use std::io::Write;
use crate::{Chunk, ChunkGroupMetadata, IoTDBValue, Path, PositionedWrite};
use crate::chunk_writer::ChunkWriter;

pub struct GroupWriter {
    pub(crate) path: Path,
    pub(crate) chunk_writers: HashMap<String, ChunkWriter>,
}

impl GroupWriter {
    pub(crate) fn update_max_group_mem_size(&self) -> u32 {
        // long bufferSize = 0;
        // for (IChunkWriter seriesWriter : chunkWriters.values()) {
        //   bufferSize += seriesWriter.estimateMaxSeriesMemSize();
        // }
        // return bufferSize;
        // let mut buffer_size = 0;
        // for series_writer in self.chunk_writers.values() {
        //     buffer_size += series_writer.estimate_max_series_mem_size();
        // }
        // buffer_size
        0
    }
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

    pub(crate) fn serialize(&mut self, file: &mut dyn PositionedWrite) -> Result<(), &str> {
        // Marker
        file.write(&[0]);
        // Chunk Group Header
        crate::write_str(file, self.path.path.as_str());
        // End Group Header
        for (_, chunk_writer) in self.chunk_writers.iter_mut() {
            chunk_writer.serialize(file);
        }
        // TODO Footer?
        Ok(())
    }

    pub(crate) fn get_metadata(&self) -> ChunkGroupMetadata {
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
