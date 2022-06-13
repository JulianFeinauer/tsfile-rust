use crate::chunk_writer::ChunkWriter;
use crate::{ChunkGroupMetadata, IoTDBValue, Path, PositionedWrite};
use std::collections::HashMap;
use std::io::Write;

pub struct GroupWriter {
    pub(crate) path: Path,
    pub(crate) chunk_writers: HashMap<String, ChunkWriter>,
}

impl GroupWriter {
    pub(crate) fn update_max_group_mem_size(&mut self) -> u32 {
        let mut buffer_size = 0;
        for (_, chunk_writer) in self.chunk_writers.iter_mut() {
            let chunk_writer_size = chunk_writer.estimate_max_series_mem_size();
            println!("Chunk Writer Size: {} for series {}", chunk_writer_size, chunk_writer.measurement_id);
            buffer_size += chunk_writer_size;
        }
        buffer_size
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
            self.chunk_writers
                .iter()
                .map(|(_, cw)| cw.get_metadata())
                .collect(),
        )
    }
}
