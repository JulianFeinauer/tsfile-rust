use std::collections::HashMap;
use crate::{ChunkGroupHeader, ChunkGroupMetadata, ChunkMetadata, Path, PositionedWrite, Serializable};

pub struct TsFileIoWriter<T: PositionedWrite> {
    pub(crate) out: T,
    current_chunk_group_device_id: Option<String>,
    chunk_metadata_list: Vec<ChunkMetadata>,
    chunk_group_metadata_list: HashMap<String, Vec<ChunkMetadata>>,
}

impl<T: PositionedWrite> TsFileIoWriter<T> {
    pub(crate) fn new(writer: T) -> TsFileIoWriter<T> {
        let mut io_writer = TsFileIoWriter {
            out: writer,
            current_chunk_group_device_id: None,
            chunk_metadata_list: vec![],
            chunk_group_metadata_list: HashMap::new()
        };
        io_writer.start_file();
        return io_writer;
    }

    fn start_file(&mut self) {
        self.out.write("TsFile".as_bytes()).expect("write failed");
        self.out.write(&[0x03]).expect("write failed");
    }

    pub(crate) fn start_chunk_group(&mut self, device_id: String) {
        let chunk_group_header = ChunkGroupHeader::new(device_id.as_str());
        chunk_group_header.serialize(&mut self.out);

        self.current_chunk_group_device_id = Some(device_id);
        self.chunk_metadata_list.clear();
    }

    // public void endChunkGroup() throws IOException {
    //     if (currentChunkGroupDeviceId == null || chunkMetadataList.isEmpty()) {
    //       return;
    //     }
    //     chunkGroupMetadataList.add(
    //         new ChunkGroupMetadata(currentChunkGroupDeviceId, chunkMetadataList));
    //     currentChunkGroupDeviceId = null;
    //     chunkMetadataList = null;
    //     out.flush();
    //   }
    pub(crate) fn end_chunk_group(&mut self) {
        if self.current_chunk_group_device_id == None || self.chunk_metadata_list.is_empty() {
            return;
        }
        let device_id = self.current_chunk_group_device_id.clone().unwrap();
        self.chunk_group_metadata_list.insert(device_id.clone(), Vec::new());
        for chunk_metadata in &self.chunk_metadata_list {
            self.chunk_group_metadata_list.get_mut(device_id.as_str()).unwrap().push(
                chunk_metadata.clone()
            )
        }
        self.current_chunk_group_device_id = None;
        self.chunk_metadata_list.clear();
        self.out.flush();
    }

    fn end_file(&mut self) {

    }
}
