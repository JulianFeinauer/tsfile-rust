use std::borrow::Borrow;
use std::collections::HashMap;
use crate::{BloomFilter, ChunkGroupHeader, ChunkGroupMetadata, ChunkMetadata, CompressionType, MetadataIndexNode, Path, PositionedWrite, Serializable, Statistics, TimeSeriesMetadata, TimeSeriesMetadatable, TSDataType, TSEncoding, TsFileConfig, TsFileMetadata};
use crate::chunk_writer::ChunkHeader;

pub struct TsFileIoWriter<'a, T: PositionedWrite> {
    config: TsFileConfig,
    pub(crate) out: T,
    current_chunk_group_device_id: Option<&'a str>,
    chunk_metadata_list: Vec<ChunkMetadata>,
    current_chunk_metadata: Option<ChunkMetadata>,
    chunk_group_metadata_list: Vec<ChunkGroupMetadata>,
    timeseries_metadata_map: HashMap<String, Vec<Box<dyn TimeSeriesMetadatable>>>,
}

impl<'a, T: PositionedWrite> TsFileIoWriter<'a, T> {
    pub(crate) fn end_current_chunk(&mut self) {
        match &self.current_chunk_metadata {
            None => {
                panic!("Something went wrong!");
            }
            Some(metadata) => {
                self.chunk_metadata_list.push(metadata.clone());
            }
        }
        self.current_chunk_metadata = None;
    }
}

impl<'a, T: PositionedWrite> TsFileIoWriter<'a, T> {
    pub(crate) fn start_flush_chunk(&mut self, measurement_id: String, compression: CompressionType, data_type: TSDataType, encoding: TSEncoding, statistics: Statistics, data_size: u32, num_pages: u32, mask: u8) {
        self.current_chunk_metadata = Some(ChunkMetadata::new(measurement_id.clone(), data_type, self.out.get_position(), statistics, mask));
        let header = ChunkHeader::new(
            measurement_id,
            data_size,
            data_type,
            compression,
            encoding,
            num_pages,
            mask
        );
        header.serialize(&mut self.out);
    }
}

impl<'a, T: PositionedWrite> TsFileIoWriter<'a, T> {
    pub(crate) fn new(writer: T, config: TsFileConfig) -> TsFileIoWriter<'a, T> {
        let mut io_writer = TsFileIoWriter {
            config,
            out: writer,
            current_chunk_group_device_id: None,
            chunk_metadata_list: vec![],
            current_chunk_metadata: None,
            chunk_group_metadata_list: vec![],
            timeseries_metadata_map: HashMap::new(),
        };
        io_writer.start_file();
        return io_writer;
    }

    fn start_file(&mut self) {
        self.out.write("TsFile".as_bytes()).expect("write failed");
        self.out.write(&[0x03]).expect("write failed");
    }

    pub(crate) fn start_chunk_group(&mut self, device_id: &'a str) {
        log::info!("Start chunk group:{}, file position {}", &device_id, self.out.get_position());
        let chunk_group_header = ChunkGroupHeader::new(device_id);
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
        // for chunk_metadata in &self.chunk_metadata_list {
        //     self.chunk_group_metadata_list.get_mut(device_id.as_str()).unwrap().push(
        //         chunk_metadata.clone()
        //     )
        // }
        self.chunk_group_metadata_list.push(ChunkGroupMetadata::new(device_id.into(), self.chunk_metadata_list.clone()));
        self.current_chunk_group_device_id = None;
        self.chunk_metadata_list.clear();
        self.out.flush();
    }

    pub(crate) fn end_file(&mut self) {
        // Statistics
        // Fetch all metadata
        // self.chunk_group_metadata = self
        //     .group_writers
        //     .iter()
        //     .map(|(_, gw)| gw.get_metadata())
        //     .collect();

        // Create metadata list
        let mut chunk_metadata_map: HashMap<Path, Vec<ChunkMetadata>> = HashMap::new();
        for group_metadata in &self.chunk_group_metadata_list {
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
        let meta_offset = self.out.get_position();

        // Write Marker 0x02
        self.out.write_all(&[0x02]);

        let metadata_index_node = self.flush_metadata_index(&chunk_metadata_map);

        let ts_file_metadata = TsFileMetadata::new(Some(metadata_index_node), meta_offset);

        let footer_index = self.out.get_position();

        ts_file_metadata.serialize(&mut self.out);

        // Now serialize the Bloom Filter ?!

        let paths = chunk_metadata_map
            .keys()
            .into_iter()
            .map(|path| path.clone())
            .collect();

        let bloom_filter = BloomFilter::build(paths, &self.config);

        bloom_filter.serialize(&mut self.out);

        let size_of_footer = (self.out.get_position() - footer_index) as u32;

        self.out.write_all(&size_of_footer.to_be_bytes());

        // Footer
        self.out.write_all("TsFile".as_bytes());
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
                    .insert(device_id.clone(), vec![]);
            }

            self.timeseries_metadata_map
                .get_mut(&device_id)
                .unwrap()
                .push(Box::new(timeseries_metadata));
        }

        return MetadataIndexNode::construct_metadata_index(&self.timeseries_metadata_map, &mut self.out, &self.config);
    }
}
