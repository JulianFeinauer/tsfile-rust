use crate::encoding::Encoder;
use crate::encoding::{PlainIntEncoder, TimeEncoder};
use crate::statistics::Statistics;
use crate::TSDataType::FLOAT;
use crate::{utils, write_str, CompressionType, IoTDBValue, PositionedWrite, Serializable, TSDataType, TSEncoding, ONLY_ONE_PAGE_CHUNK_HEADER, CHUNK_HEADER};
use std::fmt::{Display, Formatter};
use std::io;
use std::io::Write;

const MAX_NUMBER_OF_POINTS_IN_PAGE: u32 = 1048576;
const VALUE_COUNT_IN_ONE_PAGE_FOR_NEXT_CHECK: u32 = 7989;
const PAGE_SIZE_THRESHOLD: u32 = 65536;
const MINIMUM_RECORD_COUNT_FOR_CHECK: u32 = 1500;

struct PageWriter {
    time_encoder: TimeEncoder,
    value_encoder: Box<dyn Encoder>,
    data_type: TSDataType,
    statistics: Statistics,
    point_number: u32,
    // Necessary for writing
    buffer: Vec<u8>,
}

impl PageWriter {
    pub(crate) fn reset(&mut self) {
        self.statistics = Statistics::new(self.data_type);
        self.time_encoder.reset();
        self.value_encoder.reset();
        self.point_number = 0;
    }
}

impl PageWriter {
    pub(crate) fn estimate_max_mem_size(&mut self) -> u32 {
        let time_encoder_size = self.time_encoder.size();
        let value_encoder_size = self.value_encoder.size();
        let time_encoder_max_size = self.time_encoder.get_max_byte_size();
        let value_encoder_max_size = self.value_encoder.get_max_byte_size();
        let max_size = time_encoder_size
            + value_encoder_size
            + time_encoder_max_size
            + value_encoder_max_size;
        log::trace!("Max size estimated for page writer: {}", max_size);
        return max_size;
    }
}

impl PageWriter {
    fn new(data_type: TSDataType, encoding: TSEncoding) -> PageWriter {
        PageWriter {
            time_encoder: TimeEncoder::new(),
            value_encoder: <dyn Encoder>::new(data_type, encoding),
            data_type,
            statistics: Statistics::new(data_type),
            buffer: vec![],
            point_number: 0,
        }
    }

    fn write(&mut self, timestamp: i64, value: &IoTDBValue) -> Result<(), &str> {
        self.time_encoder.encode(timestamp);
        self.value_encoder.write(value);
        self.statistics.update(timestamp, value);
        self.point_number += 1;
        Ok(())
    }

    pub(crate) fn prepare_buffer(&mut self) {
        // serialize time_encoder and value encoder
        self.buffer.clear();
        let mut time_buffer = vec![];
        self.time_encoder.serialize(&mut time_buffer);
        crate::write_var_u32(time_buffer.len() as u32, &mut self.buffer);
        self.buffer.write_all(time_buffer.as_slice());
        self.value_encoder.serialize(&mut self.buffer);
    }
}

pub struct ChunkWriter {
    pub(crate) measurement_id: String,
    pub(crate) data_type: TSDataType,
    pub compression_type: CompressionType,
    pub encoding: TSEncoding,
    pub(crate) mask: u8,
    offset_of_chunk_header: Option<u64>,
    pub(crate) statistics: Statistics,
    current_page_writer: Option<PageWriter>,
    page_buffer: Vec<u8>,
    num_pages: u32,
    first_page_statistics: Option<Statistics>,
    value_count_in_one_page_for_next_check: u32,
    size_without_statistics: usize,
}

impl ChunkWriter {
    pub(crate) fn estimate_max_series_mem_size(&mut self) -> u32 {
        // return pageBuffer.size()
        // + pageWriter.estimateMaxMemSize()
        // + PageHeader.estimateMaxPageHeaderSizeWithoutStatistics()
        // + pageWriter.getStatistics().getSerializedSize();
        match &mut self.current_page_writer {
            Some(pw) => {
                let pw_mem_size = pw.estimate_max_mem_size();
                let stat_size = 2 * (4 + 1) + pw.statistics.get_serialized_size();
                let size = self.page_buffer.len() as u32 +
                    pw_mem_size +
                    // Header size
                    stat_size;
                println!("Estimated max series mem size: {}", size);
                size
            },
            None => {
                0
            }
        }
    }
}

impl ChunkWriter {
    pub fn write(&mut self, timestamp: i64, value: IoTDBValue) -> Result<(), &str> {
        // self.statistics.update(timestamp, &value);

        match &mut self.current_page_writer {
            None => {
                // Create a page
                self.current_page_writer = Some(PageWriter::new(self.data_type, self.encoding))
            }
            Some(_) => {
                // do nothing
            }
        }
        let page_writer = self.current_page_writer.as_mut().unwrap();
        page_writer.write(timestamp, &value);
        // check page size
        self.check_page_size_and_may_open_new_page();
        Ok(())
    }

    fn check_page_size_and_may_open_new_page(&mut self) {
        if self.current_page_writer.is_none() {
            return;
        }
        let page_writer = self.current_page_writer.as_mut().unwrap();
        if page_writer.point_number > MAX_NUMBER_OF_POINTS_IN_PAGE {
            self.write_page_to_buffer();
        } else if page_writer.point_number >= VALUE_COUNT_IN_ONE_PAGE_FOR_NEXT_CHECK {
            let current_page_size = page_writer.estimate_max_mem_size();

            if current_page_size > PAGE_SIZE_THRESHOLD {
                println!(
            "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{}, valueCountInOnePage:{}",
            self.measurement_id.clone(),
            PAGE_SIZE_THRESHOLD,
            current_page_size,
            page_writer.point_number);
                self.write_page_to_buffer();
                self.value_count_in_one_page_for_next_check = MINIMUM_RECORD_COUNT_FOR_CHECK;
            } else {
                self.value_count_in_one_page_for_next_check =
                    PAGE_SIZE_THRESHOLD / current_page_size * page_writer.point_number;
            }
        }
    }

    //   private void checkPageSizeAndMayOpenANewPage() {
    //   if (pageWriter.getPointNumber() == maxNumberOfPointsInPage) {
    //     logger.debug("current line count reaches the upper bound, write page {}", measurementSchema);
    //     writePageToPageBuffer();
    //   } else if (pageWriter.getPointNumber()
    //       >= valueCountInOnePageForNextCheck) { // need to check memory size
    //     // not checking the memory used for every value
    //     long currentPageSize = pageWriter.estimateMaxMemSize();
    //     if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold
    //       // we will write the current page
    //       logger.debug(
    //           "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{}, valueCountInOnePage:{}",
    //           measurementSchema.getMeasurementId(),
    //           pageSizeThreshold,
    //           currentPageSize,
    //           pageWriter.getPointNumber());
    //       writePageToPageBuffer();
    //       valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
    //     } else {
    //       // reset the valueCountInOnePageForNextCheck for the next page
    //       valueCountInOnePageForNextCheck =
    //           (int) (((float) pageSizeThreshold / currentPageSize) * pageWriter.getPointNumber());
    //     }
    //   }
    // }
    fn write_page_to_buffer(&mut self) {
        match self.current_page_writer.as_mut() {
            Some(page_writer) => {
                page_writer.prepare_buffer();

                let buffer_size: u32 = page_writer.buffer.len() as u32;

                let uncompressed_bytes = buffer_size;
                // We have no compression!
                let compressed_bytes = uncompressed_bytes;

                // TODO we need a change here if multiple pages exist
                if self.num_pages == 0 {
                    // Uncompressed size
                    self.size_without_statistics +=
                        utils::write_var_u32(uncompressed_bytes as u32, &mut self.page_buffer)
                            as usize;
                    // Compressed size
                    self.size_without_statistics +=
                        utils::write_var_u32(compressed_bytes as u32, &mut self.page_buffer)
                            as usize;

                    // Write page content
                    self.page_buffer.write_all(&page_writer.buffer);
                    &page_writer.buffer.clear();

                    self.first_page_statistics = Some(page_writer.statistics.clone())
                } else if self.num_pages == 1 {
                    let temp = self.page_buffer.clone();
                    self.page_buffer.clear();

                    log::trace!("Page Buffer offset: {}", self.page_buffer.get_position());
                    let header_bytes = &temp[0..self.size_without_statistics];
                    self.page_buffer
                        .write_all(&header_bytes);
                    log::trace!("Page Buffer offset: {}", self.page_buffer.get_position());
                    match &self.first_page_statistics {
                        Some(stat) => stat.serialize(&mut self.page_buffer),
                        _ => panic!("This should not happen!"),
                    };
                    log::trace!("Page Buffer offset: {}", self.page_buffer.get_position());
                    let remainder_bytes = &temp[self.size_without_statistics..];
                    self.page_buffer
                        .write_all(&remainder_bytes);
                    log::trace!("Page Buffer offset: {}", self.page_buffer.get_position());
                    // Uncompressed size
                    utils::write_var_u32(uncompressed_bytes as u32, &mut self.page_buffer);
                    // Compressed size
                    utils::write_var_u32(compressed_bytes as u32, &mut self.page_buffer);
                    log::trace!("Page Buffer offset: {}", self.page_buffer.get_position());
                    // Write page content
                    log::trace!("Statistics: {:?}", &page_writer.statistics);
                    page_writer.statistics.serialize(&mut self.page_buffer);

                    log::trace!("Flushing page at page buffer offset {}", self.page_buffer.get_position());

                    self.page_buffer.write_all(&page_writer.buffer);

                    log::trace!("Page Buffer offset: {}", self.page_buffer.get_position());

                    &page_writer.buffer.clear();
                    self.first_page_statistics = None;
                } else {
                    // Uncompressed size
                    utils::write_var_u32(uncompressed_bytes as u32, &mut self.page_buffer);
                    // Compressed size
                    utils::write_var_u32(compressed_bytes as u32, &mut self.page_buffer);
                    // Write page content
                    page_writer.statistics.serialize(&mut page_writer.buffer);
                    self.page_buffer.write_all(&page_writer.buffer);
                    println!("Wrote {} bytes to page buffer", &page_writer.buffer.len());
                    &page_writer.buffer.clear();
                }
                self.num_pages += 1;
                self.statistics.merge(&page_writer.statistics);
                page_writer.reset();
            }
            _ => {}
        };
    }
}

impl ChunkWriter {
    pub fn new(
        measurement_id: String,
        data_type: TSDataType,
        compression_type: CompressionType,
        encoding: TSEncoding,
    ) -> ChunkWriter {
        ChunkWriter {
            measurement_id,
            data_type,
            compression_type,
            encoding,
            mask: 0,
            offset_of_chunk_header: None,
            statistics: Statistics::new(data_type),
            current_page_writer: None,
            page_buffer: vec![],
            num_pages: 0,
            first_page_statistics: None,
            value_count_in_one_page_for_next_check: 0,
            size_without_statistics: 0,
        }
    }

    pub(crate) fn serialize(&mut self, file: &mut dyn PositionedWrite) {
        // Before we can write the header we have to serialize the current page
        self.write_page_to_buffer();

        // Chunk Header
        // store offset for metadata
        self.offset_of_chunk_header = Some(file.get_position());

        // Marker
        // (byte)((numOfPages <= 1 ? MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER : MetaMarker.CHUNK_HEADER) | (byte) mask),
        let marker = if self.num_pages <= 1 {
            ONLY_ONE_PAGE_CHUNK_HEADER
        } else {
            CHUNK_HEADER
        };
        let marker = marker | self.mask;
        file.write(&[marker]).expect("write failed"); // Marker

        write_str(file, self.measurement_id.as_str());
        // Data Lenght
        utils::write_var_u32(self.page_buffer.len() as u32, file);
        // Data Type INT32 -> 1
        file.write(&[self.data_type.serialize()])
            .expect("write failed");
        // Compression Type UNCOMPRESSED -> 0
        file.write(&[self.compression_type.serialize()])
            .expect("write failed");
        // Encoding PLAIN -> 0
        file.write(&[self.encoding.serialize()])
            .expect("write failed");
        // End Chunk Header

        log::trace!("Dumping pages at offset {}", file.get_position());

        // Write the full page
        file.write_all(&self.page_buffer);

        log::trace!("Offset after {}", file.get_position());
    }

    pub(crate) fn get_metadata(&self) -> ChunkMetadata {
        ChunkMetadata {
            measurement_id: self.measurement_id.clone(),
            data_type: self.data_type,
            // FIXME add this
            mask: 0,
            offset_of_chunk_header: match self.offset_of_chunk_header {
                None => {
                    panic!("get_metadata called before offset is defined");
                }
                Some(offset) => offset,
            } as i64,
            statistics: self.statistics.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ChunkMetadata {
    pub(crate) measurement_id: String,
    pub(crate) data_type: TSDataType,
    pub(crate) mask: u8,
    offset_of_chunk_header: i64,
    pub(crate) statistics: Statistics,
}

impl Display for ChunkMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (...)", self.measurement_id)
    }
}

impl ChunkMetadata {
    pub(crate) fn serialize(
        &self,
        file: &mut dyn PositionedWrite,
        serialize_statistics: bool,
    ) -> io::Result<()> {
        let result = file.write_all(&self.offset_of_chunk_header.to_be_bytes());
        if serialize_statistics {
            self.statistics.serialize(file);
        }
        return result;
    }
}
