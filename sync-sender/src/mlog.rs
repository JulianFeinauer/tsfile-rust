use std::io::Write;

use tsfile_writer::compression::CompressionType;
use tsfile_writer::encoding::TSEncoding;
use tsfile_writer::errors::TsFileError;
use tsfile_writer::TSDataType;

pub struct MLog {
    bytes: Vec<u8>,
}

impl MLog {
    #[allow(dead_code)]
    pub fn new() -> MLog {
        MLog { bytes: vec![] }
    }

    #[allow(dead_code)]
    pub(crate) fn flush(&mut self, writer: &mut dyn Write) -> Result<usize, std::io::Error> {
        let checksum = Self::calculate_checksum(&self.bytes);
        let length = self.bytes.len() as i32;

        writer.write_all(&length.to_be_bytes())?;
        writer.write_all(&self.bytes)?;
        writer.write_all(&checksum.to_be_bytes())?;

        self.bytes.clear();

        Ok(0)
    }

    #[allow(dead_code)]
    fn calculate_checksum(bytes: &[u8]) -> i64 {
        crc32fast::hash(bytes) as i64
    }

    #[allow(dead_code)]
    pub fn create_plan(
        &mut self,
        path: &str,
        data_type: TSDataType,
        encoding: TSEncoding,
        compression: CompressionType,
    ) -> Result<(), TsFileError> {
        Self::write_create_plan(&mut self.bytes, path, data_type, encoding, compression)
    }

    #[allow(dead_code)]
    pub(crate) fn write_create_plan(
        writer: &mut dyn Write,
        path: &str,
        data_type: TSDataType,
        encoding: TSEncoding,
        compression: CompressionType,
    ) -> Result<(), TsFileError> {
        // stream.writeByte((byte) PhysicalPlanType.CREATE_TIMESERIES.ordinal());
        writer.write_all(&[0x04])?;
        // byte[] bytes = path.getFullPath().getBytes();
        let bytes = path.as_bytes();
        // stream.writeInt(bytes.length);
        writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
        // stream.write(bytes);
        writer.write_all(bytes)?;
        // stream.write(dataType.ordinal());
        writer.write_all(&[data_type.serialize()])?;
        // stream.write(encoding.ordinal());
        writer.write_all(&[encoding.serialize()])?;
        // stream.write(compressor.ordinal());
        writer.write_all(&[compression.serialize()])?;
        // stream.writeLong(tagOffset);
        writer.write_all(&(-1_i64).to_be_bytes())?;
        // // alias
        // if (alias != null) {
        //   stream.write(1);
        //   ReadWriteIOUtils.write(alias, stream);
        // } else {
        //   stream.write(0);
        writer.write_all(&[0x00])?;
        // }
        //
        // // props
        // if (props != null && !props.isEmpty()) {
        //   stream.write(1);
        //   ReadWriteIOUtils.write(props, stream);
        // } else {
        //   stream.write(0);
        writer.write_all(&[0x00])?;
        // }
        //
        // // tags
        // if (tags != null && !tags.isEmpty()) {
        //   stream.write(1);
        //   ReadWriteIOUtils.write(tags, stream);
        // } else {
        //   stream.write(0);
        writer.write_all(&[0x00])?;
        // }
        //
        // // attributes
        // if (attributes != null && !attributes.isEmpty()) {
        //   stream.write(1);
        //   ReadWriteIOUtils.write(attributes, stream);
        // } else {
        //   stream.write(0);
        writer.write_all(&[0x00])?;
        // }
        //
        // stream.writeLong(index);
        writer.write_all(&0_i64.to_be_bytes())?;

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn set_storage_group_plan(&mut self, path: &str) -> Result<(), TsFileError> {
        Self::write_set_storage_group_plan(&mut self.bytes, path)
    }

    #[allow(dead_code)]
    pub(crate) fn write_set_storage_group_plan(
        writer: &mut dyn Write,
        path: &str,
    ) -> Result<(), TsFileError> {
        // buffer.put((byte) PhysicalPlanType.SET_STORAGE_GROUP.ordinal());
        writer.write_all(&[0x03])?;
        // putString(buffer, path.getFullPath());
        let bytes = path.as_bytes();
        writer.write_all(&(bytes.len() as i32).to_be_bytes())?;
        writer.write_all(bytes)?;
        // buffer.putLong(index);
        writer.write_all(&(0x00_i64).to_be_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::MLog;
    use std::ops::Deref;
    use tsfile_writer::compression::CompressionType;
    use tsfile_writer::encoding::TSEncoding;
    use tsfile_writer::TSDataType;

    #[test]
    fn test_write_mlog_set_sg() {
        let expected = [
            0x00, 0x00, 0x00, 0x14, 0x03, 0x00, 0x00, 0x00, 0x07, 0x72, 0x6F, 0x6F, 0x74, 0x2E,
            0x73, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xD6, 0x4F, 0xBD, 0x17,
        ];

        let mut m_log = MLog::new();
        m_log.set_storage_group_plan("root.sg");
        // m_log.create_plan("root.sg.d1.s1", TSDataType::INT32, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED);

        let mut mlog_buffer: Vec<u8> = vec![];

        m_log.flush(&mut mlog_buffer).unwrap();

        assert_eq!(expected, mlog_buffer.deref());
    }

    #[test]
    fn test_write_mlog_create_ts() {
        let expected = [
            00, 0x00, 0x00, 0x29, 0x04, 0x00, 0x00, 0x00, 0x0D, 0x72, 0x6F, 0x6F, 0x74, 0x2E, 0x73,
            0x67, 0x2E, 0x64, 0x31, 0x2E, 0x73, 0x31, 0x01, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4E, 0xF4, 0xB9, 0x3D,
        ];

        let mut m_log = MLog::new();
        // m_log.set_storage_group_plan("root.sg");
        m_log.create_plan(
            "root.sg.d1.s1",
            TSDataType::INT32,
            TSEncoding::PLAIN,
            CompressionType::UNCOMPRESSED,
        );

        let mut mlog_buffer: Vec<u8> = vec![];

        m_log.flush(&mut mlog_buffer);

        assert_eq!(expected, mlog_buffer.deref());
    }

    #[test]
    fn test_write_mlog() {
        let expected = [
            00, 0x00, 0x00, 0x14, 0x03, 0x00, 0x00, 0x00, 0x07, 0x72, 0x6F, 0x6F, 0x74, 0x2E, 0x73,
            0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xD6,
            0x4F, 0xBD, 0x17, 0x00, 0x00, 0x00, 0x29, 0x04, 0x00, 0x00, 0x00, 0x0D, 0x72, 0x6F,
            0x6F, 0x74, 0x2E, 0x73, 0x67, 0x2E, 0x64, 0x31, 0x2E, 0x73, 0x31, 0x01, 0x00, 0x00,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4E, 0xF4, 0xB9, 0x3D,
        ];

        let mut m_log = MLog::new();
        let mut mlog_buffer: Vec<u8> = vec![];

        m_log.set_storage_group_plan("root.sg");
        m_log.flush(&mut mlog_buffer);

        m_log.create_plan(
            "root.sg.d1.s1",
            TSDataType::INT32,
            TSEncoding::PLAIN,
            CompressionType::UNCOMPRESSED,
        );
        m_log.flush(&mut mlog_buffer);

        assert_eq!(expected, mlog_buffer.deref());
    }
}
