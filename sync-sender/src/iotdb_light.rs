use std::fs::File;
use tsfile_writer::compression::CompressionType;
use tsfile_writer::encoding::TSEncoding;
use tsfile_writer::errors::TsFileError;
use tsfile_writer::{IoTDBValue, Schema, TSDataType, WriteWrapper};
use tsfile_writer::schema::{DeviceBuilder, TsFileSchemaBuilder};
use tsfile_writer::tsfile_writer::TsFileWriter;

/// Lightweight pseudo-alternative to a full featured IoTDB Server
/// It accepts writes and regularly sends them to an IoTDB Server (running as receiver)
/// via the IoTDB Sync Protocol
struct IoTDBLight<'a> {
    folder: &'a str,
    config: IoTDBLightConfig,
    plans: Vec<IoTDBPlan>,
    schema_builder: TsFileSchemaBuilder<'a>,
    writer: Option<TsFileWriter<'a, WriteWrapper<File>>>
}

enum IoTDBPlan {
    CreateTimeSeries {
        path: String,
        data_type: TSDataType,
        encoding: TSEncoding,
        compression: CompressionType,
    },
    SetStorageGroup {
        storage_group: String
    }
}

#[derive(Debug)]
enum IoTDBLightError {
    NoWriter,
    WriterError(TsFileError)
}

impl From<TsFileError> for IoTDBLightError {
    fn from(e: TsFileError) -> Self {
        IoTDBLightError::WriterError(e)
    }
}

impl<'a> IoTDBLight<'a> {
    pub(crate) fn create_timeseries(&mut self, device_id: &'a str, measurement_id: &'a str, data_type: TSDataType, encoding: TSEncoding, compression: CompressionType) -> Result<(), IoTDBLightError> {
        let mut path: String = device_id.to_owned();
        path.push_str(".");
        path.push_str(measurement_id);

        self.schema_builder.add(device_id,
        DeviceBuilder::new().add(measurement_id, data_type, encoding, compression).build());

        self.plans.push(IoTDBPlan::CreateTimeSeries {
            path,
            data_type,
            encoding,
            compression
        });
        Ok(())
    }
}

struct IoTDBLightConfig {
    storage_group: String,
    remote_address: String
}

impl Default for IoTDBLightConfig {
    fn default() -> Self {
        Self {
            storage_group: "sg".to_string(),
            remote_address: "127.0.0.1:5555".to_string()
        }
    }
}

impl<'a> IoTDBLight<'a> {
    fn new(folder: &'a str, config: IoTDBLightConfig) -> Self {
        let sg = (&config.storage_group).clone();
        Self {
            folder,
            config,
            plans: vec![
                IoTDBPlan::SetStorageGroup {
                    storage_group: sg
                }
            ],
            schema_builder: TsFileSchemaBuilder::new(),
            writer: None
        }
    }

    fn write(&mut self, device_id: &'a str, measurement_id: &'a str, timestamp: i64, value: IoTDBValue) -> Result<(), IoTDBLightError> {
        if self.writer.is_none() {
            // Initialize a writer
            let schema = self.schema_builder.build();
            let writer = TsFileWriter::new(self.folder, schema, Default::default())?;
            self.writer = Some(writer);
        }
        match self.writer.as_mut() {
            None => {
                return Err(IoTDBLightError::NoWriter);
            }
            Some(mut writer) => {
                writer.write(device_id, measurement_id, timestamp, value)?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use tsfile_writer::compression::CompressionType;
    use tsfile_writer::encoding::TSEncoding;
    use tsfile_writer::{IoTDBValue, TSDataType};
    use crate::iotdb_light::{IoTDBLight, IoTDBLightError};

    #[test]
    fn init_server() -> Result<(), IoTDBLightError> {
        let mut iotdb = IoTDBLight::new("/tmp/server1/test.tsfile", Default::default());

        // Do something?
        iotdb.create_timeseries("d1", "s1", TSDataType::INT32, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED)?;

        iotdb.write("d1", "s1", 1, IoTDBValue::INT(15))?;

        Ok(())
    }
}
