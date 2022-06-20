use std::fs;
use std::fs::File;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use pnet::datalink;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel};
use tsfile_writer::compression::CompressionType;
use tsfile_writer::encoding::TSEncoding;
use tsfile_writer::errors::TsFileError;
use tsfile_writer::{IoTDBValue, Schema, TSDataType, WriteWrapper};
use tsfile_writer::schema::{DeviceBuilder, TsFileSchemaBuilder};
use tsfile_writer::tsfile_writer::TsFileWriter;
use crate::{calculate_digest, ConfirmInfo, PARTITION_INTERVAL, SyncServiceSyncClient, TSyncServiceSyncClient, write_mlog};

/// Lightweight pseudo-alternative to a full featured IoTDB Server
/// It accepts writes and regularly sends them to an IoTDB Server (running as receiver)
/// via the IoTDB Sync Protocol
struct IoTDBLight<'a> {
    folder: &'a str,
    config: IoTDBLightConfig,
    plans: Vec<IoTDBPlan>,
    schema: Schema<'a>,
    writer: Option<TsFileWriter<'a, WriteWrapper<File>>>,
    filename: Option<String>,
    absolute_filepath: Option<String>
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
    },
}

#[derive(Debug)]
enum IoTDBLightError {
    NoWriter,
    WriterError(TsFileError),
}

impl From<TsFileError> for IoTDBLightError {
    fn from(e: TsFileError) -> Self {
        IoTDBLightError::WriterError(e)
    }
}

struct IoTDBLightConfig {
    storage_group: String,
    remote_address: String,
}

impl Default for IoTDBLightConfig {
    fn default() -> Self {
        Self {
            storage_group: "sg".to_string(),
            remote_address: "127.0.0.1:5555".to_string(),
        }
    }
}

impl<'a> IoTDBLight<'a> {
    fn new(folder: &'a str, schema: Schema<'a>, config: IoTDBLightConfig) -> Self {
        let sg = (&config.storage_group).clone();
        let mut plans = vec![
            IoTDBPlan::SetStorageGroup {
                storage_group: sg
            }
        ];
        for (&device_id, mg) in schema.measurement_groups.iter() {
            for (measurement_id, measurement_schema) in mg.measurement_schemas.iter() {
                let mut path: String = device_id.to_owned();
                path.push_str(".");
                path.push_str(measurement_id);

                println!("Creating path: {}", path);

                plans.push(IoTDBPlan::CreateTimeSeries {
                    path,
                    data_type: TSDataType::INT32,
                    encoding: TSEncoding::PLAIN,
                    compression: CompressionType::UNCOMPRESSED,
                })
            }
        }
        Self {
            folder,
            config,
            plans,
            schema,
            writer: None,
            filename: None,
            absolute_filepath: None
        }
    }

    // pub(crate) fn create_timeseries(&mut self, device_id: &'a str, measurement_id: &'a str, data_type: TSDataType, encoding: TSEncoding, compression: CompressionType) -> Result<(), IoTDBLightError> {
    //     let mut path: String = device_id.to_owned();
    //     path.push_str(".");
    //     path.push_str(measurement_id);
    //
    //     self.schema_builder.add(device_id,
    //     DeviceBuilder::new().add(measurement_id, data_type, encoding, compression).build());
    //
    //     self.plans.push(IoTDBPlan::CreateTimeSeries {
    //         path,
    //         data_type,
    //         encoding,
    //         compression
    //     });
    //     Ok(())
    // }

    fn write(&mut self, device_id: &'a str, measurement_id: &'a str, timestamp: i64, value: IoTDBValue) -> Result<(), IoTDBLightError> {
        if self.writer.is_none() {
            // Initialize a writer
            let time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let filename = format!("{}.tsfile", time);
            let filepath = Path::new(self.folder).join(filename.clone()).as_os_str().to_str().unwrap().to_string();
            let writer = TsFileWriter::new(filepath.clone(), self.schema.clone(), Default::default())?;
            self.writer = Some(writer);
            self.filename = Some(filename);
            self.absolute_filepath = Some(filepath);
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

    pub(crate) fn sync(&mut self) -> Result<(), IoTDBLightError> {
        // Close the current file writer, and send it with an mlog to the server
        if self.writer.is_none() {
            return Err(IoTDBLightError::NoWriter);
        }

        self.writer.as_mut().unwrap().close();

        println!("connect to server on {}", &self.config.remote_address);
        let mut c = TTcpChannel::new();
        c.open(&self.config.remote_address).unwrap();

        let (i_chan, o_chan) = c.split().unwrap();

        let i_prot = TBinaryInputProtocol::new(TFramedReadTransport::new(i_chan), false);
        let o_prot = TBinaryOutputProtocol::new(TFramedWriteTransport::new(o_chan), false);

        let mut client = SyncServiceSyncClient::new(i_prot, o_prot);

        // Now send it to the sync server
        // GET Info
        let ip = datalink::interfaces().get(0).map(|interface| {
            interface.ips.get(0).map_or_else(
                || std::string::String::from("127.0.0.1"),
                |ip| ip.ip().to_string(),
            )
        });

        // Read UUID
        let uuid = match fs::read_to_string("uuid.lock") {
            Ok(content) => {
                println!("Using UUID from file: {}", content);
                content
            }
            Err(_) => {
                println!("Generating UUID...");
                let uuid = uuid::Uuid::new_v4().to_string().replace('-', "");
                // Save to file
                match fs::write("uuid.lock", &uuid) {
                    Ok(_) => {
                        println!("UUID saved successfully")
                    }
                    Err(_) => {
                        panic!("Unable to save uuid, aborting...")
                    }
                };
                uuid
            }
        };

        // let version = std::string::String::from("UNKNOWN");
        let version = std::string::String::from("0.13");
        let confirm = ConfirmInfo::new(ip, Some(uuid), Some(PARTITION_INTERVAL), Some(version));

        let result = client.check(confirm);

        match result {
            Ok(_) => {
                println!("Handshake successfull!")
            }
            Err(e) => {
                panic!("Unable to establish Handshake: {}", e);
            }
        }
        client.start_sync().expect("");

        let mut storage_group = "root.".to_string();
        storage_group.push_str(self.config.storage_group.as_str());

        client.init(storage_group).expect("");

        // First sync a schema
        client.init_sync_data(std::string::String::from("mlog.bin")).unwrap();

        // Create the mlog
        let mlog_bytes: Vec<u8> = write_mlog().expect("Unable to write mlog");

        client.sync_data(mlog_bytes.clone()).unwrap();

        let digest = calculate_digest(&mlog_bytes);
        println!("Digest of Sender: {}", digest);

        match client.check_data_digest(digest) {
            Ok(result) => {
                println!("Result: {}, {}", result.code, result.msg);
                if result.code == -1 {
                    panic!("Digest does not match!")
                }
            }
            _ => {
                panic!("Error on digest!")
            }
        }

        //   private String getFileInfoWithVgAndTimePartition(File file) {
        //   return file.getParentFile().getParentFile().getName()
        //       + SyncConstant.SYNC_DIR_NAME_SEPARATOR
        //       + file.getParentFile().getName()
        //       + SyncConstant.SYNC_DIR_NAME_SEPARATOR
        //       + file.getName();
        // }
        // Example path: data/data/sequence/root.sg1/0/0/xxx.tsfile
        let filename = format!("0_0_{}", self.writer.as_ref().unwrap().filename);

        client
            .init_sync_data(std::string::String::from(filename))
            .expect("");
        let bytes = fs::read("../1654074550252-1-0-0.tsfile").expect("");
        client.sync_data(bytes.clone()).expect("");
        let digest = calculate_digest(&bytes);
        println!("Digest of Sender: {}", digest);
        match client.check_data_digest(digest) {
            Ok(result) => {
                println!("Result: {}, {}", result.code, result.msg);
            }
            _ => {
                println!("Error on digest!")
            }
        }
        client.end_sync().unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tsfile_writer::compression::CompressionType;
    use tsfile_writer::encoding::TSEncoding;
    use tsfile_writer::{IoTDBValue, Schema, TSDataType};
    use crate::iotdb_light::{IoTDBLight, IoTDBLightError};

    #[test]
    fn init_server() -> Result<(), IoTDBLightError> {
        let schema = Schema::simple("d1", "s1", TSDataType::INT32, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED);
        let mut iotdb = IoTDBLight::new("/tmp/server1/", schema, Default::default());

        // Do something?
        iotdb.write("d1", "s1", 1, IoTDBValue::INT(15))?;

        iotdb.sync();

        Ok(())
    }
}
