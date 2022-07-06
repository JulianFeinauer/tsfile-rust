use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use pnet::datalink;
use sha2::Digest;
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TInputProtocol, TOutputProtocol,
};
use thrift::transport::{
    ReadHalf, TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel, WriteHalf,
};
use thrift::Error;

use crate::writer::errors::TsFileError;
use crate::writer::Schema;

use crate::sync::mlog::MLog;
use crate::sync::sync::{ConfirmInfo, SyncServiceSyncClient, TSyncServiceSyncClient};

// TODO what is this?
const PARTITION_INTERVAL: i64 = 604800;

pub struct SyncSender
{
    local_addr: String,
    uuid: String,
    version: String,
    client: SyncServiceSyncClient<TBinaryInputProtocol<TFramedReadTransport<ReadHalf<TTcpChannel>>>,
        TBinaryOutputProtocol<TFramedWriteTransport<WriteHalf<TTcpChannel>>>>,
}

#[derive(Debug)]
pub enum SyncSenderError {
    NoLocalIpFound,
    UnableGenerateUuid,
    ConnectionError,
    HandshakeError,
}

impl From<thrift::Error> for SyncSenderError {
    fn from(_: Error) -> Self {
        SyncSenderError::ConnectionError
    }
}

impl
    SyncSender
{
    #[allow(clippy::type_complexity)]
    pub fn new(
        remote_addr: &str,
        local_addr: Option<&str>,
        uuid: Option<&str>,
    ) -> Result<
        SyncSender,
        SyncSenderError,
    > {
        let local_addr = match local_addr {
            None => {
                // GET Info
                let ip = datalink::interfaces().get(0).map(|interface| {
                    interface.ips.get(0).map_or_else(
                        || std::string::String::from("127.0.0.1"),
                        |ip| ip.ip().to_string(),
                    )
                });
                if ip.is_none() {
                    return Err(SyncSenderError::NoLocalIpFound);
                }
                ip.unwrap()
            }
            Some(la) => la.to_string(),
        };
        let uuid = match uuid {
            Some(u) => u.to_string(),
            None => Self::get_or_generate_uuid()?,
        };

        // Connect client
        let mut c = TTcpChannel::new();
        c.open(remote_addr)?;

        let (i_chan, o_chan) = c.split()?;

        let i_prot = TBinaryInputProtocol::new(TFramedReadTransport::new(i_chan), false);
        let o_prot = TBinaryOutputProtocol::new(TFramedWriteTransport::new(o_chan), false);

        let client = SyncServiceSyncClient::new(i_prot, o_prot);

        // Do the handshake
        let mut sender = SyncSender {
            local_addr,
            uuid,
            version: "0.13".to_string(),
            client,
        };

        sender._connect()?;

        Ok(sender)
    }

    fn _connect(&mut self) -> Result<(), SyncSenderError> {
        let confirm = ConfirmInfo::new(
            self.local_addr.clone(),
            Some(self.uuid.clone()),
            Some(PARTITION_INTERVAL),
            Some(self.version.clone()),
        );

        let result = self.client.check(confirm);

        match result {
            Ok(_) => Ok(()),
            Err(_) => Err(SyncSenderError::HandshakeError),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn sync(
        &mut self,
        filename: &str,
        storage_group: &str,
        schema: Option<Schema>,
    ) -> Result<(), SyncSenderError> {
        self.client.start_sync().expect("");
        self.client.init(std::string::String::from(storage_group))?;

        // First sync a schema
        self.client
            .init_sync_data(std::string::String::from("mlog.bin"))?;

        // Create the mlog
        let mlog_bytes: Vec<u8> =
            Self::write_mlog(storage_group, schema).expect("Unable to write mlog");

        self.client.sync_data(mlog_bytes.clone())?;

        let digest = Self::calculate_digest(&mlog_bytes);
        println!("Digest of Sender: {}", digest);

        match self.client.check_data_digest(digest) {
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

        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        self.client
            .init_sync_data(format!("0_0_{}-1-0-0.tsfile", time).to_string())
            .expect("");
        let bytes = fs::read(filename).expect("");
        self.client.sync_data(bytes.clone()).expect("");
        let digest = Self::calculate_digest(&bytes);
        println!("Digest of Sender: {}", digest);
        match self.client.check_data_digest(digest) {
            Ok(result) => {
                println!("Result: {}, {}", result.code, result.msg);
            }
            _ => {
                println!("Error on digest!")
            }
        }
        self.client.end_sync()?;
        Ok(())
    }

    #[allow(dead_code)]
    fn calculate_digest(writer: &[u8]) -> std::string::String {
        let sha256 = sha2::Sha256::digest(&writer);
        let digest = sha256.as_slice();
        let digest = hex::encode(digest);

        // Remove leading 0es
        let digest = digest.trim_start_matches('0').to_string();

        digest
    }

    #[allow(dead_code)]
    fn write_mlog(storage_group: &str, schema: Option<Schema>) -> Result<Vec<u8>, TsFileError> {
        // Create the mlog
        let mut m_log = MLog::new();
        let mut mlog_buffer: Vec<u8> = vec![];

        // We can only do that if its the first sync (ever?!)
        if let Some(schema) = schema {
            m_log.set_storage_group_plan(storage_group)?;
            m_log.flush(&mut mlog_buffer)?;

            // Create a plan for each timeseries in Schema
            for (device_id, series) in schema.get_devices() {
                for (measurement_id, timeseries) in series.get_timeseries() {
                    let path = format!("{}.{}", device_id, measurement_id);
                    m_log.create_plan(
                        path.as_str(),
                        timeseries.data_type,
                        timeseries.encoding,
                        timeseries.compression,
                    )?;
                }
            }
        }
        m_log.flush(&mut mlog_buffer)?;

        Ok(mlog_buffer)
    }

    fn get_or_generate_uuid() -> Result<String, SyncSenderError> {
        // Read UUID
        match fs::read_to_string("uuid.lock") {
            Ok(content) => {
                println!("Using UUID from file: {}", content);
                Ok(content)
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
                        return Err(SyncSenderError::UnableGenerateUuid);
                    }
                };
                Ok(uuid)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::sync_sender::SyncSender;

    #[test]
    #[ignore]
    fn initalize() {
        let sender = SyncSender::new("129.168.169.1", None, None).unwrap();
    }
}
