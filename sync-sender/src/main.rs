// generated Rust module from Thrift IDL
mod mlog;
mod sync;

use crate::sync::{ConfirmInfo, SyncServiceSyncClient, TSyncServiceSyncClient};
use pnet::datalink;
use sha2::Digest;
use std::fs;
use std::io::Write;
use std::thread::sleep;
use std::time::Duration;
use thrift::protocol::TType::String;
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol,
};
use thrift::protocol::{TInputProtocol, TOutputProtocol};
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport};
use thrift::transport::{TIoChannel, TTcpChannel};
use thrift::TThriftClient;
use uuid::Uuid;

// TODO what is this?
const PARTITION_INTERVAL: i64 = 604800;

fn main() {
    match run() {
        Ok(()) => println!("client ran successfully"),
        Err(e) => {
            println!("client failed with {:?}", e);
            std::process::exit(1);
        }
    }
}

fn run() -> thrift::Result<()> {
    //
    // build client
    //

    println!("connect to server on 127.0.0.1:5555");
    let mut c = TTcpChannel::new();
    c.open("127.0.0.1:5555")?;

    let (i_chan, o_chan) = c.split()?;

    let i_prot = TBinaryInputProtocol::new(TFramedReadTransport::new(i_chan), false);
    let o_prot = TBinaryOutputProtocol::new(TFramedWriteTransport::new(o_chan), false);

    let mut client = SyncServiceSyncClient::new(i_prot, o_prot);

    //
    // alright! - let's make some calls
    //

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
            let uuid = uuid::Uuid::new_v4().to_string().replace("-", "");
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
    client.init(std::string::String::from("root.sg")).expect("");

    // First sync a schema
    client.init_sync_data(std::string::String::from("mlog.bin"));

    // Create the mlog
    let mlog_bytes: Vec<u8> = write_mlog();

    client.sync_data(mlog_bytes.clone());

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
    let filename = "0_0_1654074550252-1-0-0.tsfile";

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
    client.end_sync();

    sleep(Duration::from_secs(100));

    // done!
    Ok(())
}

fn calculate_digest(writer: &Vec<u8>) -> std::string::String {
    let sha256 = sha2::Sha256::digest(&writer);
    let digest = sha256.as_slice();
    let digest = hex::encode(digest);

    // Remove leading 0es
    let digest = digest.trim_start_matches("0").to_string();

    digest
}

use crate::mlog::MLog;
use tsfile_writer::compression::CompressionType;
use tsfile_writer::encoding::TSEncoding;
use tsfile_writer::TSDataType;

pub fn write_mlog() -> Vec<u8> {
    // Create the mlog
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

    return mlog_buffer;
}
