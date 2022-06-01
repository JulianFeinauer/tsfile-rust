// generated Rust module from Thrift IDL
mod sync;

use std::fs;
use std::io::Write;
use std::str::Utf8Error;
use std::thread::sleep;
use std::time::Duration;
use pnet::datalink;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol};
use thrift::protocol::{TInputProtocol, TOutputProtocol};
use thrift::protocol::TType::String;
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport};
use thrift::transport::{TIoChannel, TTcpChannel};
use thrift::TThriftClient;
use uuid::Uuid;
use crate::sync::{ConfirmInfo, SyncServiceSyncClient, TSyncServiceSyncClient};
use sha2::Digest;

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

    let i_prot = TBinaryInputProtocol::new(
        TFramedReadTransport::new(i_chan), false
    );
    let o_prot = TBinaryOutputProtocol::new(
        TFramedWriteTransport::new(o_chan), false
    );

    let mut client = SyncServiceSyncClient::new(i_prot, o_prot);

    //
    // alright! - let's make some calls
    //

    // GET Info
    let ip = datalink::interfaces().get(0).map(|interface|{
        interface.ips.get(0).map_or_else(||{std::string::String::from("127.0.0.1")}, |ip|{
            ip.ip().to_string()
        })
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

    let version = std::string::String::from("UNKNOWN");
    // let version = std::string::String::from("0.13");
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
    let mut writer: Vec<u8> = vec![];
    write_set_storage_group_plan(&mut writer, "root.sg.d1.s1");
    write_create_plan(&mut writer, "root.sg.d1.s1", TSDataType::INT32, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED);

    let mut writer_with_length: Vec<u8> = vec![];
    writer_with_length.write(&(writer.len() as i32).to_be_bytes());
    writer_with_length.write_all(&writer);
    writer_with_length.write_all(&(277109834 as i64).to_be_bytes());

    client.sync_data(writer_with_length.clone());

    fs::write("mlog2.bin", writer_with_length.clone());

    let digest = calculate_digest(&mut writer_with_length);
    println!("Digest of Sender: {}", digest);

    match client.check_data_digest(digest) {
        Ok(result) => {
            println!("Result: {}, {}", result.code, result.msg);
            if result.code == -1 {
                panic!("Digest does not match!")
            }
        },
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
    let filename = "0_0_1654009177996-1-0-0.tsfile";

    client.init_sync_data(std::string::String::from(filename)).expect("");
    let bytes = fs::read("../1654009177996-1-0-0.tsfile").expect("");
    client.sync_data(bytes.clone()).expect("");
    let digest = calculate_digest(&bytes);
    println!("Digest of Sender: {}", digest);
    match client.check_data_digest(digest) {
        Ok(result) => {
            println!("Result: {}, {}", result.code, result.msg);
        },
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

use tsfile_rust::TSDataType;
use tsfile_rust::encoding::TSEncoding;
use tsfile_rust::compression::CompressionType;

fn write_create_plan(writer: &mut dyn Write, path: &str, data_type: TSDataType, encoding: TSEncoding, compression: CompressionType) {
    // stream.writeByte((byte) PhysicalPlanType.CREATE_TIMESERIES.ordinal());
    writer.write(&[0x04]);
    // byte[] bytes = path.getFullPath().getBytes();
    let bytes = path.as_bytes();
    // stream.writeInt(bytes.length);
    writer.write(&(bytes.len() as i32).to_be_bytes());
    // stream.write(bytes);
    writer.write(bytes);
    // stream.write(dataType.ordinal());
    writer.write(&[data_type.serialize()]);
    // stream.write(encoding.ordinal());
    writer.write(&[encoding.serialize()]);
    // stream.write(compressor.ordinal());
    writer.write(&[compression.serialize()]);
    // stream.writeLong(tagOffset);
    writer.write(&(-1 as i64).to_be_bytes());
    // // alias
    // if (alias != null) {
    //   stream.write(1);
    //   ReadWriteIOUtils.write(alias, stream);
    // } else {
    //   stream.write(0);
    writer.write(&[0x00]);
    // }
    //
    // // props
    // if (props != null && !props.isEmpty()) {
    //   stream.write(1);
    //   ReadWriteIOUtils.write(props, stream);
    // } else {
    //   stream.write(0);
    writer.write(&[0x00]);
    // }
    //
    // // tags
    // if (tags != null && !tags.isEmpty()) {
    //   stream.write(1);
    //   ReadWriteIOUtils.write(tags, stream);
    // } else {
    //   stream.write(0);
    writer.write(&[0x00]);
    // }
    //
    // // attributes
    // if (attributes != null && !attributes.isEmpty()) {
    //   stream.write(1);
    //   ReadWriteIOUtils.write(attributes, stream);
    // } else {
    //   stream.write(0);
    writer.write(&[0x00]);
    // }
    //
    // stream.writeLong(index);
    writer.write(&0_i64.to_be_bytes());
}

fn write_set_storage_group_plan(writer: &mut dyn Write, path: &str) {
    // buffer.put((byte) PhysicalPlanType.SET_STORAGE_GROUP.ordinal());
    writer.write(&[0x04]);
    // putString(buffer, path.getFullPath());
    let bytes = path.as_bytes();
    writer.write(&(bytes.len() as i32).to_be_bytes());
    // buffer.putLong(index);
    writer.write(&(0x00 as i64).to_be_bytes());
}

pub fn write_mlog() -> Vec<u8> {
    // Create the mlog
    let mut writer: Vec<u8> = vec![];
    write_set_storage_group_plan(&mut writer, "root.sg.d1.s1");
    write_create_plan(&mut writer, "root.sg.d1.s1", TSDataType::INT32, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED);

    let mut writer_with_length: Vec<u8> = vec![];
    writer_with_length.write(&(writer.len() as i32).to_be_bytes());
    writer_with_length.write_all(&writer);
    writer_with_length.write_all(&(277109834 as i64).to_be_bytes());
    return writer_with_length;
}

#[cfg(test)]
mod test {
    use std::ops::Deref;
    use crate::write_mlog;

    #[test]
    fn test_write_mlog() {
        let expected = [00, 0x00, 0x00, 0x14, 0x03, 0x00, 0x00, 0x00, 0x07, 0x72, 0x6F, 0x6F, 0x74, 0x2E, 0x73, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xD6, 0x4F, 0xBD, 0x17, 0x00, 0x00, 0x00, 0x29, 0x04, 0x00, 0x00, 0x00, 0x0D, 0x72, 0x6F, 0x6F, 0x74, 0x2E, 0x73, 0x67, 0x2E, 0x64, 0x31, 0x2E, 0x73, 0x31, 0x01, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4E, 0xF4, 0xB9, 0x3D];
        let mlog = write_mlog();

        assert_eq!(expected, mlog.deref());
    }
}
