// generated Rust module from Thrift IDL
mod sync;

use std::fs;
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
        interface.ips.get(0).map_or_else(||{String::from("127.0.0.1")}, |ip|{
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

    // let version = String::from("0.13");
    let version = String::from("0.13");
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
    client.init(String::from("root.sg")).expect("");

    // First sync a schema

    client.init_sync_data(String::from("1653988688846-1-0-0.tsfile")).expect("");
    let bytes = fs::read("../1653988688846-1-0-0.tsfile").expect("");
    client.sync_data(bytes.clone()).expect("");
    let digest = md5::compute(bytes);
    let md5 = format!("{:x}", digest);
    println!("Hash is: {}", md5);
    client.check_data_digest(md5.to_string()).expect("");
    client.end_sync();

    sleep(Duration::from_secs(100));

    // done!
    Ok(())
}
