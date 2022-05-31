// generated Rust module from Thrift IDL
mod sync;

use pnet::datalink;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol};
use thrift::protocol::{TInputProtocol, TOutputProtocol};
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
        TFramedReadTransport::new(i_chan), true
    );
    let o_prot = TBinaryOutputProtocol::new(
        TFramedWriteTransport::new(o_chan), true
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

    let uuid = uuid::Uuid::new_v4().to_string().replace("-", "");

    // let version = String::from("0.13");
    let version = String::from("UNKNOWN");
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
    client.init(String::from("sg")).expect("");
    client.init_sync_data(String::from("123.tsfile")).expect("");
    client.sync_data(vec![0, 1, 2, 3, 4, 5]).expect("");
    client.check_data_digest(String::from("1234")).expect("");
    client.end_sync();

    // done!
    Ok(())
}
