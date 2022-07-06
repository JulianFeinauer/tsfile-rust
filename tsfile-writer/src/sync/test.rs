use crate::sync::container_iotdb;
use crate::sync::container_iotdb::IoTDB;
use crate::sync::sync_sender::SyncSender;
use crate::writer::compression::CompressionType;
use crate::writer::encoding::TSEncoding;
use crate::writer::schema::TsFileSchemaBuilder;
use crate::writer::tsfile_writer::TsFileWriter;
use crate::writer::{IoTDBValue, Schema, TSDataType};
use iotdb::client::remote::{Config, RpcSession};
use iotdb::client::{DataSet, Session, Value};
use std::fs::metadata;
use std::thread::sleep;
use std::time::Duration;
use testcontainers::{
    core::WaitFor,
    images::{generic::GenericImage, hello_world::HelloWorld},
    *,
};

fn execute_test(test_code: impl FnOnce(String), assertions: impl FnOnce((Vec<String>, Vec<Vec<i64>>))) {
    let docker = clients::Cli::default();

    let _container = docker.run(IoTDB::new());

    println!("Container is ready...");

    let port = _container.get_host_port_ipv4(container_iotdb::SYNC_SERVER_PORT);
    println!("Exposed port: {}", port);

    let addr = format!("127.0.0.1:{}", port);

    test_code(addr);

    // Wait some seconds
    sleep(Duration::from_secs(5));

    // Check if the data is now present in iotdb
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: _container.get_host_port_ipv4(container_iotdb::SERVER_PORT) as i32,
        ..Default::default()
    };
    let mut session = RpcSession::new(&config).expect("");
    session.open().expect("");

    // Output all storage groups
    // session.set_storage_group("root.sg");

    println!("Execute Query");
    let result: Box<dyn DataSet> = session
        .execute_query_statement("SELECT * FROM root.**", None)
        .expect("");

    println!("Columns: {:?}", &result.get_column_names());

    let columns = (&result.get_column_names()).clone();
    let mut results: Vec<Vec<i64>> = Vec::new();

    result.for_each(|r| {
        println!("Iterating Record");
        let mut row: Vec<i64> = Vec::new();
        r.values.iter().for_each(|v| {
            match v {
                Value::Int64(v) => {
                    row.push(*v);
                    println!("Found value {}", *v)
                }
                _ => {
                    // nothing
                }
            }
        });
        results.push(row);
    });

    session.close();

    println!("Stopping...");
    _container.stop();

    assertions((columns, results));
}


#[test]
fn sync_once() {
    execute_test(|addr| {
        // Connect the sync server
        let mut sender = SyncSender::new(addr.as_str(), None, None).unwrap();

        // Now we could send over a file
        let schema = Schema::simple(
            "root.sg.d1",
            "s1",
            TSDataType::INT64,
            TSEncoding::PLAIN,
            CompressionType::UNCOMPRESSED,
        );

        let mut writer =
            TsFileWriter::new("test.tsfile", schema.clone(), Default::default()).expect("");
        writer.write("root.sg.d1", "s1", 1, IoTDBValue::LONG(13));
        writer.close();

        sender.sync("test.tsfile", "root.sg", Some(schema));
    }, |(columns, results)| {
        // Assertions (I have no idea why the column at the end occurs twice)
        assert_eq!(vec!["Time", "root.sg.d1.s1", "root.sg.d1.s1"], columns);
        assert_eq!(vec![vec![1_i64, 13_i64, 13_i64]], results);
    })
}

#[test]
fn sync_twice() {
    execute_test(|addr| {
        // Connect the sync server
        let mut sender = SyncSender::new(addr.as_str(), None, None).unwrap();

        // Now we could send over a file
        let schema = Schema::simple(
            "root.sg.d1",
            "s1",
            TSDataType::INT64,
            TSEncoding::PLAIN,
            CompressionType::UNCOMPRESSED,
        );

        // First run
        let mut writer =
            TsFileWriter::new("test.tsfile", schema.clone(), Default::default()).expect("");
        writer.write("root.sg.d1", "s1", 1, IoTDBValue::LONG(13));
        writer.close();
        sender.sync("test.tsfile", "root.sg", Some(schema.clone()));

        // Second run
        let mut sender = SyncSender::new(addr.as_str(), None, None).unwrap();
        let mut writer =
            TsFileWriter::new("test2.tsfile", schema.clone(), Default::default()).expect("");
        writer.write("root.sg.d1", "s1", 2, IoTDBValue::LONG(14));
        writer.close();
        // Do not try to recreate the schema now!
        sender.sync("test2.tsfile", "root.sg", None);
    }, |(columns, results)| {
        // Assertions (I have no idea why the column at the end occurs twice)
        assert_eq!(vec!["Time", "root.sg.d1.s1", "root.sg.d1.s1"], columns);
        assert_eq!(vec![
            vec![1_i64, 13_i64, 13_i64],
            vec![2_i64, 14_i64, 14_i64]
        ], results);
    })
}
