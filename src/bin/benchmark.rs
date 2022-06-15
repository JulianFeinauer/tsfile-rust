use std::time::SystemTime;
use tsfile_rust::{IoTDBValue, Schema, TSDataType};
use tsfile_rust::compression::CompressionType;
use tsfile_rust::encoding::TSEncoding;
use tsfile_rust::test_utils::write_ts_file;

fn main() {
    let schema = Schema::simple("d1", "s", TSDataType::INT64, TSEncoding::PLAIN, CompressionType::UNCOMPRESSED);

    let mut durations: Vec<f64> = vec![];
    for _ in 0..10 {
        let start = SystemTime::now();
        write_ts_file("benchmark.tsfile", schema.clone(), |writer| {
            for i in 0..100000001 {
                writer.write("d1", "s", i, IoTDBValue::LONG(i));
            }
        });
        let end = SystemTime::now();

        let duration = end.duration_since(start);

        let duration_s = duration.unwrap_or_default().as_millis() as f64 / 1000_f64;
        durations.push(duration_s);
        println!("Execution took {:.2}s", duration_s)
    }

    println!("Results:");
    for d in durations.iter() {
        println!(" - {:.3}s", *d);
    }

    let count = *&durations.len() as f64;
    println!("Mean: {:.3}", durations.into_iter().reduce(|a, b| a + b).unwrap()/ count);
}
