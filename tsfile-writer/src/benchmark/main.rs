use crate::{CompressionType, IoTDBValue, TSDataType, TSEncoding, TsFileWriter, write_file_3};
use crate::schema::{DeviceBuilder, TsFileSchemaBuilder};

#[test]
fn benchmark() {
    for _ in 0..100 {
        use std::time::Instant;
        let now = Instant::now();

        // Code block to measure.
        {
            let schema = TsFileSchemaBuilder::new()
                .add(
                    "d1",
                    DeviceBuilder::new()
                        .add(
                            "s",
                            TSDataType::INT64,
                            TSEncoding::PLAIN,
                            CompressionType::UNCOMPRESSED,
                        )
                        .build(),
                )
                .build();

            let mut writer = TsFileWriter::new("benchmark.tsfile", schema, Default::default());

            for i in 0..10001 {
                writer.write("d1", "s", i, IoTDBValue::LONG(2 * i));
            }

            writer.close();
        }

        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);
    }
}
