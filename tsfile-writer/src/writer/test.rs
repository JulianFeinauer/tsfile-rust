// // enum IoTDBValue {
// //     DOUBLE(f64),
// //     FLOAT(f32),
// //     INT(i32),
// // }
//
// struct Chunk<T> {
//     raw_value: Option<T>,
// }
//
// trait Chunkeable {
//     fn set_raw(&mut self, value: IoTDBValue) -> Result<(), ()>;
// }
//
// impl Chunkeable for Chunk<i32> {
//     fn set_raw(&mut self, value: IoTDBValue) -> Result<(), ()> {
//         return match value {
//             IoTDBValue::INT(inner) => {
//                 self.raw_value = Some(inner);
//                 Ok(())
//             }
//             _ => Err(()),
//         };
//     }
// }
//
// impl Chunkeable for Chunk<f32> {
//     fn set_raw(&mut self, value: IoTDBValue) -> Result<(), ()> {
//         return match value {
//             IoTDBValue::FLOAT(inner) => {
//                 self.raw_value = Some(inner);
//                 Ok(())
//             }
//             _ => Err(()),
//         };
//     }
// }
//
// impl Chunkeable for Chunk<f64> {
//     fn set_raw(&mut self, value: IoTDBValue) -> Result<(), ()> {
//         return match value {
//             IoTDBValue::DOUBLE(inner) => {
//                 self.raw_value = Some(inner);
//                 Ok(())
//             }
//             _ => Err(()),
//         };
//     }
// }
//
// impl<T> Chunk<T> {
//     fn new() -> Chunk<T> {
//         return Chunk { raw_value: None };
//     }
// }
//
// struct ChunkGroup {
//     chunks: Vec<Box<dyn Chunkeable>>,
// }

#[cfg(test)]
mod test {
    use std::env::current_dir;

    use crate::writer::test_utils::{validate_output, write_ts_file};
    use crate::writer::{CompressionType, IoTDBValue, Schema, TSDataType, TSEncoding};

    // Can not run currently, as it needs the ts file analyzer tool
    #[test]
    #[ignore]
    fn flush_chunk() {
        let schema = Schema::simple(
            "d1",
            "s",
            TSDataType::INT64,
            TSEncoding::PLAIN,
            CompressionType::UNCOMPRESSED,
        );

        write_ts_file("target/benchmark.tsfile", schema, |writer| {
            for i in 0..30000001 {
                writer.write("d1", "s", i, IoTDBValue::LONG(i));
            }
        });

        // Analyse the file
        let expected_structure = "---------------------
Starting Printing the TsFile Sketch
---------------------
Sketch save path:TsFile_sketch_view.txt
-------------------------------- TsFile Sketch --------------------------------
file length: 245835939

            POSITION|	CONTENT
            -------- 	-------
                   0|	[magic head] TsFile
                   6|	[version number] 3
|||||||||||||||||||||	[Chunk Group] of d1, num of Chunks:1
                   7|	[Chunk Group Header]
                    |		[marker] 0
                    |		[deviceID] d1
                  11|	[Chunk] of s, numOfPoints:16378918, time range:[0,16378917], tsDataType:INT64,
                     	startTime: 0 endTime: 16378917 count: 16378918 [minValue:0,maxValue:16378917,firstValue:0,lastValue:16378917,sumValue:1.34134469235903E14]
                    |		[chunk header] marker=1, measurementId=s, dataSize=134217396, serializedSize=10
                    |		[chunk] java.nio.HeapByteBuffer[pos=0 lim=134217396 cap=134217396]
                    |		[page]  CompressedSize:65402, UncompressedSize:65402
|||||||||||||||||||||	[Chunk Group] of d1 ends
|||||||||||||||||||||	[Chunk Group] of d1, num of Chunks:1
           134217417|	[Chunk Group Header]
                    |		[marker] 0
                    |		[deviceID] d1
           134217421|	[Chunk] of s, numOfPoints:13621083, time range:[16378918,30000000], tsDataType:INT64,
                     	startTime: 16378918 endTime: 30000000 count: 13621083 [minValue:16378918,maxValue:30000000,firstValue:16378918,lastValue:30000000,sumValue:3.15865545764097E14]
                    |		[chunk header] marker=1, measurementId=s, dataSize=111618210, serializedSize=10
                    |		[chunk] java.nio.HeapByteBuffer[pos=0 lim=111618210 cap=111618210]
                    |		[page]  CompressedSize:65402, UncompressedSize:65402
|||||||||||||||||||||	[Chunk Group] of d1 ends
           245835641|	[marker] 2
           245835642|	[TimeseriesIndex] of d1.s, tsDataType:INT64
                    |		[ChunkIndex] s, offset=11
                    |		[ChunkIndex] s, offset=134217421
                    |		[startTime: 0 endTime: 30000000 count: 30000001 [minValue:0,maxValue:30000000,firstValue:0,lastValue:30000000,sumValue:4.50000015E14]]
|||||||||||||||||||||
           245835844|	[IndexOfTimerseriesIndex Node] type=LEAF_MEASUREMENT
                    |		<s, 245835642>
                    |		<endOffset, 245835844>
           245835864|	[TsFileMetadata]
                    |		[meta offset] 245835641
                    |		[num of devices] 1
                    |		1 key&TsMetadataIndex
                    |		[bloom filter bit vector byte array length] 32
                    |		[bloom filter bit vector byte array]
                    |		[bloom filter number of bits] 256
                    |		[bloom filter number of hash functions] 5
           245835929|	[TsFileMetadataSize] 65
           245835933|	[magic tail] TsFile
           245835939|	END of TsFile
---------------------------- IndexOfTimerseriesIndex Tree -----------------------------
	[MetadataIndex:LEAF_DEVICE]
	└──────[d1,245835844]
			[MetadataIndex:LEAF_MEASUREMENT]
			└──────[s,245835642]
---------------------------------- TsFile Sketch End ----------------------------------";
        validate_output(
            current_dir()
                .unwrap()
                .join("target/benchmark.tsfile")
                .as_path()
                .to_str()
                .unwrap(),
            expected_structure,
        );
    }
}
