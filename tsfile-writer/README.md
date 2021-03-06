# Rust TsFile Writer

This is a not yet feature complete Writer for TsFiles Version 3 (as defined from the Apache IoTDB Project).
Currently not all features of TsFiles are supported.
Most notably:

* No Aligned Chunks can be written
* Not all Encodings are available
* Not all DataTypes are supported
* Not all Compression Types are supported

But generally, the TsFiles written with this client are 100% compatible with TsFiles written in Java.

## Quickstart

To write a TsFile just do something like

```
// Create the Schema
// Two devices with two sensors each
let schema = TsFileSchemaBuilder::new()
        .add(
            "d1",
            DeviceBuilder::new()
                .add(
                    "s1",
                    TSDataType::INT64,
                    TSEncoding::PLAIN,
                    CompressionType::UNCOMPRESSED,
                )
                .add(
                    "s2",
                    TSDataType::FLOAT,
                    TSEncoding::PLAIN,
                    CompressionType::UNCOMPRESSED,
                )
                .build(),
        )
        .add(
            "d2",
            DeviceBuilder::new()
                .add(
                    "s1",
                    TSDataType::INT64,
                    TSEncoding::PLAIN,
                    CompressionType::UNCOMPRESSED,
                )
                .add(
                    "s2",
                    TSDataType::FLOAT,
                    TSEncoding::PLAIN,
                    CompressionType::UNCOMPRESSED,
                )
                .build(),
        )
        .build();
        
// Create the writer
let mut writer = TsFileWriter::new(
    "target/benchmark2.tsfile",
    schema,
    Default::default(),
)
.unwrap();
        
// Write multiple timeseries at once
writer.write_many("d1",1, vec![
        DataPoint::new("s1", IoTDBValue::LONG(i)),
        DataPoint::new("s2", IoTDBValue::FLOAT(i as f32)),
]);
    
// Write single series
writer.write("d2", "s1", 1, IoTDBValue::LONG(i));
writer.write("d2", "s2", 1, IoTDBValue::FLOAT(i as f32));
```

## Currently implemented features

### Encodings

* [x] Plain
* [x] TS2Diff Encoding for INT32 and INT64
* [ ] everything else...

### Datatypes

* [x] INT32
* [x] INT64
* [x] FLOAT
* [ ] everything else...

### Compression

* [x] Uncompressed
* [x] SNAPPY
* [ ] everything else...

## Feature 'sync_sender'

This is a very simple implementation of a "Sync-Client" for the Apache IoTDB Server.
This means, this tool behaves like an IoTDB Server with `start-sync-client.sh` running.
I.e. it will send tsfiles to the respective reveiving server using Apache IoTDBs Sync Protocol.


## Changelog

### 0.2.1 (re-release due to wrong changelog)

- TsFileWriter::write_many now accepts `IntoIterator<Item=DataPoint<'a>>` as argument instead of only `Vec<DataPoint<'a>>`
- Added SyncSender (feature `sync_sender`)

### 0.1.3

- TS2Diff is now available for INT64 and INT32

### 0.1.2

- SNAPPY Compression is now available
- CI Pipeline and cargo fmt used in the codebase

### 0.1.1

- Lots of Refactoring
- Added `Result<_, TsFileError>` to basically every operation
- Improved Readme
