# rust-tsfile

This is a simple Implementation to write simple tsfiles in rust.

It also contains a (draft) sync module to sync the written tsfiles with an IoTDB server.

## Content

The workspace contains 4 different crates:

* tsfile-writer - the main crate, published on crates.io: https://crates.io/crates/tsfile-writer
* tsfile-writer-c - a C lib wrapper around the tsfile-writer module (experimental)
* examples - some examples how to use the lib
* sync-sender - A rust implementation for an iotdb-server compatible sync-sender (WIP)
