# Rust TsFile Writer

This is a not yet feature complete Writer for TsFiles Version 3 (as defined from the Apache IoTDB Project).
Currently not all features of TsFiles are supported.
Most notably:

* No Aligned Chunks can be written
* Not all Encodings are available
* Not all DataTypes are supported
* Not all Compression Types are supported

But generally, the TsFiles written with this client are 100% compatible with TsFiles written in Java.
