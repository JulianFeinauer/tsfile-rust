# Rust implementation of a Sync Server

This is a very simple implementation of a "Sync-Client" for the Apache IoTDB Server.
This means, this tool behaves like an IoTDB Server with `start-sync-client.sh` running.
I.e. it will send tsfiles to the respective reveiving server using Apache IoTDBs Sync Protocol.
