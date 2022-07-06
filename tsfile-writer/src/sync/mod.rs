//! Simple implementation of a Sync Sender for the IoTDB Protocol
//! This tool can send tsfiles to an IoTDB server in exactly the same way
//! that an IoTDB server would send a server if running the `sync-client.sh` script.
//!
//! Note: The receiving IoTDB Server must be started with the receiver being activated in the
//! configuration
#[cfg(test)]
mod container_iotdb;
mod mlog;
#[allow(clippy::module_inception)] // is not exported
mod sync;
pub mod sync_sender;
mod test;
