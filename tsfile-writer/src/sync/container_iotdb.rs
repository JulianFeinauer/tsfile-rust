use std::env::current_dir;
use testcontainers::core::WaitFor;
use testcontainers::Image;

pub(crate) const SYNC_SERVER_PORT: u16 = 5555;
pub(crate) const SERVER_PORT: u16 = 6667;

const NAME: &str = "apache/iotdb";
const TAG: &str = "0.13.0-node";

#[derive(Default, Debug)]
pub struct IoTDB {
    val_key: String,
    val_val: String
}

const VOL_KEY: &str = "";
const VOL_VAL: &str = "";

impl IoTDB {
    pub(crate) fn new() -> IoTDB {
        let dir = current_dir().expect("").join("src/sync/iotdb-engine.properties");

        println!("Directory: {}", dir.as_path().to_str().expect(""));
        IoTDB {
            val_key: dir.as_path().to_str().expect("").to_string(),
            val_val: "/iotdb/conf/iotdb-engine.properties".to_string()
        }
    }
}

impl Image for IoTDB {
    type Args = ();

    fn name(&self) -> String {
        NAME.to_owned()
    }

    fn tag(&self) -> String {
        TAG.to_owned()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("IoTDB has started.")]
    }

    fn volumes(&self) -> Box<dyn Iterator<Item=(&String, &String)> + '_> {
        Box::new(vec![(&self.val_key, &self.val_val)].into_iter())
    }
}
