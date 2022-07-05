use crate::writer::errors::TsFileError;
use crate::writer::tsfile_writer::TsFileWriter;
use crate::writer::{Schema, WriteWrapper};
use std::fs::File;
use std::process::Command;

const PATH_TO_TSFILE_TOOL: &str = "/Users/julian/Downloads/apache-iotdb-0.13.0-all-bin/tools/tsfileToolSet/print-tsfile-sketch.sh";

pub fn validate_output(filename: &str, expected_structure: &str) {
    let output = Command::new(PATH_TO_TSFILE_TOOL)
        .arg(filename)
        .output()
        .expect("Failed to execute command");

    let structure = String::from_utf8(output.stdout).unwrap();
    let real = structure
        .lines()
        .filter(|line| {
            !line.contains("[main]") && !line.contains("TsFile path") && !line.contains("file path")
        })
        .map(|line| line.trim_end())
        .collect::<Vec<&str>>()
        .join("\n");
    assert_eq!(expected_structure, real)
}

pub fn write_ts_file<'a, F: FnOnce(&mut TsFileWriter<WriteWrapper<File>>)>(
    filename: &'a str,
    schema: Schema<'a>,
    test_code: F,
) -> Result<(), TsFileError> {
    let mut writer = TsFileWriter::new(filename, schema, Default::default())?;

    // Execute the test
    test_code(&mut writer);

    // writer.flush();
    writer.close();

    Ok(())
}
