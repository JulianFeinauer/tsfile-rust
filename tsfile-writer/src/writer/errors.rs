//! Default Errors for the TsFile crate
use std::io::Error;

#[derive(Debug)]
pub enum TsFileError {
    Error { source: Option<String> }, // Generic Error
    IoError { source: std::io::Error },
    WriteError,
    OutOfOrderData,
    IllegalState { source: Option<String> },
    Compression,
    WrongTypeForSeries,
    Encoding,
}

impl PartialEq for TsFileError {
    fn eq(&self, other: &Self) -> bool {
        match self {
            TsFileError::Error { source: a } => match other {
                TsFileError::Error { source: b } => a == b,
                _ => false,
            },
            TsFileError::IoError { .. } => false,
            TsFileError::WriteError => matches!(other, TsFileError::WriteError),
            TsFileError::OutOfOrderData => matches!(other, TsFileError::OutOfOrderData),
            TsFileError::IllegalState { source: a } => match other {
                TsFileError::IllegalState { source: b } => a == b,
                _ => false,
            },
            TsFileError::Compression => matches!(other, TsFileError::Compression),
            TsFileError::WrongTypeForSeries => matches!(other, TsFileError::WrongTypeForSeries),
            TsFileError::Encoding => matches!(other, TsFileError::Encoding),
        }
    }
}

impl From<std::io::Error> for TsFileError {
    fn from(e: Error) -> Self {
        TsFileError::IoError { source: e }
    }
}
