use std::io::Error;

#[derive(Debug)]
pub enum TsFileError {
    Error { source: Option<String> }, // Generic Error
    IoError { source: std::io::Error },
    WriteError,
    OutOfOrderData,
    IllegalState { source: Option<String> },
}

impl PartialEq for TsFileError {
    fn eq(&self, other: &Self) -> bool {
        match self {
            TsFileError::Error { source: a } => match other {
                TsFileError::Error { source: b } => a == b,
                _ => false,
            },
            TsFileError::IoError { .. } => false,
            TsFileError::WriteError => match other {
                TsFileError::WriteError => true,
                _ => false,
            },
            TsFileError::OutOfOrderData => match other {
                TsFileError::OutOfOrderData => true,
                _ => false,
            },
            TsFileError::IllegalState { source: a } => match other {
                TsFileError::IllegalState { source: b } => a == b,
                _ => false,
            },
        }
    }
}

impl<'a> From<std::io::Error> for TsFileError {
    fn from(e: Error) -> Self {
        TsFileError::IoError { source: e }
    }
}
