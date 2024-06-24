//! Comstom error type.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("Data is corrupted: {0}")]
    DataError(String),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("Bitcask is immutable: {0}")]
    OptionError(String),
}
