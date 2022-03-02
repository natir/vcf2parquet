//! error of vcf2parquet-bin

/* std use */

/* crate use */

/* project use */

#[derive(thiserror::Error, std::fmt::Debug)]
pub enum Error {
    /// Io error
    #[error(transparent)]
    IoError { error: std::io::Error },

    /// Niffler error
    #[error(transparent)]
    NifflerError { error: niffler::Error },

    /// vcf2parquet-lib error
    #[error(transparent)]
    LibError {
        error: vcf2parquet_lib::error::Error,
    },
}

pub fn mapping<E>(error: E) -> Error
where
    E: std::convert::Into<Error>,
{
    error.into()
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IoError { error }
    }
}

impl From<niffler::Error> for Error {
    fn from(error: niffler::Error) -> Self {
        Error::NifflerError { error }
    }
}

impl From<vcf2parquet_lib::error::Error> for Error {
    fn from(error: vcf2parquet_lib::error::Error) -> Self {
        Error::LibError { error }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
