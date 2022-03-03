//! vcf2parquet error

/* std use */

/* crate use */

/* project use */

#[derive(thiserror::Error, std::fmt::Debug)]
pub enum Error {
    /// Not support type conversion
    #[error("Conversion of arrow type in noodles type isn't support.")]
    NoConversion,

    /// Arrow error
    #[error(transparent)]
    ArrowError { error: arrow2::error::ArrowError },

    /// Parquet error
    #[error(transparent)]
    ParquetError {
        error: parquet2::error::ParquetError,
    },

    /// Io error
    #[error(transparent)]
    IoError { error: std::io::Error },

    /// Noodles header vcf error
    #[error(transparent)]
    NoodlesHeaderError {
        error: noodles::vcf::header::ParseError,
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

impl From<arrow2::error::ArrowError> for Error {
    fn from(error: arrow2::error::ArrowError) -> Self {
        Error::ArrowError { error }
    }
}

impl From<parquet2::error::ParquetError> for Error {
    fn from(error: parquet2::error::ParquetError) -> Self {
        Error::ParquetError { error }
    }
}

impl From<noodles::vcf::header::ParseError> for Error {
    fn from(error: noodles::vcf::header::ParseError) -> Self {
        Error::NoodlesHeaderError { error }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
