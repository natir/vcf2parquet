//! vcf2parquet error

/* std use */

/* crate use */

/* project use */

#[non_exhaustive]
#[derive(thiserror::Error, std::fmt::Debug)]
pub enum Error {
    /// Not support type conversion
    #[error("Conversion of arrow type in noodles type isn't support.")]
    NoConversion,

    /// Arrow error
    #[error(transparent)]
    Arrow { error: arrow2::error::Error },

    /// Parquet error
    #[error(transparent)]
    Parquet {
        error: arrow2::io::parquet::read::ParquetError,
    },

    /// Io error
    #[error(transparent)]
    Io { error: std::io::Error },

    /// Noodles header vcf error
    #[error(transparent)]
    NoodlesHeader {
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
        Error::Io { error }
    }
}

impl From<arrow2::error::Error> for Error {
    fn from(error: arrow2::error::Error) -> Self {
        Error::Arrow { error }
    }
}

impl From<arrow2::io::parquet::read::ParquetError> for Error {
    fn from(error: arrow2::io::parquet::read::ParquetError) -> Self {
        Error::Parquet { error }
    }
}

impl From<noodles::vcf::header::ParseError> for Error {
    fn from(error: noodles::vcf::header::ParseError) -> Self {
        Error::NoodlesHeader { error }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_conversion() {
        let error: std::io::Error = std::io::Error::new(std::io::ErrorKind::NotFound, "test");
        assert_eq!(
            format!("{:?}", mapping(error)),
            format!(
                "{:?}",
                Error::from(std::io::Error::new(std::io::ErrorKind::NotFound, "test"))
            )
        );

        assert_eq!(
            format!(
                "{:?}",
                Error::from(std::io::Error::new(std::io::ErrorKind::NotFound, "test"))
            ),
            "Io { error: Custom { kind: NotFound, error: \"test\" } }".to_string()
        );

        assert_eq!(
            format!(
                "{:?}",
                Error::from(arrow2::error::Error::NotYetImplemented("test".to_string()))
            ),
            "Arrow { error: NotYetImplemented(\"test\") }".to_string()
        );

        assert_eq!(
            format!(
                "{:?}",
                Error::from(arrow2::io::parquet::read::ParquetError::OutOfSpec(
                    "test".to_string()
                ))
            ),
            "Parquet { error: OutOfSpec(\"test\") }".to_string()
        );

        assert_eq!(
            format!(
                "{:?}",
                Error::from(noodles::vcf::header::ParseError::MissingHeader)
            ),
            "NoodlesHeader { error: MissingHeader }".to_string()
        );
    }
}
