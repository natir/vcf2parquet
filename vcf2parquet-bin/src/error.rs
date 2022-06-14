//! error of vcf2parquet-bin

/* std use */

/* crate use */

/* project use */

#[derive(thiserror::Error, std::fmt::Debug)]
pub enum Error {
    /// Io error
    #[error(transparent)]
    Io { error: std::io::Error },

    /// Niffler error
    #[error(transparent)]
    Niffler { error: niffler::Error },

    /// vcf2parquet-lib error
    #[error(transparent)]
    Lib {
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
        Error::Io { error }
    }
}

impl From<niffler::Error> for Error {
    fn from(error: niffler::Error) -> Self {
        Error::Niffler { error }
    }
}

impl From<vcf2parquet_lib::error::Error> for Error {
    fn from(error: vcf2parquet_lib::error::Error) -> Self {
        Error::Lib { error }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_conversion() {
        assert_eq!(
            format!(
                "{:?}",
                Error::from(std::io::Error::new(std::io::ErrorKind::NotFound, "test"))
            ),
            "Io { error: Custom { kind: NotFound, error: \"test\" } }".to_string()
        );

        assert_eq!(
            format!("{:?}", Error::from(niffler::Error::FileTooShort)),
            "Niffler { error: FileTooShort }".to_string()
        );

        assert_eq!(
            format!(
                "{:?}",
                Error::from(vcf2parquet_lib::error::Error::NoConversion)
            ),
            "Lib { error: NoConversion }".to_string()
        );
    }
}
