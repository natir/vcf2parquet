//! vcf2parquet error

/* std use */

/* crate use */

/* project use */

#[non_exhaustive]
#[derive(thiserror::Error, std::fmt::Debug)]
pub enum Error {
    /// Not support type conversion
    #[error("Conversion of arrow type in noodles type isn't supported.")]
    NoConversion,

    /// Arrow error
    #[error(transparent)]
    Arrow(#[from] arrow2::error::Error),

    /// Parquet error
    #[error(transparent)]
    Parquet(#[from] arrow2::io::parquet::read::ParquetError),

    /// Io error
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Noodles header vcf error
    #[error(transparent)]
    NoodlesHeader(#[from] noodles::vcf::header::ParseError),
}

pub type Result<T> = std::result::Result<T, Error>;
