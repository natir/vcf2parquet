//! vcf2parquet error

/* std use */

/* crate use */

/* project use */

#[derive(thiserror::Error, std::fmt::Debug)]
pub enum Error {
    /// Not support type conversion
    #[error("Conversion of arrow type in noodles type isn't support.")]
    NoConversion,
}

pub type Result<T> = std::result::Result<T, Error>;
