//! vcf2parquet python binding

/* std use */
use vcf2parquet as lib;

/* crate use */
use pyo3::prelude::*;

/* mod decalaration */
mod error;

/* projet use */
use error::PyVcf2ParquetErr;

#[pyclass]
#[derive(Debug, Clone, Copy)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
}

#[pyclass]
#[derive(Debug, Clone, Copy)]
pub enum ParquetVersion {
    V2_0,
    V1_0,
}

#[pyfunction]
#[pyo3(signature = (input,output,read_buffer=8192,batch_size=100_000,compression=Compression::Snappy,info_optional=false,parquet_version=ParquetVersion::V2_0))]
fn convert_vcf(
    input: std::path::PathBuf,
    output: std::path::PathBuf,
    read_buffer: usize,
    batch_size: usize,
    compression: Compression,
    info_optional: bool,
    parquet_version: ParquetVersion,
) -> PyResult<()> {
    let mut reader = std::fs::File::open(input)
        .map(Box::new)
        .map(|x| niffler::get_reader(x))?
        .map(|(file, _)| std::io::BufReader::with_capacity(read_buffer, file))
        .map_err(PyVcf2ParquetErr::Niffle)?;

    let mut output = std::fs::File::create(output)?;

    let compression = match compression {
        Compression::Uncompressed => parquet::basic::Compression::UNCOMPRESSED,
        Compression::Snappy => parquet::basic::Compression::SNAPPY,
        Compression::Gzip => {
            parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default())
        }
        Compression::Lzo => parquet::basic::Compression::LZO,
        Compression::Brotli => {
            parquet::basic::Compression::BROTLI(parquet::basic::BrotliLevel::default())
        }
        Compression::Lz4 => parquet::basic::Compression::LZ4,
        Compression::Zstd => {
            parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default())
        }
    };

    let parquet_version = match parquet_version {
        ParquetVersion::V2_0 => parquet::file::properties::WriterVersion::PARQUET_2_0,
        ParquetVersion::V1_0 => parquet::file::properties::WriterVersion::PARQUET_1_0,
    };

    lib::vcf2parquet(
        &mut reader,
        &mut output,
        batch_size,
        compression,
        info_optional,
        parquet_version,
    )
    .map_err(PyVcf2ParquetErr::from)
    .map_err(PyErr::from)
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn pyvcf2parquet(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert_vcf, m)?)?;
    m.add_class::<Compression>()?;
    Ok(())
}
