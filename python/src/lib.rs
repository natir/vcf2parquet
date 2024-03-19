//! vcf2parquet python binding

/* std use */
use vcf2parquet_lib as lib;

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

#[pyfunction]
#[pyo3(signature = (input,output,read_buffer=8192,batch_size=100_000,compression=Compression::Snappy,info_optional=false))]
fn convert_vcf(
    input: std::path::PathBuf,
    output: std::path::PathBuf,
    read_buffer: usize,
    batch_size: usize,
    compression: Compression,
    info_optional: bool,
) -> PyResult<()> {
    let mut reader = std::fs::File::open(input)
        .map(Box::new)
        .map(|x| niffler::get_reader(x))?
        .map(|(file, _)| std::io::BufReader::with_capacity(read_buffer, file))
        .map_err(PyVcf2ParquetErr::Niffle)?;

    let mut output = std::fs::File::create(output)?;

    let compression = match compression {
        Compression::Uncompressed => arrow2::io::parquet::write::CompressionOptions::Uncompressed,
        Compression::Snappy => arrow2::io::parquet::write::CompressionOptions::Snappy,
        Compression::Gzip => arrow2::io::parquet::write::CompressionOptions::Gzip(None),
        Compression::Lzo => arrow2::io::parquet::write::CompressionOptions::Lzo,
        Compression::Brotli => arrow2::io::parquet::write::CompressionOptions::Brotli(None),
        Compression::Lz4 => arrow2::io::parquet::write::CompressionOptions::Lz4,
        Compression::Zstd => arrow2::io::parquet::write::CompressionOptions::Zstd(None),
    };

    lib::vcf2parquet(
        &mut reader,
        &mut output,
        batch_size,
        compression,
        info_optional,
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
