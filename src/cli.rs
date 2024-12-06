//! cli of vcf2parquet-bin

/* std use */

/* crate use */
use parquet::file::properties::WriterVersion;

/* project use */

/// Parquet version available for user
#[derive(Debug, clap::ValueEnum, Clone, Copy)]
pub enum ParquetVersion {
    ///Parquet version 1
    V1,
    ///Parquet version 2
    V2,
}

/// Compression available for user
#[derive(Debug, clap::ValueEnum, Clone, Copy)]
pub enum Compression {
    /// No compression
    Uncompressed,

    /// Snappy compression
    Snappy,

    /// Gzip compression
    Gzip,

    /// Lzo compression
    Lzo,

    /// Brotly compression
    Brotli,

    /// Lz4 compression
    Lz4,

    /// Zstd compression
    Zstd,
}

/// Define cli of vcf2parquet
#[derive(clap::Parser, std::fmt::Debug)]
#[command(
    name = "vcf2parquet",
    author = "Pierre Marijon <pierre.marijon-ext@aphp.fr>",
    version = "0.4.1",
    about = "Convert a vcf in parquet"
)]
pub struct Command {
    /// Input path
    #[clap(short = 'i', long = "input")]
    input: std::path::PathBuf,

    /// Batch size (default 100,000)
    #[clap(short = 'b', long = "batch-size")]
    batch_size: Option<usize>,

    /// Compression method (default snappy)
    #[clap(value_enum, short = 'c', long = "compression")]
    compression: Option<Compression>,

    /// Read buffer size in bytes (default 8192)
    #[clap(short = 'r', long = "read-buffer")]
    read_buffer: Option<usize>,

    /// All information fields are optional
    #[clap(short = 'I', long = "info-optional")]
    info_optional: bool,

    /// Select version of parquet version default v2
    #[clap(long = "parquet-version")]
    parquet_version: Option<ParquetVersion>,

    #[clap(subcommand)]
    subcommand: SubCommand,
}

/// Enum to manage sub command
#[derive(clap::Parser, std::fmt::Debug, Clone)]
pub enum SubCommand {
    /// Convert a vcf in a parquet
    Convert(Convert),

    /// Convert a vcf in multiple parquet file each file contains `batch_size` record
    Split(Split),
}

/// Convert a vcf in a parquet
#[derive(clap::Parser, std::fmt::Debug, Clone)]
pub struct Convert {
    /// Output path
    #[clap(short = 'o', long = "output")]
    output: std::path::PathBuf,
}

/// Convert a vcf in multiple parquet file each file contains `batch_size` record
#[derive(clap::Parser, std::fmt::Debug, Clone)]
pub struct Split {
    /// Output format string, first {} are replace by number
    #[clap(short = 'f', long = "output-format")]
    format: String,
}

impl Command {
    /// Get input
    pub fn input(&self) -> &std::path::PathBuf {
        &self.input
    }

    /// Get batch_size set by user or default value
    pub fn batch_size(&self) -> usize {
        self.batch_size.unwrap_or(100_000)
    }

    /// Get compression set by user or default value
    pub fn compression(&self) -> parquet::basic::Compression {
        match self.compression {
            Some(Compression::Uncompressed) => parquet::basic::Compression::UNCOMPRESSED,
            Some(Compression::Snappy) => parquet::basic::Compression::SNAPPY,
            Some(Compression::Gzip) => {
                parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default())
            }
            Some(Compression::Lzo) => parquet::basic::Compression::LZO,
            Some(Compression::Brotli) => {
                parquet::basic::Compression::BROTLI(parquet::basic::BrotliLevel::default())
            }
            Some(Compression::Lz4) => parquet::basic::Compression::LZ4,
            Some(Compression::Zstd) => {
                parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default())
            }
            None => parquet::basic::Compression::SNAPPY,
        }
    }

    /// Get parquet version
    pub fn parquet_version(&self) -> WriterVersion {
        match self.parquet_version {
            Some(ParquetVersion::V1) => WriterVersion::PARQUET_1_0,
            Some(ParquetVersion::V2) => WriterVersion::PARQUET_2_0,
            None => WriterVersion::PARQUET_2_0,
        }
    }

    /// Get read buffer size
    pub fn read_buffer(&self) -> usize {
        self.read_buffer.unwrap_or(8192)
    }

    /// Get info optional
    pub fn info_optional(&self) -> bool {
        self.info_optional
    }

    /// Get subcommand
    pub fn subcommand(&self) -> &SubCommand {
        &self.subcommand
    }
}

impl Convert {
    /// Get output
    pub fn output(&self) -> &std::path::PathBuf {
        &self.output
    }
}

impl Split {
    /// Get output format
    pub fn format(&self) -> &str {
        &self.format
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn basic_value() {
        let mut params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Snappy),
            read_buffer: None,
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
            info_optional: false,
            parquet_version: None,
        };

        assert_eq!(
            params.input(),
            &std::path::Path::new("test/input.vcf").to_path_buf()
        );

        match params.subcommand.clone() {
            SubCommand::Convert(c) => assert_eq!(
                c.output(),
                &std::path::Path::new("test/output.parquet").to_path_buf()
            ),
            _ => unreachable!(),
        }

        assert_eq!(params.batch_size(), 100_000);
        assert_eq!(params.read_buffer(), 8192);

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: Some(100),
            compression: Some(Compression::Snappy),
            read_buffer: Some(8194),
            subcommand: SubCommand::Split(Split {
                format: "test_{}.parquet".to_string(),
            }),
            info_optional: false,
            parquet_version: Some(ParquetVersion::V1),
        };

        assert_eq!(params.batch_size(), 100);
        assert_eq!(params.read_buffer(), 8194);
        assert_eq!(params.parquet_version(), WriterVersion::PARQUET_1_0);

        match params.subcommand.clone() {
            SubCommand::Split(s) => assert_eq!(s.format(), "test_{}.parquet"),
            _ => unreachable!(),
        }
    }

    #[test]
    fn compression() {
        let mut params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: None,
            read_buffer: None,
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
            info_optional: false,
            parquet_version: None,
        };

        assert_eq!(params.compression(), parquet::basic::Compression::SNAPPY);
        assert_eq!(params.parquet_version(), WriterVersion::PARQUET_2_0);

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Uncompressed),
            read_buffer: None,
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
            info_optional: false,
            parquet_version: None,
        };

        assert_eq!(
            params.compression(),
            parquet::basic::Compression::UNCOMPRESSED
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Snappy),
            read_buffer: None,
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
            info_optional: false,
            parquet_version: None,
        };

        assert_eq!(params.compression(), parquet::basic::Compression::SNAPPY);

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Gzip),
            read_buffer: None,
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
            info_optional: false,
            parquet_version: None,
        };

        assert_eq!(
            params.compression(),
            parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default())
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Lzo),
            read_buffer: None,
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
            info_optional: false,
            parquet_version: None,
        };

        assert_eq!(params.compression(), parquet::basic::Compression::LZO);

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Brotli),
            read_buffer: None,
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
            info_optional: false,
            parquet_version: None,
        };

        assert_eq!(
            params.compression(),
            parquet::basic::Compression::BROTLI(parquet::basic::BrotliLevel::default())
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Lz4),
            read_buffer: None,
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
            info_optional: false,
            parquet_version: None,
        };

        assert_eq!(params.compression(), parquet::basic::Compression::LZ4);
    }
}
