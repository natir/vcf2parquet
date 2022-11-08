//! cli of vcf2parquet-bin

/* std use */

/* crate use */

/* project use */

#[derive(Debug, clap::ValueEnum, Clone, Copy)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
}

#[derive(clap::Parser, std::fmt::Debug)]
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

    #[clap(subcommand)]
    subcommand: SubCommand,
}

#[derive(clap::Parser, std::fmt::Debug, Clone)]
pub enum SubCommand {
    Convert(Convert),
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
    pub fn compression(&self) -> arrow2::io::parquet::write::CompressionOptions {
        match self.compression {
            Some(Compression::Uncompressed) => {
                arrow2::io::parquet::write::CompressionOptions::Uncompressed
            }
            Some(Compression::Snappy) => arrow2::io::parquet::write::CompressionOptions::Snappy,
            Some(Compression::Gzip) => arrow2::io::parquet::write::CompressionOptions::Gzip(None),
            Some(Compression::Lzo) => arrow2::io::parquet::write::CompressionOptions::Lzo,
            Some(Compression::Brotli) => {
                arrow2::io::parquet::write::CompressionOptions::Brotli(None)
            }
            Some(Compression::Lz4) => arrow2::io::parquet::write::CompressionOptions::Lz4,
            Some(Compression::Zstd) => arrow2::io::parquet::write::CompressionOptions::Zstd(None),
            None => arrow2::io::parquet::write::CompressionOptions::Snappy,
        }
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
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
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

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: Some(100),
            compression: Some(Compression::Snappy),
            subcommand: SubCommand::Split(Split {
                format: "test_{}.parquet".to_string(),
            }),
        };

        assert_eq!(params.batch_size(), 100);

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
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Snappy
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Uncompressed),
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Uncompressed
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Snappy),
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Snappy
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Gzip),
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Gzip
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Lzo),
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Lzo
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Brotli),
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Brotli
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Lz4),
            subcommand: SubCommand::Convert(Convert {
                output: std::path::Path::new("test/output.parquet").to_path_buf(),
            }),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Lz4
        );
    }
}
