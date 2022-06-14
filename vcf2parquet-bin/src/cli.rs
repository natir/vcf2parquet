//! cli of vcf2parquet-bin

/* std use */

/* crate use */

/* project use */

#[derive(Debug, clap::ArgEnum, Clone, Copy)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    // Zstd,
}

#[derive(clap::Parser, std::fmt::Debug)]
pub struct Command {
    /// Input path
    #[clap(short = 'i', long = "input")]
    input: std::path::PathBuf,

    /// Output path
    #[clap(short = 'o', long = "output")]
    output: std::path::PathBuf,

    /// Batch size (default 100,000)
    #[clap(short = 'b', long = "batch-size")]
    batch_size: Option<usize>,

    /// Compression method (default snappy)
    #[clap(arg_enum, short = 'c', long = "compression")]
    compression: Option<Compression>,
}

impl Command {
    pub fn input(&self) -> &std::path::PathBuf {
        &self.input
    }

    pub fn output(&self) -> &std::path::PathBuf {
        &self.output
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size.unwrap_or(100_000)
    }

    pub fn compression(&self) -> arrow2::io::parquet::write::CompressionOptions {
        match self.compression {
            Some(Compression::Uncompressed) => {
                arrow2::io::parquet::write::CompressionOptions::Uncompressed
            }
            Some(Compression::Snappy) => arrow2::io::parquet::write::CompressionOptions::Snappy,
            Some(Compression::Gzip) => arrow2::io::parquet::write::CompressionOptions::Gzip,
            Some(Compression::Lzo) => arrow2::io::parquet::write::CompressionOptions::Lzo,
            Some(Compression::Brotli) => arrow2::io::parquet::write::CompressionOptions::Brotli,
            Some(Compression::Lz4) => arrow2::io::parquet::write::CompressionOptions::Lz4,
            // Some(Compression::Zstd) => arrow2::io::parquet::write::CompressionOptions::Zstd,
            None => arrow2::io::parquet::write::CompressionOptions::Snappy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_value() {
        let mut params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            output: std::path::Path::new("test/output.parquet").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Snappy),
        };

        assert_eq!(
            params.input(),
            &std::path::Path::new("test/input.vcf").to_path_buf()
        );

        assert_eq!(
            params.output(),
            &std::path::Path::new("test/output.parquet").to_path_buf()
        );

        assert_eq!(params.batch_size(), 100_000);

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            output: std::path::Path::new("test/output.parquet").to_path_buf(),
            batch_size: Some(100),
            compression: Some(Compression::Snappy),
        };

        assert_eq!(params.batch_size(), 100);
    }

    #[test]
    fn compression() {
        let mut params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            output: std::path::Path::new("test/output.parquet").to_path_buf(),
            batch_size: None,
            compression: None,
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Snappy
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            output: std::path::Path::new("test/output.parquet").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Uncompressed),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Uncompressed
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            output: std::path::Path::new("test/output.parquet").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Snappy),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Snappy
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            output: std::path::Path::new("test/output.parquet").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Gzip),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Gzip
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            output: std::path::Path::new("test/output.parquet").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Lzo),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Lzo
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            output: std::path::Path::new("test/output.parquet").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Brotli),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Brotli
        );

        params = Command {
            input: std::path::Path::new("test/input.vcf").to_path_buf(),
            output: std::path::Path::new("test/output.parquet").to_path_buf(),
            batch_size: None,
            compression: Some(Compression::Lz4),
        };

        assert_eq!(
            params.compression(),
            arrow2::io::parquet::write::CompressionOptions::Lz4
        );
    }
}
