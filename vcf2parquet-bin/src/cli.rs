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
    Zstd,
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

    pub fn compression(&self) -> arrow2::io::parquet::write::Compression {
        match self.compression {
            Some(Compression::Uncompressed) => {
                arrow2::io::parquet::write::Compression::Uncompressed
            }
            Some(Compression::Snappy) => arrow2::io::parquet::write::Compression::Snappy,
            Some(Compression::Gzip) => arrow2::io::parquet::write::Compression::Gzip,
            Some(Compression::Lzo) => arrow2::io::parquet::write::Compression::Lzo,
            Some(Compression::Brotli) => arrow2::io::parquet::write::Compression::Brotli,
            Some(Compression::Lz4) => arrow2::io::parquet::write::Compression::Lz4,
            Some(Compression::Zstd) => arrow2::io::parquet::write::Compression::Zstd,
            None => arrow2::io::parquet::write::Compression::Snappy,
        }
    }
}
