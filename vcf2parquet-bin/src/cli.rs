//! cli of vcf2parquet-bin

/* std use */

/* crate use */

/* project use */

#[derive(clap::Parser, std::fmt::Debug)]
pub struct Command {
    /// Input path
    #[clap(short = 'i', long = "input")]
    input: std::path::PathBuf,

    /// Output path
    #[clap(short = 'o', long = "output")]
    output: std::path::PathBuf,

    /// Batch size (default 10,000)
    #[clap(short = 'b', long = "batch-size")]
    batch_size: Option<usize>,
}

impl Command {
    pub fn input(&self) -> &std::path::PathBuf {
        &self.input
    }

    pub fn output(&self) -> &std::path::PathBuf {
        &self.output
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size.unwrap_or(100)
    }
}
