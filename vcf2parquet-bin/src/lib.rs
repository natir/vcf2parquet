//! vcf2parquet bin

/* std use */

/* crate use */
use clap::Parser as _;

/* project use */
use vcf2parquet_lib as lib;

/* mod section */
pub mod cli;
pub mod error;

pub fn main() -> error::Result<()> {
    let params = cli::Command::parse();

    let mut reader = std::fs::File::open(params.input())
        .map_err(error::mapping)
        .map(Box::new)
        .map(|x| niffler::get_reader(x))
        .map_err(error::mapping)?
        .map(|(file, _)| std::io::BufReader::new(file))?;

    let mut output = std::fs::File::create(params.output()).map_err(error::mapping)?;

    lib::vcf2parquet(
        &mut reader,
        &mut output,
        params.batch_size(),
        params.compression(),
    )
    .map_err(error::mapping)?;

    Ok(())
}
