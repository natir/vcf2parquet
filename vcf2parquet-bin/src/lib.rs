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

    match params.subcommand() {
        cli::SubCommand::Convert(subparams) => convert(&params, subparams),
        cli::SubCommand::Split(subparams) => split(&params, subparams),
    }
}

fn convert(params: &cli::Command, subparams: &cli::Convert) -> error::Result<()> {
    let mut reader = std::fs::File::open(params.input())
        .map_err(error::mapping)
        .map(Box::new)
        .map(|x| niffler::get_reader(x))
        .map_err(error::mapping)?
        .map(|(file, _)| std::io::BufReader::with_capacity(params.read_buffer(), file))?;

    let mut output = std::fs::File::create(subparams.output()).map_err(error::mapping)?;

    lib::vcf2parquet(
        &mut reader,
        &mut output,
        params.batch_size(),
        params.compression(),
        params.info_optional(),
    )
    .map_err(error::mapping)?;

    Ok(())
}

fn split(params: &cli::Command, subparams: &cli::Split) -> error::Result<()> {
    let mut reader = std::fs::File::open(params.input())
        .map_err(error::mapping)
        .map(Box::new)
        .map(|x| niffler::get_reader(x))
        .map_err(error::mapping)?
        .map(|(file, _)| std::io::BufReader::with_capacity(params.read_buffer(), file))?;

    lib::vcf2multiparquet(
        &mut reader,
        subparams.format(),
        params.batch_size(),
        params.compression(),
        params.info_optional(),
    )
    .map_err(error::mapping)?;

    Ok(())
}
