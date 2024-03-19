//! vcf2parquet bin

/* std use */

/* crate use */
use clap::Parser as _;

/* project use */
use vcf2parquet::cli;
use vcf2parquet::error;

/* mod section */

pub fn main() -> error::Result<()> {
    let params = cli::Command::parse();

    match params.subcommand() {
        cli::SubCommand::Convert(subparams) => convert(&params, subparams),
        cli::SubCommand::Split(subparams) => split(&params, subparams),
    }
}

fn convert(params: &cli::Command, subparams: &cli::Convert) -> error::Result<()> {
    let mut reader = std::fs::File::open(params.input())
        .map(Box::new)
        .map(|x| niffler::get_reader(x))?
        .map(|(file, _)| std::io::BufReader::with_capacity(params.read_buffer(), file))?;

    let mut output = std::fs::File::create(subparams.output())?;

    vcf2parquet::vcf2parquet(
        &mut reader,
        &mut output,
        params.batch_size(),
        params.compression(),
        params.info_optional(),
    )?;

    Ok(())
}

fn split(params: &cli::Command, subparams: &cli::Split) -> error::Result<()> {
    let mut reader = std::fs::File::open(params.input())
        .map(Box::new)
        .map(|x| niffler::get_reader(x))?
        .map(|(file, _)| std::io::BufReader::with_capacity(params.read_buffer(), file))?;

    vcf2parquet::vcf2multiparquet(
        &mut reader,
        subparams.format(),
        params.batch_size(),
        params.compression(),
        params.info_optional(),
    )?;

    Ok(())
}
