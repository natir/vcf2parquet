//! Functional test on vcf2parquet

/* std use */

/* crate use */

/* project use */

use std::io::Read;

#[test]
fn help() -> Result<(), assert_cmd::cargo::CargoError> {
    let mut cmd = assert_cmd::Command::cargo_bin("vcf2parquet")?;

    cmd.args(["-h"]);

    let truth: &[u8] = b"Convert a vcf in parquet

Usage: vcf2parquet [OPTIONS] --input <INPUT> <COMMAND>

Commands:
  convert  Convert a vcf in a parquet
  split    Convert a vcf in multiple parquet file each file contains `batch_size` record
  help     Print this message or the help of the given subcommand(s)

Options:
  -i, --input <INPUT>              Input path
  -b, --batch-size <BATCH_SIZE>    Batch size (default 100,000)
  -c, --compression <COMPRESSION>  Compression method (default snappy) [possible values: uncompressed, snappy, gzip, lzo, brotli, lz4, zstd]
  -r, --read-buffer <READ_BUFFER>  Read buffer size in bytes (default 8192)
  -I, --info-optional              All information fields are optional
  -h, --help                       Print help
  -V, --version                    Print version
";

    let assert = cmd.assert();

    assert.success().stdout(truth);

    Ok(())
}

#[test]
fn convert() -> Result<(), assert_cmd::cargo::CargoError> {
    let mut cmd = assert_cmd::Command::cargo_bin("vcf2parquet")?;

    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path();
    let parquet_path = temp_path.join("tests.parquet");

    cmd.args([
        "-i",
        "tests/data/test.vcf",
        "convert",
        "-o",
        parquet_path.as_os_str().to_str().unwrap(),
    ]);

    let assert = cmd.assert();

    assert.success();

    let mut output = Vec::new();
    std::fs::File::open(parquet_path)
        .unwrap()
        .read_to_end(&mut output)
        .unwrap();

    let mut truth = Vec::new();
    std::fs::File::open("tests/data/test.parquet")
        .unwrap()
        .read_to_end(&mut truth)
        .unwrap();

    assert_eq!(output, truth);

    Ok(())
}

#[test]
fn split() -> Result<(), assert_cmd::cargo::CargoError> {
    let mut cmd = assert_cmd::Command::cargo_bin("vcf2parquet")?;

    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path();
    let parquet_path = temp_path.join("test_{}.parquet");

    cmd.args([
        "-i",
        "tests/data/test.vcf",
        "split",
        "-f",
        parquet_path.as_os_str().to_str().unwrap(),
    ]);

    let assert = cmd.assert();

    assert.success();

    let mut output = Vec::new();
    std::fs::File::open(temp_path.join("test_0.parquet"))
        .unwrap()
        .read_to_end(&mut output)
        .unwrap();

    let mut truth = Vec::new();
    std::fs::File::open("tests/data/test.parquet")
        .unwrap()
        .read_to_end(&mut truth)
        .unwrap();

    assert_eq!(output, truth);

    Ok(())
}
