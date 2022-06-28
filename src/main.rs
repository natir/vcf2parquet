#[cfg(feature = "bin")]
pub use vcf2parquet_bin::*;

#[cfg(feature = "bin")]
fn main() -> vcf2parquet_bin::error::Result<()> {
    vcf2parquet_bin::main()
}
