#[cfg(feature = "bin")]
pub use vcf2parquet_bin::{error, main as bin_main};

pub fn main() -> error::Result<()> {
    bin_main()
}
