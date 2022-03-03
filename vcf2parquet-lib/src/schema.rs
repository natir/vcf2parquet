//! Construct parquet schema corresponding to vcf

/* std use */

/* crate use */

/* project use */
use crate::*;

pub fn from_header(
    header: &noodles::vcf::Header,
) -> error::Result<parquet2::metadata::SchemaDescriptor> {
    Ok(parquet2::metadata::SchemaDescriptor::new(
        "vcf4.3".to_string(),
        Vec::new(),
    ))
}
