//! vcf2parquet block

/* std use */

/* crate use */

/* project use */
use crate::*;

pub struct Block(std::collections::HashMap<String, Vec<schema::Internal>>);

impl Block {
    pub fn new() -> Self {
        Block(std::collections::HashMap::new())
    }

    pub fn add_record(&mut self, _record: &noodles::vcf::Record) {}
}

impl TryInto<arrow::record_batch::RecordBatch> for Block {
    type Error = error::Error;

    fn try_into(self) -> Result<arrow::record_batch::RecordBatch, Self::Error> {
        Err(error::Error::NoConversion)
    }
}
