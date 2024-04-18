//! Convert vcf record iterator into Parquet chunk

/* std use */

/* crate use */

use arrow::datatypes::Field;

/* project use */
use crate::name2data::*;

/// Convert vcf record iterator into Parquet chunk
pub struct Record2Chunk<T> {
    inner: T,
    length: usize,
    header: noodles::vcf::Header,
    schema: std::sync::Arc<arrow::datatypes::Schema>,
    end: bool,
}

impl<T> Record2Chunk<T>
where
    T: Iterator<Item = std::io::Result<noodles::vcf::Record>>,
{
    /// Create a new Record2Chunk
    pub fn new(
        inner: T,
        length: usize,
        header: noodles::vcf::Header,
        schema: std::sync::Arc<arrow::datatypes::Schema>,
    ) -> Self {
        Self {
            inner,
            length,
            header,
            schema,
            end: false,
        }
    }
}

impl<T> Iterator for Record2Chunk<T>
where
    T: Iterator<Item = std::io::Result<noodles::vcf::Record>>,
{
    type Item = Result<arrow::array::RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end {
            return None;
        }

        let mut name2data = Name2Data::new(self.length, &self.schema);

        for _ in 0..self.length {
            match self.inner.next() {
                Some(Ok(record)) => {
                    if let Err(e) = name2data.add_record(
                        record,
                        &self.header,
                        &self
                            .schema
                            .all_fields()
                            .into_iter()
                            .map(|f| (f.name().to_string(), f.clone()))
                            .collect::<rustc_hash::FxHashMap<String, Field>>(),
                    ) {
                        return Some(Err(e));
                    }
                }
                Some(Err(e)) => {
                    return Some(Err(arrow::error::ArrowError::IoError("".to_string(), e)))
                }
                None => {
                    self.end = true;

                    return Some(arrow::record_batch::RecordBatch::try_new(
                        self.schema.clone(),
                        name2data.into_arc(&self.schema),
                    ));
                }
            }
        }

        Some(arrow::record_batch::RecordBatch::try_new(
            self.schema.clone(),
            name2data.into_arc(&self.schema),
        ))
    }
}
