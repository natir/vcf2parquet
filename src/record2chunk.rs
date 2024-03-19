//! Convert vcf record iterator into Parquet chunk

/* std use */

/* crate use */

/* project use */
use crate::name2data::*;

pub struct Record2Chunk<T> {
    inner: T,
    length: usize,
    header: noodles::vcf::Header,
    schema: arrow2::datatypes::Schema,
    end: bool,
}

impl<T> Record2Chunk<T>
where
    T: Iterator<Item = std::io::Result<noodles::vcf::Record>>,
{
    pub fn new(
        inner: T,
        length: usize,
        header: noodles::vcf::Header,
        schema: arrow2::datatypes::Schema,
    ) -> Self {
        Self {
            inner,
            length,
            header,
            schema,
            end: false,
        }
    }

    pub fn encodings(&self) -> Vec<Vec<arrow2::io::parquet::write::Encoding>> {
        self.schema
            .fields
            .iter()
            .map(|f| {
                arrow2::io::parquet::write::transverse(&f.data_type, |_| {
                    arrow2::io::parquet::write::Encoding::Plain
                })
            })
            .collect()
    }
}

impl<T> Iterator for Record2Chunk<T>
where
    T: Iterator<Item = std::io::Result<noodles::vcf::Record>>,
{
    type Item = Result<
        arrow2::chunk::Chunk<std::sync::Arc<dyn arrow2::array::Array>>,
        arrow2::error::Error,
    >;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end {
            return None;
        }

        let mut name2data = Name2Data::new(self.length, &self.schema);

        for _ in 0..self.length {
            match self.inner.next() {
                Some(Ok(record)) => {
                    if let Err(e) = name2data.add_record(record, &self.header, &self.schema) {
                        return Some(Err(e));
                    }
                }
                Some(Err(e)) => return Some(Err(arrow2::error::Error::Io(e))),
                None => {
                    self.end = true;

                    return Some(Ok(arrow2::chunk::Chunk::new(
                        name2data.into_arc(&self.schema),
                    )));
                }
            }
        }

        Some(Ok(arrow2::chunk::Chunk::new(
            name2data.into_arc(&self.schema),
        )))
    }
}
