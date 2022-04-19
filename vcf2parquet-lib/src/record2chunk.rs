//! Convert vcf record iterator into Parquet chunk

/* std use */

/* crate use */

/* project use */
use crate::name2data::*;

pub struct Record2Chunk<'a, R>
where
    R: std::io::BufRead,
{
    inner: noodles::vcf::reader::Records<'a, 'a, R>,
    length: usize,
    end: bool,
    header: noodles::vcf::Header,
    schema: arrow2::datatypes::Schema,
}

impl<'a, R> Record2Chunk<'a, R>
where
    R: std::io::BufRead,
{
    pub fn new(
        inner: noodles::vcf::reader::Records<'a, 'a, R>,
        length: usize,
        header: noodles::vcf::Header,
        schema: arrow2::datatypes::Schema,
    ) -> Self {
        Self {
            inner,
            length,
            end: false,
            header,
            schema,
        }
    }

    pub fn encodings(&self) -> Vec<arrow2::io::parquet::write::Encoding> {
        vec![arrow2::io::parquet::write::Encoding::Plain; self.schema.fields.len()]
    }
}

impl<'a, R> Iterator for Record2Chunk<'a, R>
where
    R: std::io::BufRead,
{
    type Item = Result<
        arrow2::chunk::Chunk<std::sync::Arc<dyn arrow2::array::Array>>,
        arrow2::error::ArrowError,
    >;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end {
            return None;
        }

        let mut name2data = Name2Data::new(self.length, &self.header);

        for _ in 0..self.length {
            match self.inner.next() {
                Some(Ok(record)) => {
                    if let Err(e) = name2data.add_record(record, &self.header) {
                        return Some(Err(e));
                    }
                }
                Some(Err(e)) => return Some(Err(arrow2::error::ArrowError::Io(e))),
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
