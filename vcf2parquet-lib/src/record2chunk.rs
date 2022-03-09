//! Convert vcf record iterator into Parquet chunk

/* std use */

/* crate use */
use arrow2::array::MutableArray;
use arrow2::array::TryPush;

/* project use */

pub struct Record2Chunk<'a, R>
where
    R: std::io::BufRead,
{
    inner: noodles::vcf::reader::Records<'a, 'a, R>,
    length: usize,
    end: bool,
}

impl<'a, R> Record2Chunk<'a, R>
where
    R: std::io::BufRead,
{
    pub fn new(inner: noodles::vcf::reader::Records<'a, 'a, R>, length: usize) -> Self {
        Self {
            inner,
            length,
            end: false,
        }
    }

    pub fn encodings(&self) -> Vec<arrow2::io::parquet::write::Encoding> {
        vec![arrow2::io::parquet::write::Encoding::Plain; 7]
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

        let mut chromosomes = arrow2::array::MutableUtf8Array::<i32>::with_capacity(self.length);
        let mut positions = arrow2::array::MutablePrimitiveArray::<i32>::with_capacity(self.length);
        let mut identifiers = arrow2::array::MutableListArray::<
            i32,
            arrow2::array::MutableUtf8Array<i32>,
        >::with_capacity(self.length);
        let mut references = arrow2::array::MutableUtf8Array::<i32>::with_capacity(self.length);
        let mut alternatives: arrow2::array::MutableListArray<
            i32,
            arrow2::array::MutableUtf8Array<i32>,
        > = arrow2::array::MutableListArray::new();
        let mut qualities = arrow2::array::MutablePrimitiveArray::<f32>::with_capacity(self.length);

        let mut filters = arrow2::array::MutableListArray::<
            i32,
            arrow2::array::MutableUtf8Array<i32>,
        >::with_capacity(self.length);

        for _ in 0..self.length {
            match self.inner.next() {
                Some(Ok(record)) => {
                    chromosomes.push(Some(record.chromosome().to_string()));
                    positions.push(record.position().try_into().ok());

                    if record.ids().is_empty() {
                        identifiers.push_null();
                    } else {
                        let data: Vec<Option<String>> =
                            record.ids().iter().map(|x| Some(x.to_string())).collect();

                        if let Err(e) = identifiers.try_push(Some(data)) {
                            return Some(Err(e));
                        }
                    }

                    references.push(Some(record.reference_bases().to_string()));

                    if record.alternate_bases().is_empty() {
                        alternatives.push_null();
                    } else {
                        let data: Vec<Option<String>> = record
                            .alternate_bases()
                            .iter()
                            .map(|x| Some(x.to_string()))
                            .collect();

                        if let Err(e) = alternatives.try_push(Some(data)) {
                            return Some(Err(e));
                        }
                    }

                    if let Some(quality) = record.quality_score() {
                        qualities.push(quality.try_into().ok());
                    } else {
                        qualities.push_null();
                    }

                    if let Some(f) = record.filters() {
                        match f {
                            noodles::vcf::record::Filters::Pass => {
                                if let Err(e) =
                                    filters.try_push(Some(vec![Some("PASS".to_string())]))
                                {
                                    return Some(Err(e));
                                }
                            }
                            noodles::vcf::record::Filters::Fail(fs) => {
                                let data: Vec<Option<String>> =
                                    fs.iter().map(|x| Some(x.to_string())).collect();
                                if let Err(e) = filters.try_push(Some(data)) {
                                    return Some(Err(e));
                                }
                            }
                        }
                    } else {
                        filters.push_null();
                    }
                }
                Some(Err(e)) => return Some(Err(arrow2::error::ArrowError::Io(e))),
                None => {
                    self.end = true;

                    return Some(Ok(arrow2::chunk::Chunk::new(vec![
                        chromosomes.into_arc(),
                        positions.into_arc(),
                        identifiers.into_arc(),
                        references.into_arc(),
                        alternatives.into_arc(),
                        qualities.into_arc(),
                        filters.into_arc(),
                    ])));
                }
            }
        }

        Some(Ok(arrow2::chunk::Chunk::new(vec![
            chromosomes.into_arc(),
            positions.into_arc(),
            identifiers.into_arc(),
            references.into_arc(),
            alternatives.into_arc(),
            qualities.into_arc(),
            filters.into_arc(),
        ])))
    }
}
