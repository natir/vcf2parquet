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
    header: noodles::vcf::Header,
}

impl<'a, R> Record2Chunk<'a, R>
where
    R: std::io::BufRead,
{
    pub fn new(
        inner: noodles::vcf::reader::Records<'a, 'a, R>,
        length: usize,
        header: noodles::vcf::Header,
    ) -> Self {
        Self {
            inner,
            length,
            end: false,
            header,
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

        let name2dayta: std::collections::HashMap<String, ColumnData> =
            std::collections::HashMap::new();

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

        let mut infos: std::collections::HashMap<String, ColumnData> =
            std::collections::HashMap::new();

        for _ in 0..self.length {
            match self.inner.next() {
                Some(Ok(record)) => {
                    // Chromosome name
                    chromosomes.push(Some(record.chromosome().to_string()));

                    // Position
                    positions.push(record.position().try_into().ok());

                    // ID
                    if record.ids().is_empty() {
                        identifiers.push_null();
                    } else {
                        let data: Vec<Option<String>> =
                            record.ids().iter().map(|x| Some(x.to_string())).collect();

                        if let Err(e) = identifiers.try_push(Some(data)) {
                            return Some(Err(e));
                        }
                    }

                    // Ref sequence
                    references.push(Some(record.reference_bases().to_string()));

                    // Alt sequences
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

                    // Quality
                    if let Some(quality) = record.quality_score() {
                        qualities.push(quality.try_into().ok());
                    } else {
                        qualities.push_null();
                    }

                    // Filter
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

                    // Info
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

type name2data = std::collections::HashMap<String, ColumnData>;

enum ColumnData {
    Int(arrow2::array::MutablePrimitiveArray<i32>),
    Float(arrow2::array::MutablePrimitiveArray<f32>),
    String(arrow2::array::MutableUtf8Array<i32>),
    ListInt(arrow2::array::MutableListArray<i32, arrow2::array::MutablePrimitiveArray<i32>>),
    ListFloat(arrow2::array::MutableListArray<i32, arrow2::array::MutablePrimitiveArray<f32>>),
    ListString(arrow2::array::MutableListArray<i32, arrow2::array::MutableUtf8Array<i32>>),
}

impl ColumnData {
    pub fn push_i32(&mut self, value: i32) {
        match self {
            ColumnData::Int(a) => a.push(Some(value)),
            _ => todo!(),
        }
    }

    pub fn push_f32(&mut self, value: f32) {
        match self {
            ColumnData::Float(a) => a.push(Some(value)),
            _ => todo!(),
        }
    }

    pub fn push_string(&mut self, value: String) {
        match self {
            ColumnData::String(a) => a.push(Some(value)),
            _ => todo!(),
        }
    }

    pub fn push_veci32(&mut self, value: Vec<Option<i32>>) -> arrow2::error::Result<()> {
        match self {
            ColumnData::ListInt(a) => a.try_push(Some(value)),
            _ => todo!(),
        }
    }

    pub fn push_vecf32(&mut self, value: Vec<Option<f32>>) -> arrow2::error::Result<()> {
        match self {
            ColumnData::ListFloat(a) => a.try_push(Some(value)),
            _ => todo!(),
        }
    }

    pub fn push_vecstring(&mut self, value: Vec<Option<String>>) -> arrow2::error::Result<()> {
        match self {
            ColumnData::ListString(a) => a.try_push(Some(value)),
            _ => todo!(),
        }
    }

    pub fn into_arc(self) -> std::sync::Arc<dyn arrow2::array::Array> {
        match self {
            ColumnData::Int(a) => a.into_arc(),
            ColumnData::Float(a) => a.into_arc(),
            ColumnData::String(a) => a.into_arc(),
            ColumnData::ListInt(a) => a.into_arc(),
            ColumnData::ListFloat(a) => a.into_arc(),
            ColumnData::ListString(a) => a.into_arc(),
        }
    }
}
