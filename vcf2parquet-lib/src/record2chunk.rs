//! Convert vcf record iterator into Parquet chunk

/* std use */

/* crate use */
use arrow2::array::MutableArray;
use arrow2::array::MutablePrimitiveArray;
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

        let mut name2data = Name2Data::new(self.length);
        name2data.add_info(&self.header, self.length);
        name2data.add_genotype(&self.header, self.length);

        for _ in 0..self.length {
            match self.inner.next() {
                Some(Ok(record)) => {
                    // Chromosome name
                    name2data
                        .get_mut("chromosome")
                        .unwrap()
                        .push_string(record.chromosome().to_string());

                    // Position
                    name2data
                        .get_mut("position")
                        .unwrap()
                        .push_i32(record.position().try_into().ok());

                    // ID
                    if record.ids().is_empty() {
                        name2data.get_mut("identifier").unwrap().push_null();
                    } else {
                        if let Err(e) = name2data.get_mut("identifier").unwrap().push_vecstring(
                            record.ids().iter().map(|x| Some(x.to_string())).collect(),
                        ) {
                            return Some(Err(e));
                        }
                    }

                    // Ref sequence
                    name2data
                        .get_mut("reference")
                        .unwrap()
                        .push_string(record.reference_bases().to_string());

                    // Alt sequences
                    if record.alternate_bases().is_empty() {
                        name2data.get_mut("alternate").unwrap().push_null();
                    } else {
                        if let Err(e) = name2data.get_mut("alternate").unwrap().push_vecstring(
                            record
                                .alternate_bases()
                                .iter()
                                .map(|x| Some(x.to_string()))
                                .collect(),
                        ) {
                            return Some(Err(e));
                        }
                    }

                    // Quality
                    if let Some(quality) = record.quality_score() {
                        name2data
                            .get_mut("quality")
                            .unwrap()
                            .push_f32(quality.try_into().ok());
                    } else {
                        name2data.get_mut("quality").unwrap().push_null();
                    }

                    // Filter
                    if let Some(f) = record.filters() {
                        match f {
                            noodles::vcf::record::Filters::Pass => {
                                if let Err(e) = name2data
                                    .get_mut("filter")
                                    .unwrap()
                                    .push_vecstring(vec![Some("PASS".to_string())])
                                {
                                    return Some(Err(e));
                                }
                            }
                            noodles::vcf::record::Filters::Fail(fs) => {
                                if let Err(e) = name2data.get_mut("filter").unwrap().push_vecstring(
                                    fs.iter().map(|x| Some(x.to_string())).collect(),
                                ) {
                                    return Some(Err(e));
                                }
                            }
                        }
                    } else {
                        name2data.get_mut("filter").unwrap().push_null();
                    }

                    // Info
                    for value in record.info().values() {
                        match value.value() {
                            Some(noodles::vcf::record::info::field::Value::Integer(val)) => {
                                name2data
                                    .get_mut(&format!("info_{}", value.key()))
                                    .unwrap()
                                    .push_i32(Some(*val))
                            }
                            Some(noodles::vcf::record::info::field::Value::Float(val)) => name2data
                                .get_mut(&format!("info_{}", value.key()))
                                .unwrap()
                                .push_f32(Some(*val)),
                            Some(noodles::vcf::record::info::field::Value::Flag) => name2data
                                .get_mut(&format!("info_{}", value.key()))
                                .unwrap()
                                .push_bool(Some(true)),

                            Some(noodles::vcf::record::info::field::Value::Character(val)) => {
                                name2data
                                    .get_mut(&format!("info_{}", value.key()))
                                    .unwrap()
                                    .push_string(val.to_string())
                            }
                            Some(noodles::vcf::record::info::field::Value::String(val)) => {
                                name2data
                                    .get_mut(&format!("info_{}", value.key()))
                                    .unwrap()
                                    .push_string(val.to_string())
                            }
                            Some(noodles::vcf::record::info::field::Value::IntegerArray(vals)) => {
                                if let Err(e) = name2data
                                    .get_mut(&format!("info_{}", value.key()))
                                    .unwrap()
                                    .push_veci32(vals.to_vec())
                                {
                                    return Some(Err(e));
                                }
                            }
                            Some(noodles::vcf::record::info::field::Value::FloatArray(vals)) => {
                                if let Err(e) = name2data
                                    .get_mut(&format!("info_{}", value.key()))
                                    .unwrap()
                                    .push_vecf32(vals.to_vec())
                                {
                                    return Some(Err(e));
                                }
                            }
                            Some(noodles::vcf::record::info::field::Value::CharacterArray(
                                vals,
                            )) => {
                                if let Err(e) = name2data
                                    .get_mut(&format!("info_{}", value.key()))
                                    .unwrap()
                                    .push_vecstring(
                                        vals.iter().map(|x| x.map(String::from)).collect(),
                                    )
                                {
                                    return Some(Err(e));
                                }
                            }
                            Some(noodles::vcf::record::info::field::Value::StringArray(vals)) => {
                                if let Err(e) = name2data
                                    .get_mut(&format!("info_{}", value.key()))
                                    .unwrap()
                                    .push_vecstring(vals.to_vec())
                                {
                                    return Some(Err(e));
                                }
                            }
                            None => name2data
                                .get_mut(&format!("info_{}", value.key()))
                                .unwrap()
                                .push_null(),
                        }
                    }

                    // format
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

struct Name2Data(std::collections::HashMap<String, ColumnData>);

impl Name2Data {
    pub fn new(length: usize) -> Self {
        let mut name2data = std::collections::HashMap::new();

        name2data.insert(
            "chromosome".to_string(),
            ColumnData::String(arrow2::array::MutableUtf8Array::<i32>::with_capacity(
                length,
            )),
        );
        name2data.insert(
            "position".to_string(),
            ColumnData::Int(MutablePrimitiveArray::<i32>::with_capacity(length)),
        );

        name2data.insert(
            "identifier".to_string(),
            ColumnData::ListString(arrow2::array::MutableListArray::<
                i32,
                arrow2::array::MutableUtf8Array<i32>,
            >::with_capacity(length)),
        );

        name2data.insert(
            "reference".to_string(),
            ColumnData::String(arrow2::array::MutableUtf8Array::<i32>::with_capacity(
                length,
            )),
        );

        name2data.insert(
            "alternate".to_string(),
            ColumnData::ListString(arrow2::array::MutableListArray::with_capacity(length)),
        );

        name2data.insert(
            "quality".to_string(),
            ColumnData::Float(MutablePrimitiveArray::<f32>::with_capacity(length)),
        );

        name2data.insert(
            "filter".to_string(),
            ColumnData::ListString(arrow2::array::MutableListArray::<
                i32,
                arrow2::array::MutableUtf8Array<i32>,
            >::with_capacity(length)),
        );

        Name2Data(name2data)
    }

    pub fn get(&self, key: &str) -> Option<&ColumnData> {
        self.0.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut ColumnData> {
        self.0.get_mut(key)
    }

    pub fn add_info(&mut self, header: &noodles::vcf::Header, length: usize) {
        for (key, value) in header.infos() {
            match (value.ty(), value.number()) {
                (
                    noodles::vcf::header::info::Type::Integer,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::Int(arrow2::array::MutablePrimitiveArray::<i32>::with_capacity(
                        length,
                    )),
                ),
                (noodles::vcf::header::info::Type::Integer, _) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::ListInt(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::info::Type::Float,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::Float(arrow2::array::MutablePrimitiveArray::<f32>::with_capacity(
                        length,
                    )),
                ),
                (noodles::vcf::header::info::Type::Float, _) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::ListFloat(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::info::Type::Flag,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::Bool(arrow2::array::MutableBooleanArray::with_capacity(length)),
                ),
                (noodles::vcf::header::info::Type::Flag, _) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::ListBool(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::info::Type::Character,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::String(arrow2::array::MutableUtf8Array::with_capacity(length)),
                ),
                (noodles::vcf::header::info::Type::Character, _) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::ListString(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::info::Type::String,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::String(arrow2::array::MutableUtf8Array::with_capacity(length)),
                ),
                (noodles::vcf::header::info::Type::String, _) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::ListString(arrow2::array::MutableListArray::with_capacity(length)),
                ),
            };
        }
    }

    pub fn add_genotype(&mut self, header: &noodles::vcf::Header, length: usize) {
        for (key, value) in header.formats() {
            match (value.ty(), value.number()) {
                (
                    noodles::vcf::header::format::Type::Integer,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::Int(arrow2::array::MutablePrimitiveArray::<i32>::with_capacity(
                        length,
                    )),
                ),
                (noodles::vcf::header::format::Type::Integer, _) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::ListInt(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::format::Type::Float,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::Float(arrow2::array::MutablePrimitiveArray::<f32>::with_capacity(
                        length,
                    )),
                ),
                (noodles::vcf::header::format::Type::Float, _) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::ListFloat(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::format::Type::Character,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::String(arrow2::array::MutableUtf8Array::with_capacity(length)),
                ),
                (noodles::vcf::header::format::Type::Character, _) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::ListString(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::format::Type::String,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::String(arrow2::array::MutableUtf8Array::with_capacity(length)),
                ),
                (noodles::vcf::header::format::Type::String, _) => self.0.insert(
                    format!("info_{}", key),
                    ColumnData::ListString(arrow2::array::MutableListArray::with_capacity(length)),
                ),
            };
        }
    }

    pub fn into_arc(
        mut self,
        schema: &arrow2::datatypes::Schema,
    ) -> Vec<std::sync::Arc<dyn arrow2::array::Array>> {
        schema
            .fields
            .iter()
            .map(|x| self.0.remove(&x.name).unwrap().into_arc())
            .collect()
    }
}

enum ColumnData {
    Bool(arrow2::array::MutableBooleanArray),
    Int(arrow2::array::MutablePrimitiveArray<i32>),
    Float(arrow2::array::MutablePrimitiveArray<f32>),
    String(arrow2::array::MutableUtf8Array<i32>),
    ListBool(arrow2::array::MutableListArray<i32, arrow2::array::MutableBooleanArray>),
    ListInt(arrow2::array::MutableListArray<i32, MutablePrimitiveArray<i32>>),
    ListFloat(arrow2::array::MutableListArray<i32, MutablePrimitiveArray<f32>>),
    ListString(arrow2::array::MutableListArray<i32, arrow2::array::MutableUtf8Array<i32>>),
}

impl ColumnData {
    pub fn push_null(&mut self) {
        match self {
            ColumnData::Bool(a) => a.push_null(),
            ColumnData::Int(a) => a.push_null(),
            ColumnData::Float(a) => a.push_null(),
            ColumnData::String(a) => a.push_null(),
            ColumnData::ListBool(a) => a.push_null(),
            ColumnData::ListInt(a) => a.push_null(),
            ColumnData::ListFloat(a) => a.push_null(),
            ColumnData::ListString(a) => a.push_null(),
        }
    }

    pub fn push_bool(&mut self, value: Option<bool>) {
        match self {
            ColumnData::Bool(a) => a.push(value),
            _ => todo!(),
        }
    }

    pub fn push_i32(&mut self, value: Option<i32>) {
        match self {
            ColumnData::Int(a) => a.push(value),
            _ => todo!(),
        }
    }

    pub fn push_f32(&mut self, value: Option<f32>) {
        match self {
            ColumnData::Float(a) => a.push(value),
            _ => todo!(),
        }
    }

    pub fn push_string(&mut self, value: String) {
        match self {
            ColumnData::String(a) => a.push(Some(value)),
            _ => todo!(),
        }
    }

    pub fn push_vecbool(&mut self, value: Vec<Option<bool>>) -> arrow2::error::Result<()> {
        match self {
            ColumnData::ListBool(a) => a.try_push(Some(value)),
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
            ColumnData::Bool(a) => a.into_arc(),
            ColumnData::Int(a) => a.into_arc(),
            ColumnData::Float(a) => a.into_arc(),
            ColumnData::String(a) => a.into_arc(),
            ColumnData::ListBool(a) => a.into_arc(),
            ColumnData::ListInt(a) => a.into_arc(),
            ColumnData::ListFloat(a) => a.into_arc(),
            ColumnData::ListString(a) => a.into_arc(),
        }
    }
}
