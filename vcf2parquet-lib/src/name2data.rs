//! Struct to link name and data

/* std use */

/* crate use */
use arrow2::array::MutableArray;
use arrow2::array::MutablePrimitiveArray;
use arrow2::array::TryPush;

/* project use */

#[derive(Debug)]
pub struct Name2Data(std::collections::HashMap<String, ColumnData>);

impl Name2Data {
    pub fn new(length: usize, header: &noodles::vcf::Header) -> Self {
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

        Self::add_info(&mut name2data, header, length);
        Self::add_genotype(&mut name2data, header, length);

        Name2Data(name2data)
    }

    pub fn get(&self, key: &str) -> Option<&ColumnData> {
        self.0.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut ColumnData> {
        self.0.get_mut(key)
    }

    pub fn add_record(
        &mut self,
        record: noodles::vcf::Record,
        header: &noodles::vcf::Header,
    ) -> std::result::Result<(), arrow2::error::ArrowError> {
        let mut not_changed_key = self
            .0
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<String>>();

        // Chromosome name
        self.get_mut("chromosome")
            .unwrap()
            .push_string(record.chromosome().to_string());
        not_changed_key.remove("chromosome");

        // Position
        self.get_mut("position")
            .unwrap()
            .push_i32(record.position().try_into().ok());
        not_changed_key.remove("position");

        // ID
        if record.ids().is_empty() {
            self.get_mut("identifier").unwrap().push_null();
        } else if let Err(e) = self
            .get_mut("identifier")
            .unwrap()
            .push_vecstring(record.ids().iter().map(|x| Some(x.to_string())).collect())
        {
            return Err(e);
        }
        not_changed_key.remove("identifier");

        // Ref sequence
        self.get_mut("reference")
            .unwrap()
            .push_string(record.reference_bases().to_string());
        not_changed_key.remove("reference");

        // Alt sequences
        if record.alternate_bases().is_empty() {
            self.get_mut("alternate").unwrap().push_null();
        } else if let Err(e) = self.get_mut("alternate").unwrap().push_vecstring(
            record
                .alternate_bases()
                .iter()
                .map(|x| Some(x.to_string()))
                .collect(),
        ) {
            return Err(e);
        }
        not_changed_key.remove("alternate");

        // Quality
        if let Some(quality) = record.quality_score() {
            self.get_mut("quality")
                .unwrap()
                .push_f32(quality.try_into().ok());
        } else {
            self.get_mut("quality").unwrap().push_null();
        }
        not_changed_key.remove("quality");

        // Filter
        if let Some(f) = record.filters() {
            match f {
                noodles::vcf::record::Filters::Pass => {
                    if let Err(e) = self
                        .get_mut("filter")
                        .unwrap()
                        .push_vecstring(vec![Some("PASS".to_string())])
                    {
                        return Err(e);
                    }
                }
                noodles::vcf::record::Filters::Fail(fs) => {
                    if let Err(e) = self
                        .get_mut("filter")
                        .unwrap()
                        .push_vecstring(fs.iter().map(|x| Some(x.to_string())).collect())
                    {
                        return Err(e);
                    }
                }
            }
        } else {
            self.get_mut("filter").unwrap().push_null();
        }
        not_changed_key.remove("filter");

        // Info
        for value in record.info().values() {
            let key = format!("info_{}", value.key());
            not_changed_key.remove(&key);

            match value.value() {
                Some(noodles::vcf::record::info::field::Value::Integer(val)) => {
                    self.get_mut(&key).unwrap().push_i32(Some(*val))
                }
                Some(noodles::vcf::record::info::field::Value::Float(val)) => {
                    self.get_mut(&key).unwrap().push_f32(Some(*val))
                }
                Some(noodles::vcf::record::info::field::Value::Flag) => {
                    self.get_mut(&key).unwrap().push_bool(Some(true))
                }

                Some(noodles::vcf::record::info::field::Value::Character(val)) => {
                    self.get_mut(&key).unwrap().push_string(val.to_string())
                }
                Some(noodles::vcf::record::info::field::Value::String(val)) => {
                    self.get_mut(&key).unwrap().push_string(val.to_string())
                }
                Some(noodles::vcf::record::info::field::Value::IntegerArray(vals)) => {
                    if let Err(e) = self.get_mut(&key).unwrap().push_veci32(vals.to_vec()) {
                        return Err(e);
                    }
                }
                Some(noodles::vcf::record::info::field::Value::FloatArray(vals)) => {
                    if let Err(e) = self.get_mut(&key).unwrap().push_vecf32(vals.to_vec()) {
                        return Err(e);
                    }
                }
                Some(noodles::vcf::record::info::field::Value::CharacterArray(vals)) => {
                    if let Err(e) = self
                        .get_mut(&key)
                        .unwrap()
                        .push_vecstring(vals.iter().map(|x| x.map(String::from)).collect())
                    {
                        return Err(e);
                    }
                }
                Some(noodles::vcf::record::info::field::Value::StringArray(vals)) => {
                    if let Err(e) = self.get_mut(&key).unwrap().push_vecstring(vals.to_vec()) {
                        return Err(e);
                    }
                }
                None => self.get_mut(&key).unwrap().push_null(),
            }
        }

        // format
        for (genotypes, sample) in record.genotypes().iter().zip(header.sample_names()) {
            for (key, value) in genotypes.iter() {
                let key = format!("format_{}_{}", sample, key);
                not_changed_key.remove(&key);

                match value.value() {
                    Some(noodles::vcf::record::genotypes::genotype::field::Value::Integer(val)) => {
                        self.get_mut(&key).unwrap().push_i32(Some(*val))
                    }
                    Some(noodles::vcf::record::genotypes::genotype::field::Value::Float(val)) => {
                        self.get_mut(&key).unwrap().push_f32(Some(*val))
                    }
                    Some(noodles::vcf::record::genotypes::genotype::field::Value::Character(
                        val,
                    )) => self.get_mut(&key).unwrap().push_string(val.to_string()),
                    Some(noodles::vcf::record::genotypes::genotype::field::Value::String(val)) => {
                        self.get_mut(&key).unwrap().push_string(val.to_string())
                    }
                    Some(
                        noodles::vcf::record::genotypes::genotype::field::Value::IntegerArray(vals),
                    ) => {
                        if let Err(e) = self.get_mut(&key).unwrap().push_veci32(vals.to_vec()) {
                            return Err(e);
                        }
                    }
                    Some(noodles::vcf::record::genotypes::genotype::field::Value::FloatArray(
                        vals,
                    )) => {
                        if let Err(e) = self.get_mut(&key).unwrap().push_vecf32(vals.to_vec()) {
                            return Err(e);
                        }
                    }
                    Some(
                        noodles::vcf::record::genotypes::genotype::field::Value::CharacterArray(
                            vals,
                        ),
                    ) => {
                        if let Err(e) = self
                            .get_mut(&key)
                            .unwrap()
                            .push_vecstring(vals.iter().map(|x| x.map(String::from)).collect())
                        {
                            return Err(e);
                        }
                    }
                    Some(noodles::vcf::record::genotypes::genotype::field::Value::StringArray(
                        vals,
                    )) => {
                        if let Err(e) = self.get_mut(&key).unwrap().push_vecstring(vals.to_vec()) {
                            return Err(e);
                        }
                    }
                    None => self.get_mut(&key).unwrap().push_null(),
                }
            }
        }

        for key in not_changed_key {
            self.get_mut(&key).unwrap().push_null();
        }

        Ok(())
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

    fn add_info(
        data: &mut std::collections::HashMap<String, ColumnData>,
        header: &noodles::vcf::Header,
        length: usize,
    ) {
        for (key, value) in header.infos() {
            match (value.ty(), value.number()) {
                (
                    noodles::vcf::header::info::Type::Integer,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => data.insert(
                    format!("info_{}", key),
                    ColumnData::Int(arrow2::array::MutablePrimitiveArray::<i32>::with_capacity(
                        length,
                    )),
                ),
                (noodles::vcf::header::info::Type::Integer, _) => data.insert(
                    format!("info_{}", key),
                    ColumnData::ListInt(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::info::Type::Float,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => data.insert(
                    format!("info_{}", key),
                    ColumnData::Float(arrow2::array::MutablePrimitiveArray::<f32>::with_capacity(
                        length,
                    )),
                ),
                (noodles::vcf::header::info::Type::Float, _) => data.insert(
                    format!("info_{}", key),
                    ColumnData::ListFloat(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::info::Type::Flag,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => data.insert(
                    format!("info_{}", key),
                    ColumnData::Bool(arrow2::array::MutableBooleanArray::with_capacity(length)),
                ),
                (noodles::vcf::header::info::Type::Flag, _) => data.insert(
                    format!("info_{}", key),
                    ColumnData::ListBool(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::info::Type::Character,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => data.insert(
                    format!("info_{}", key),
                    ColumnData::String(arrow2::array::MutableUtf8Array::with_capacity(length)),
                ),
                (noodles::vcf::header::info::Type::Character, _) => data.insert(
                    format!("info_{}", key),
                    ColumnData::ListString(arrow2::array::MutableListArray::with_capacity(length)),
                ),
                (
                    noodles::vcf::header::info::Type::String,
                    noodles::vcf::header::Number::Count(0 | 1),
                ) => data.insert(
                    format!("info_{}", key),
                    ColumnData::String(arrow2::array::MutableUtf8Array::with_capacity(length)),
                ),
                (noodles::vcf::header::info::Type::String, _) => data.insert(
                    format!("info_{}", key),
                    ColumnData::ListString(arrow2::array::MutableListArray::with_capacity(length)),
                ),
            };
        }
    }

    fn add_genotype(
        data: &mut std::collections::HashMap<String, ColumnData>,
        header: &noodles::vcf::Header,
        length: usize,
    ) {
        for sample in header.sample_names() {
            for (key, value) in header.formats() {
                let key = format!("format_{}_{}", sample, key);

                match (value.ty(), value.number()) {
                    (
                        noodles::vcf::header::format::Type::Integer,
                        noodles::vcf::header::Number::Count(0 | 1),
                    ) => data.insert(
                        key,
                        ColumnData::Int(
                            arrow2::array::MutablePrimitiveArray::<i32>::with_capacity(length),
                        ),
                    ),
                    (noodles::vcf::header::format::Type::Integer, _) => data.insert(
                        key,
                        ColumnData::ListInt(arrow2::array::MutableListArray::with_capacity(length)),
                    ),
                    (
                        noodles::vcf::header::format::Type::Float,
                        noodles::vcf::header::Number::Count(0 | 1),
                    ) => data.insert(
                        key,
                        ColumnData::Float(
                            arrow2::array::MutablePrimitiveArray::<f32>::with_capacity(length),
                        ),
                    ),
                    (noodles::vcf::header::format::Type::Float, _) => data.insert(
                        key,
                        ColumnData::ListFloat(arrow2::array::MutableListArray::with_capacity(
                            length,
                        )),
                    ),
                    (
                        noodles::vcf::header::format::Type::Character,
                        noodles::vcf::header::Number::Count(0 | 1),
                    ) => data.insert(
                        key,
                        ColumnData::String(arrow2::array::MutableUtf8Array::with_capacity(length)),
                    ),
                    (noodles::vcf::header::format::Type::Character, _) => data.insert(
                        key,
                        ColumnData::ListString(arrow2::array::MutableListArray::with_capacity(
                            length,
                        )),
                    ),
                    (
                        noodles::vcf::header::format::Type::String,
                        noodles::vcf::header::Number::Count(0 | 1),
                    ) => data.insert(
                        key,
                        ColumnData::String(arrow2::array::MutableUtf8Array::with_capacity(length)),
                    ),
                    (noodles::vcf::header::format::Type::String, _) => data.insert(
                        key,
                        ColumnData::ListString(arrow2::array::MutableListArray::with_capacity(
                            length,
                        )),
                    ),
                };
            }
        }
    }
}

#[derive(Debug)]
pub enum ColumnData {
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
