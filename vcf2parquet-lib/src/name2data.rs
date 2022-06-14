//! Struct to link name and data

/* std use */

/* crate use */
use arrow2::array::MutableArray;
use arrow2::array::MutablePrimitiveArray;
use arrow2::array::TryPush;

/* project use */

///Alias of [std::collections::HashMap] that associate a column name and [ColumnData], a proxy of arrow2 datastructure
#[derive(Debug)]
pub struct Name2Data(std::collections::HashMap<String, ColumnData>);

impl Name2Data {
    /// Create a new Name2Data, vcf header is required to add info and genotype column
    /// length parameter is used to preallocate memory
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

    /// Just a wrapper arround [std::collections::HashMap::get]
    pub fn get(&self, key: &str) -> Option<&ColumnData> {
        self.0.get(key)
    }

    /// Just a wrapper arround [std::collections::HashMap::get_mut]
    pub fn get_mut(&mut self, key: &str) -> Option<&mut ColumnData> {
        self.0.get_mut(key)
    }

    /// Add a vcf record in [std::collections::HashMap] struct
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
                    println!("{}", key);
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

    ///Convert Name2Data in vector of arrow2 array
    pub fn into_arc(
        mut self,
        schema: &arrow2::datatypes::Schema,
    ) -> Vec<std::sync::Arc<dyn arrow2::array::Array>> {
        let s: Vec<std::sync::Arc<dyn arrow2::array::Array>> = schema
            .fields
            .iter()
            .map(|x| self.0.remove(&x.name).unwrap().into_arc())
            .collect();

        s
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
    /// Add a Null value in array
    pub fn push_null(&mut self) {
        match self {
            ColumnData::Bool(a) => a.push_null(),
            ColumnData::Int(a) => a.push_null(),
            ColumnData::Float(a) => a.push_null(),
            ColumnData::String(a) => a.push_null(),
            ColumnData::ListBool(a) => a.push_null(),
            ColumnData::ListInt(a) => a.push_null(),
            ColumnData::ListFloat(a) => a.push_null(),
            ColumnData::ListString(_a) => {
                if let Err(e) = self.push_vecstring(vec![None]) {
                    panic!("ListString {:?}", e);
                }
            }
        }
    }

    /// Add a boolean value in array, if it's not a boolean array failled
    pub fn push_bool(&mut self, value: Option<bool>) {
        match self {
            ColumnData::Bool(a) => a.push(value),
            _ => todo!(),
        }
    }

    /// Add a i32 value in array, if it's not a integer array failled
    pub fn push_i32(&mut self, value: Option<i32>) {
        match self {
            ColumnData::Int(a) => a.push(value),
            _ => todo!(),
        }
    }

    /// Add a f32 value in array, if it's not a float array failled
    pub fn push_f32(&mut self, value: Option<f32>) {
        match self {
            ColumnData::Float(a) => a.push(value),
            _ => todo!(),
        }
    }

    /// Add a string value in array, if it's not a string array failled
    pub fn push_string(&mut self, value: String) {
        match self {
            ColumnData::String(a) => a.push(Some(value)),
            _ => todo!(),
        }
    }

    /// Add a vector of bool value in array, if it's not a vector of bool array failled
    pub fn push_vecbool(&mut self, value: Vec<Option<bool>>) -> arrow2::error::Result<()> {
        match self {
            ColumnData::ListBool(a) => a.try_push(Some(value)),
            _ => todo!(),
        }
    }

    /// Add a vector of integer value in array, if it's not a vector of integer array failled
    pub fn push_veci32(&mut self, value: Vec<Option<i32>>) -> arrow2::error::Result<()> {
        match self {
            ColumnData::ListInt(a) => a.try_push(Some(value)),
            _ => todo!(),
        }
    }

    /// Add a vector of float value in array, if it's not a vector of float array failled
    pub fn push_vecf32(&mut self, value: Vec<Option<f32>>) -> arrow2::error::Result<()> {
        match self {
            ColumnData::ListFloat(a) => a.try_push(Some(value)),
            _ => todo!(),
        }
    }

    /// Add a vector of string value in array, if it's not a vector of string array failled
    pub fn push_vecstring(&mut self, value: Vec<Option<String>>) -> arrow2::error::Result<()> {
        match self {
            ColumnData::ListString(a) => a.try_push(Some(value)),
            _ => todo!(),
        }
    }

    /// Convert ColumnData in Arrow2 array
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

#[cfg(test)]
mod tests {
    use super::*;

    static VCF_FILE: &[u8] = b"##fileformat=VCFv4.1
##fileDate=2022-05-28
##source=ClinVar
##reference=GRCh38
##ID=<Description=\"ClinVar Variation ID\">
##INFO=<ID=ALLELEID,Number=1,Type=Integer,Description=\"the ClinVar Allele ID\">
##INFO=<ID=AF_ESP,Number=1,Type=Float,Description=\"allele frequencies from GO-ESP\">
##INFO=<ID=DBVARID,Number=0,Type=Flag,Description=\"nsv accessions from dbVar for the variant\">
##INFO=<ID=GENEINFO,Number=1,Type=Character,Description=\"Gene(s) for the variant reported as gene symbol:gene id.\">
##INFO=<ID=CLNVC,Number=2,Type=String,Description=\"Variant type\">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description=\"Allelic depths for the ref and alt alleles in the order listed\">
##FORMAT=<ID=DC,Number=1,Type=Float,Description=\"Approximate read depth (reads with MQ=255 or with bad mates are filtered)\">
##FORMAT=<ID=GQ,Number=1,Type=Character,Description=\"Genotype Quality\">
##FORMAT=<ID=GF,Number=3,Type=String,Description=\"Genotype\">
##SAMPLE=<ID=first,Genomes=Germline,Mixture=1.,Description=\"first\">
##SAMPLE=<ID=second,Genomes=Germline,Mixture=1.,Description=\"second\">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tfirst\tsecond
1\t10\t.\tA\tC,T\t23\tPASS\tALLELEID=14;AF_ESP=1.2;DBVARID;GENEINFO=c;CLNVC=test1,test2\tAD:GQ:GF:DC\t1:c:test:1.2\t2:a:Test:4.5
";

    #[test]
    fn init() {
        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap().parse().unwrap();

        let mut data = Name2Data::new(10, &header);
        let mut col_names = data.0.keys().cloned().collect::<Vec<String>>();
        col_names.sort();

        assert_eq!(
            col_names,
            vec![
                "alternate".to_string(),
                "chromosome".to_string(),
                "filter".to_string(),
                "format_first_AD".to_string(),
                "format_first_DC".to_string(),
                "format_first_GF".to_string(),
                "format_first_GQ".to_string(),
                "format_second_AD".to_string(),
                "format_second_DC".to_string(),
                "format_second_GF".to_string(),
                "format_second_GQ".to_string(),
                "identifier".to_string(),
                "info_AF_ESP".to_string(),
                "info_ALLELEID".to_string(),
                "info_CLNVC".to_string(),
                "info_DBVARID".to_string(),
                "info_GENEINFO".to_string(),
                "position".to_string(),
                "quality".to_string(),
                "reference".to_string()
            ]
        );

        assert_eq!(
            format!("{:?}", data.get("chromosome")),
            format!(
                "{:?}",
                Some(&ColumnData::String(arrow2::array::MutableUtf8Array::new()))
            )
        );

        assert_eq!(
            format!("{:?}", data.get_mut("chromosome")),
            format!(
                "{:?}",
                Some(&ColumnData::String(arrow2::array::MutableUtf8Array::new()))
            )
        );
    }

    #[test]
    fn add_record() {
        println!("{}", String::from_utf8(VCF_FILE.to_vec()).unwrap());

        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap().parse().unwrap();

        let mut data = Name2Data::new(10, &header);

        let mut iterator = reader.records(&header);
        let record = iterator.next().unwrap().unwrap();

        data.add_record(record, &header).unwrap();
        assert_eq!(format!("{:?}", data.get("alternate")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: [0, 2], values: MutableUtf8Array { data_type: Utf8, offsets: [0, 1, 2], values: [67, 84], validity: None }, validity: None }))".to_string());

        assert_eq!(format!("{:?}", data.get("chromosome")), "Some(String(MutableUtf8Array { data_type: Utf8, offsets: [0, 1], values: [49], validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("filter")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: [0, 1], values: MutableUtf8Array { data_type: Utf8, offsets: [0, 4], values: [80, 65, 83, 83], validity: None }, validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("format_first_AD")), "Some(ListInt(MutableListArray { data_type: List(Field { name: \"item\", data_type: Int32, is_nullable: true, metadata: {} }), offsets: [0, 1], values: MutablePrimitiveArray { data_type: Int32, values: [1], validity: None }, validity: None }))".to_string());
        assert_eq!(
            format!("{:?}", data.get("format_first_DC")),
            "Some(Float(MutablePrimitiveArray { data_type: Float32, values: [1.2], validity: None }))".to_string()
        );
        assert_eq!(format!("{:?}", data.get("format_first_GQ")), "Some(String(MutableUtf8Array { data_type: Utf8, offsets: [0, 1], values: [99], validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("format_first_GF")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: [0, 1], values: MutableUtf8Array { data_type: Utf8, offsets: [0, 4], values: [116, 101, 115, 116], validity: None }, validity: None }))".to_string());
        assert_eq!(
            format!("{:?}", data.get("format_second_AD")),
            "Some(ListInt(MutableListArray { data_type: List(Field { name: \"item\", data_type: Int32, is_nullable: true, metadata: {} }), offsets: [0, 1], values: MutablePrimitiveArray { data_type: Int32, values: [2], validity: None }, validity: None }))".to_string()
        );
        assert_eq!(
            format!("{:?}", data.get("format_second_DC")),
            "Some(Float(MutablePrimitiveArray { data_type: Float32, values: [4.5], validity: None }))".to_string()
        );
        assert_eq!(
            format!("{:?}", data.get("format_second_GQ")),
            "Some(String(MutableUtf8Array { data_type: Utf8, offsets: [0, 1], values: [97], validity: None }))".to_string()
        );
        assert_eq!(
            format!("{:?}", data.get("format_second_GF")),
            "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: [0, 1], values: MutableUtf8Array { data_type: Utf8, offsets: [0, 4], values: [84, 101, 115, 116], validity: None }, validity: None }))".to_string()
        );
        assert_eq!(format!("{:?}", data.get("identifier")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: [0, 1], values: MutableUtf8Array { data_type: Utf8, offsets: [0, 0], values: [], validity: Some([0b_______0]) }, validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("info_AF_ESP")), "Some(Float(MutablePrimitiveArray { data_type: Float32, values: [1.2], validity: None }))".to_string());
        assert_eq!(
            format!("{:?}", data.get("info_ALLELEID")),
            "Some(Int(MutablePrimitiveArray { data_type: Int32, values: [14], validity: None }))"
                .to_string()
        );
        assert_eq!(format!("{:?}", data.get("info_CLNVC")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: [0, 2], values: MutableUtf8Array { data_type: Utf8, offsets: [0, 5, 10], values: [116, 101, 115, 116, 49, 116, 101, 115, 116, 50], validity: None }, validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("info_DBVARID")), "Some(Bool(MutableBooleanArray { data_type: Boolean, values: [0b_______1], validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("info_GENEINFO")), "Some(String(MutableUtf8Array { data_type: Utf8, offsets: [0, 1], values: [99], validity: None }))".to_string());
        assert_eq!(
            format!("{:?}", data.get("position")),
            "Some(Int(MutablePrimitiveArray { data_type: Int32, values: [10], validity: None }))"
                .to_string()
        );
        assert_eq!(format!("{:?}", data.get("quality")), "Some(Float(MutablePrimitiveArray { data_type: Float32, values: [23.0], validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("reference")), "Some(String(MutableUtf8Array { data_type: Utf8, offsets: [0, 1], values: [65], validity: None }))".to_string());
    }
}
