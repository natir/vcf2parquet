//! Struct to link name and data

/* std use */

/* crate use */
use arrow::datatypes::Field;
use noodles::vcf::record::genotypes::sample::value::genotype::allele::Phasing;

/* project use */
use crate::columndata::ColumnData;

///Alias of [std::collections::HashMap] that associate a column name and [ColumnData], a proxy of arrow2 datastructure
#[derive(Debug)]
pub struct Name2Data(rustc_hash::FxHashMap<String, ColumnData>);

impl Name2Data {
    /// Create a new Name2Data, vcf header is required to add info and genotype column
    /// length parameter is used to preallocate memory
    pub fn new(length: usize, schema: &arrow::datatypes::Schema) -> Self {
        let mut name2data = rustc_hash::FxHashMap::default();
        for field in schema.fields.iter() {
            let nullable = match field.data_type() {
                arrow::datatypes::DataType::List(a) => a.is_nullable(),
                _ => field.is_nullable(),
            };

            let column = ColumnData::new(field.data_type(), length, field.name(), nullable);
            name2data.insert(field.name().to_string(), column);
        }
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
        schema: &rustc_hash::FxHashMap<String, Field>,
    ) -> std::result::Result<(), arrow::error::ArrowError> {
        let allele_count = record.alternate_bases().len() + 1;
        for (alt_id, allele) in record.alternate_bases().iter().enumerate() {
            for (key, column) in self.0.iter_mut() {
                match key.as_str() {
                    "chromosome" => column.push_string(record.chromosome().to_string()),
                    "position" => column.push_i32(Some(usize::from(record.position()) as i32)),
                    "identifier" => column.push_vecstring(
                        record.ids().iter().map(|s| Some(s.to_string())).collect(),
                    )?,
                    "reference" => column.push_string(record.reference_bases().to_string()),
                    "alternate" => column.push_string(allele.to_string()),
                    "quality" => column.push_f32(record.quality_score().map(|v| v.into())),
                    "filter" => column.push_vecstring(
                        record
                            .filters()
                            .iter()
                            .map(|s| Some(s.to_string()))
                            .collect(),
                    )?,
                    _ => {}
                }
            }
            self.add_info(&record, header, schema, alt_id, allele_count)?;
            self.add_format(&record, header, schema, alt_id, allele_count)?;
        }
        Ok(())
    }

    fn add_info(
        &mut self,
        record: &noodles::vcf::Record,
        header: &noodles::vcf::Header,
        schema: &rustc_hash::FxHashMap<String, Field>,
        alt_id: usize,
        allele_count: usize,
    ) -> std::result::Result<(), arrow::error::ArrowError> {
        let info = record.info();

        for key in header.infos().keys() {
            let key_name = format!("info_{}", key);
            let info_def = header.infos().get(key).unwrap();
            if let Some(column) = self.0.get_mut(&key_name) {
                match info.get(key) {
                    Some(value) => match value {
                        Some(noodles::vcf::record::info::field::Value::Flag) => {
                            column.push_bool(true);
                        }
                        Some(noodles::vcf::record::info::field::Value::Integer(value)) => {
                            column.push_i32(Some(*value));
                        }
                        Some(noodles::vcf::record::info::field::Value::Float(value)) => {
                            column.push_f32(Some(*value));
                        }
                        Some(noodles::vcf::record::info::field::Value::String(value)) => {
                            column.push_string(value.to_string());
                        }
                        Some(noodles::vcf::record::info::field::Value::Character(value)) => {
                            column.push_string(value.to_string());
                        }
                        Some(noodles::vcf::record::info::field::Value::Array(arr)) => match arr
                            .clone()
                        {
                            noodles::vcf::record::info::field::value::Array::Integer(array_val) => {
                                match info_def.number() {
                                    noodles::vcf::header::Number::Count(0 | 1) => {
                                        unreachable!(
                                            "Field {} declared as single value but found array",
                                            key
                                        )
                                    }
                                    noodles::vcf::header::Number::Count(_) => {
                                        column.push_veci32(array_val)?;
                                    }
                                    noodles::vcf::header::Number::A => {
                                        column.push_i32(*array_val.get(alt_id).unwrap());
                                    }
                                    noodles::vcf::header::Number::R => {
                                        column.push_veci32(vec![
                                            *array_val.first().unwrap(),
                                            *array_val.get(alt_id + 1).unwrap(),
                                        ])?;
                                    }
                                    noodles::vcf::header::Number::G => {
                                        if array_val.len()
                                            == (allele_count * (allele_count + 1) / 2)
                                        {
                                            column.push_veci32(vec![
                                                *array_val.first().unwrap(),
                                                *array_val
                                                    .get((alt_id * alt_id + 3 * alt_id + 2) / 2)
                                                    .unwrap(),
                                                *array_val
                                                    .get((alt_id * alt_id + 5 * alt_id + 4) / 2)
                                                    .unwrap(),
                                            ])?;
                                        } else if array_val.len() == allele_count {
                                            column.push_veci32(vec![
                                                *array_val.first().unwrap(),
                                                Some(0),
                                                *array_val.get(alt_id + 1).unwrap(),
                                            ])?;
                                        } else {
                                            column.push_null();
                                        }
                                    }
                                    noodles::vcf::header::Number::Unknown => {
                                        column.push_veci32(array_val)?;
                                    }
                                }
                            }
                            noodles::vcf::record::info::field::value::Array::Float(array_val) => {
                                match info_def.number() {
                                    noodles::vcf::header::Number::Count(0 | 1) => {
                                        unreachable!(
                                            "Field {} declared as single value but found array",
                                            key
                                        )
                                    }
                                    noodles::vcf::header::Number::Count(_) => {
                                        column.push_vecf32(array_val)?;
                                    }
                                    noodles::vcf::header::Number::A => {
                                        column.push_f32(*array_val.get(alt_id).unwrap());
                                    }
                                    noodles::vcf::header::Number::R => {
                                        column.push_vecf32(vec![
                                            *array_val.first().unwrap(),
                                            *array_val.get(alt_id + 1).unwrap(),
                                        ])?;
                                    }
                                    noodles::vcf::header::Number::G => {
                                        if array_val.len()
                                            == (allele_count * (allele_count + 1) / 2)
                                        {
                                            column.push_vecf32(vec![
                                                *array_val.first().unwrap(),
                                                *array_val
                                                    .get((alt_id * alt_id + 3 * alt_id + 2) / 2)
                                                    .unwrap(),
                                                *array_val
                                                    .get((alt_id * alt_id + 5 * alt_id + 4) / 2)
                                                    .unwrap(),
                                            ])?;
                                        } else if array_val.len() == allele_count {
                                            column.push_vecf32(vec![
                                                *array_val.first().unwrap(),
                                                Some(0.),
                                                *array_val.get(alt_id + 1).unwrap(),
                                            ])?;
                                        } else {
                                            column.push_null();
                                        }
                                    }
                                    noodles::vcf::header::Number::Unknown => {
                                        column.push_vecf32(array_val)?;
                                    }
                                }
                            }
                            noodles::vcf::record::info::field::value::Array::String(array_val) => {
                                match info_def.number() {
                                    noodles::vcf::header::Number::Count(0 | 1) => {
                                        unreachable!(
                                            "Field {} declared as single value but found array",
                                            key_name
                                        )
                                    }
                                    noodles::vcf::header::Number::Count(_) => {
                                        column.push_vecstring(array_val)?;
                                    }
                                    noodles::vcf::header::Number::A => {
                                        column.push_string(
                                            array_val.get(alt_id).unwrap().clone().unwrap(),
                                        );
                                    }
                                    noodles::vcf::header::Number::R => {
                                        column.push_vecstring(vec![
                                            Some(array_val.first().unwrap().clone().unwrap()),
                                            Some(
                                                array_val.get(alt_id + 1).unwrap().clone().unwrap(),
                                            ),
                                        ])?;
                                    }
                                    noodles::vcf::header::Number::G => {
                                        if array_val.len()
                                            == (allele_count * (allele_count + 1) / 2)
                                        {
                                            column.push_vecstring(vec![
                                                array_val.first().unwrap().clone(),
                                                array_val
                                                    .get((alt_id * alt_id + 3 * alt_id + 2) / 2)
                                                    .unwrap()
                                                    .clone(),
                                                array_val
                                                    .get((alt_id * alt_id + 5 * alt_id + 4) / 2)
                                                    .unwrap()
                                                    .clone(),
                                            ])?;
                                        } else if array_val.len() == allele_count {
                                            column.push_vecstring(vec![
                                                array_val.first().unwrap().clone(),
                                                Some(".".to_string()),
                                                array_val.get(alt_id + 1).unwrap().clone(),
                                            ])?;
                                        } else {
                                            column.push_null();
                                        }
                                    }
                                    noodles::vcf::header::Number::Unknown => {
                                        column.push_vecstring(array_val)?;
                                    }
                                }
                            }
                            noodles::vcf::record::info::field::value::Array::Character(
                                array_val,
                            ) => match info_def.number() {
                                noodles::vcf::header::Number::Count(0 | 1) => {
                                    unreachable!(
                                        "Field {} declared as single value but found array",
                                        key_name
                                    )
                                }
                                noodles::vcf::header::Number::Count(_) => {
                                    column.push_vecstring(
                                        array_val
                                            .iter()
                                            .map(|s| s.as_ref().map(|s| s.to_string()))
                                            .collect::<Vec<Option<String>>>(),
                                    )?;
                                }
                                noodles::vcf::header::Number::A => {
                                    column.push_string(
                                        (*array_val.get(alt_id).unwrap()).unwrap().to_string(),
                                    );
                                }
                                noodles::vcf::header::Number::R => {
                                    column.push_vecstring(vec![
                                        Some(array_val.first().unwrap().unwrap().to_string()),
                                        Some(
                                            array_val.get(alt_id + 1).unwrap().unwrap().to_string(),
                                        ),
                                    ])?;
                                }
                                noodles::vcf::header::Number::G => {
                                    if array_val.len() == (allele_count * (allele_count + 1) / 2) {
                                        column.push_vecstring(vec![
                                            Some(array_val.first().unwrap().unwrap().to_string()),
                                            Some(
                                                array_val
                                                    .get((alt_id * alt_id + 3 * alt_id + 2) / 2)
                                                    .unwrap()
                                                    .unwrap()
                                                    .to_string(),
                                            ),
                                            Some(
                                                array_val
                                                    .get((alt_id * alt_id + 5 * alt_id + 4) / 2)
                                                    .unwrap()
                                                    .unwrap()
                                                    .to_string(),
                                            ),
                                        ])?;
                                    } else if array_val.len() == allele_count {
                                        column.push_vecstring(vec![
                                            Some(array_val.first().unwrap().unwrap().to_string()),
                                            Some(".".to_string()),
                                            Some(
                                                array_val
                                                    .get(alt_id + 1)
                                                    .unwrap()
                                                    .unwrap()
                                                    .to_string(),
                                            ),
                                        ])?;
                                    } else {
                                        column.push_null();
                                    }
                                }
                                noodles::vcf::header::Number::Unknown => {
                                    column.push_vecstring(
                                        array_val
                                            .iter()
                                            .map(|s| s.as_ref().map(|s| s.to_string()))
                                            .collect::<Vec<Option<String>>>(),
                                    )?;
                                }
                            },
                        },
                        None => {
                            unreachable!(
                                "Since the outermost option is Some, this should be unreachable"
                            );
                        }
                    },
                    None => {
                        if info_def.ty()
                            == noodles::vcf::header::record::value::map::info::Type::Flag
                        {
                            column.push_bool(false);
                        } else {
                            //Handle missing info field, only matters for FixedSizeList
                            if schema.get(&key_name).is_some() {
                                match column {
                                    ColumnData::ListFloat(_) => {
                                        column.push_vecf32(vec![])?;
                                    }
                                    ColumnData::ListInt(_) => {
                                        column.push_veci32(vec![])?;
                                    }
                                    ColumnData::ListString(_) => {
                                        column.push_vecstring(vec![])?;
                                    }
                                    _ => column.push_null(), //Otherwise, just push null
                                }
                            } else {
                                unreachable!("Malformed VCF, {} should be in schema", key_name);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn add_format(
        &mut self,
        record: &noodles::vcf::Record,
        header: &noodles::vcf::Header,
        schema: &rustc_hash::FxHashMap<String, Field>,
        alt_id: usize,
        allele_count: usize,
    ) -> std::result::Result<(), arrow::error::ArrowError> {
        for key in header.formats().keys() {
            for (idx, sample) in header.sample_names().iter().enumerate() {
                let key_name = format!("format_{}_{}", sample, key);
                let format_def = header.formats().get(key).unwrap();
                if let Some(column) = self.0.get_mut(&key_name) {
                    if let Some(format_field) = record.genotypes().get_index(idx) {
                        match format_field.get(key).flatten() {
                            Some(value) => match value {
                                    noodles::vcf::record::genotypes::sample::Value::Integer(value) => column.push_i32(Some(*value)),
                                    noodles::vcf::record::genotypes::sample::Value::Float(value) => column.push_f32(Some(*value)),
                                    noodles::vcf::record::genotypes::sample::Value::String(value) => {
                                    if key.to_string()=="GT" {
                                        let mut gt_str = String::with_capacity(32); //Arbitrary capacity
                                        if let Some(gt) = format_field.genotype().and_then(|g|g.ok())
                                        {
                                            gt.iter().enumerate().for_each(|(i,allele)| {
                                                let (position, phasing) = (allele.position(), allele.phasing());
                                                match position {
                                                    Some(a) if a == alt_id + 1 => {
                                                        gt_str.push('1');
                                                    }
                                                    Some(0)=>{
                                                        gt_str.push('0');
                                                    }
                                                    Some(_) =>{
                                                        gt_str.push('.');
                                                    }
                                                    None=>{
                                                        gt_str.push('.');
                                                    }
                                                }
                                                if i < gt.len() - 1 {
                                                    gt_str.push(match phasing {
							Phasing::Phased => '|',
							Phasing::Unphased => '/',
                                                    });
                                                }
                                            });
                                        }
                                        else {
                                            unreachable!("If GT is not present, the match arm won't take us there")
                                        }
                                        column.push_string(gt_str);
                                    } else {
                                        column.push_string(value.to_string());
                                    }
                                }
                                    noodles::vcf::record::genotypes::sample::Value::Character(
                                        value
                                ) => {
                                    column.push_string(value.to_string());
                                }
                                    noodles::vcf::record::genotypes::sample::Value::Array(arr)
                                 => match arr.clone() {
                                    noodles::vcf::record::genotypes::sample::value::Array::Integer(
                                        array_val,
                                    ) => match format_def.number() {
                                        noodles::vcf::header::Number::Count(0 | 1) => {
                                            unreachable!(
                                                "Field {} declared as single value but found array",
                                                key
                                            )
                                        }
                                        noodles::vcf::header::Number::Count(_) => {
                                            column.push_veci32(array_val)?;
                                        }
                                        noodles::vcf::header::Number::A => {
                                            column.push_i32(*array_val.get(alt_id).unwrap());
                                        }
                                        noodles::vcf::header::Number::R => {
                                            //TODO: Use push_fixed_size_i32
                                            column.push_veci32(vec![
                                                *array_val.first().unwrap(),
                                                *array_val.get(alt_id + 1).unwrap(),
                                            ])?;
                                        }
                                        noodles::vcf::header::Number::G => {
                                            if array_val.len()
                                            == (allele_count * (allele_count + 1) / 2)
                                            {
                                                column.push_veci32(vec![
                                                    *array_val.first().unwrap(),
                                                    *array_val
                                                        .get((alt_id * alt_id + 3 * alt_id + 2) / 2)
                                                        .unwrap(),
                                                    *array_val
                                                        .get((alt_id * alt_id + 5 * alt_id + 4) / 2)
                                                        .unwrap(),
                                                ])?;
                                            } else if array_val.len() == allele_count {
                                                column.push_veci32(vec![
                                                    *array_val.first().unwrap(),
                                                    Some(0),
                                                    *array_val.get(alt_id + 1).unwrap(),
                                                ])?;
                                            } else {
                                                column.push_null();
                                            }
                                        }
                                        noodles::vcf::header::Number::Unknown => {
                                            column.push_veci32(array_val)?;
                                        }
                                    },
                                    noodles::vcf::record::genotypes::sample::value::Array::Float(
                                        array_val,
                                    ) => match format_def.number() {
                                        noodles::vcf::header::Number::Count(0 | 1) => {
                                            unreachable!(
                                                "Field {} declared as single value but found array",
                                                key
                                            )
                                        }
                                        noodles::vcf::header::Number::Count(_) => {
                                            column.push_vecf32(array_val)?;
                                        }
                                        noodles::vcf::header::Number::A => {
                                            column.push_f32(*array_val.get(alt_id).unwrap());
                                        }
                                        noodles::vcf::header::Number::R => {
                                            //TODO: Use push_fixed_size_f32
                                            column.push_vecf32(vec![
                                                *array_val.first().unwrap(),
                                                *array_val.get(alt_id + 1).unwrap(),
                                            ])?;
                                        }
                                        noodles::vcf::header::Number::G => {
                                            if array_val.len()
                                            == (allele_count * (allele_count + 1) / 2)
                                            {
                                                column.push_vecf32(vec![
                                                    *array_val.first().unwrap(),
                                                    *array_val
                                                        .get((alt_id * alt_id + 3 * alt_id + 2) / 2)
                                                        .unwrap(),
                                                    *array_val
                                                        .get((alt_id * alt_id + 5 * alt_id + 4) / 2)
                                                        .unwrap(),
                                                ])?;
                                            } else if array_val.len() == allele_count {
                                                column.push_vecf32(vec![
                                                    *array_val.first().unwrap(),
                                                    Some(0.),
                                                    *array_val.get(alt_id + 1).unwrap(),
                                                ])?;
                                            } else {
                                                column.push_null();
                                            }
                                        }
                                        noodles::vcf::header::Number::Unknown => {
                                            column.push_vecf32(array_val)?;
                                        }
                                    },
                                    noodles::vcf::record::genotypes::sample::value::Array::String(
                                        array_val,
                                    ) => match format_def.number() {
                                        noodles::vcf::header::Number::Count(0 | 1) => {
                                            unreachable!(
                                                "Field {} declared as single value but found array",
                                                key_name
                                            )
                                        },
                                        noodles::vcf::header::Number::Count(_) => {
                                            column.push_vecstring(array_val)?;
                                        },
                                        noodles::vcf::header::Number::A => {
                                            column.push_string(
                                                array_val.get(alt_id).unwrap().clone().unwrap(),
                                            );
                                        },
                                        noodles::vcf::header::Number::R => {
                                            //TODO: Use push_fixed_size_string
                                            column.push_vecstring(vec![
                                                Some(array_val.first().unwrap().clone().unwrap()),
                                                Some(array_val.get(alt_id + 1).unwrap().clone().unwrap()),
                                            ])?;
                                        },
                                        noodles::vcf::header::Number::G => {
                                            if array_val.len()
                                            == (allele_count * (allele_count + 1) / 2)
                                            {
                                                column.push_vecstring(vec![
                                                    array_val.first().unwrap().clone(),
                                                    array_val
                                                        .get((alt_id * alt_id + 3 * alt_id + 2) / 2)
                                                        .unwrap()
                                                        .clone(),
                                                    array_val
                                                        .get((alt_id * alt_id + 5 * alt_id + 4) / 2)
                                                        .unwrap()
                                                        .clone(),
                                                ])?;
                                            } else if array_val.len() == allele_count {
                                                column.push_vecstring(vec![
                                                    array_val.first().unwrap().clone(),
                                                    Some(".".to_string()),
                                                    array_val.get(alt_id + 1).unwrap().clone(),
                                                ])?;
                                            } else {
                                                column.push_null();
                                            }
                                        }
                                        noodles::vcf::header::Number::Unknown => {
                                            column.push_vecstring(array_val)?;
                                        }
                                    },
                                    noodles::vcf::record::genotypes::sample::value::Array::Character(
                                        array_val,
                                    ) => match format_def.number() {
                                        noodles::vcf::header::Number::Count(0 | 1) => {
                                            unreachable!(
                                                "Field {} declared as single value but found array",
                                                key_name
                                            )
                                        },
                                        noodles::vcf::header::Number::Count(_) => {
                                            column.push_vecstring(
                                                array_val
                                                    .iter()
                                                    .map(|s| s.as_ref().map(|s| s.to_string()))
                                                    .collect::<Vec<Option<String>>>(),
                                            )?;
                                        },
                                        noodles::vcf::header::Number::A => {
                                            column.push_string(
                                                (*array_val.get(alt_id).unwrap()).unwrap().to_string(),
                                            );
                                        },
                                        noodles::vcf::header::Number::R => {
                                            column.push_vecstring(vec![
                                                Some(array_val.first().unwrap().unwrap().to_string()),
                                                Some(array_val.get(alt_id + 1).unwrap().unwrap().to_string()),
                                            ])?;
                                        },
                                        noodles::vcf::header::Number::G => {
                                            if array_val.len() == (allele_count * (allele_count + 1) / 2) {
                                                column.push_vecstring(vec![
                                                    Some(array_val.first().unwrap().unwrap().to_string()),
                                                    Some(
                                                        array_val
                                                            .get((alt_id * alt_id + 3 * alt_id + 2) / 2)
                                                            .unwrap()
                                                            .unwrap()
                                                            .to_string(),
                                                    ),
                                                    Some(
                                                        array_val
                                                            .get((alt_id * alt_id + 5 * alt_id + 4) / 2)
                                                            .unwrap()
                                                            .unwrap()
                                                            .to_string(),
                                                    ),
                                                ])?;
                                            } else if array_val.len() == allele_count {
                                                column.push_vecstring(vec![
                                                    Some(array_val.first().unwrap().unwrap().to_string()),
                                                    Some(".".to_string()),
                                                    Some(
                                                        array_val.get(alt_id + 1).unwrap().unwrap().to_string(),
                                                    ),
                                                ])?;
                                            } else {
                                                column.push_null();
                                            }
                                        }
                                        ,
                                        noodles::vcf::header::Number::Unknown => {
                                            column.push_vecstring(
                                                array_val
                                                    .iter()
                                                    .map(|s| s.as_ref().map(|s| s.to_string()))
                                                    .collect::<Vec<Option<String>>>(),
                                            )?;
                                        },
                                    },

                                },
                            },
                            None => if schema.get(&key_name).is_some() {
                                match column {
                                    ColumnData::ListFloat(_) => {
                                        column.push_vecf32(vec![])?;
                                    }
                                    ColumnData::ListInt(_) => {
                                        column.push_veci32(vec![])?;
                                    }
                                    ColumnData::ListString(_) => {
                                        column.push_vecstring(vec![])?;
                                    }
                                    _ if key.to_string() == "GT" => {
                                        column.push_string("./.".to_string());
                                    }
                                    _ => column.push_null(),
                                }
                            } else {
                                unreachable!("{} should be in schema", key_name);
                            },
                        }
                    } else {
                        todo!("Understand how we could get there (the tests never did)");
                    }
                }
            }
        }
        Ok(())
    }

    ///Convert Name2Data in vector of arrow2 array
    pub fn into_arc(
        mut self,
        schema: &arrow::datatypes::Schema,
    ) -> Vec<std::sync::Arc<dyn arrow::array::Array>> {
        schema
            .fields
            .iter()
            .map(|x| self.0.remove(x.name()).unwrap().into_arc())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::schema;
    use arrow::array::ArrayBuilder;

    use super::*;

    static VCF_FILE: &[u8] = b"##fileformat=VCFv4.3
##contig=<ID=chr1,length=2147483648,species=\"random\">
##contig=<ID=23,length=2147483648,species=\"random\">
##contig=<ID=93,length=2147483648,species=\"random\">
##contig=<ID=chrMT,length=2147483648,species=\"random\">
##contig=<ID=X,length=2147483648,species=\"random\">
##contig=<ID=NC_000015.10,length=2147483648,species=\"random\">
##contig=<ID=ENA|LT795502|LT795502.1,length=2147483648,species=\"random\">
##contig=<ID=NC_016845.1,length=2147483648,species=\"random\">
##contig=<ID=YAR028W,length=2147483648,species=\"random\">
##contig=<ID=1,length=2147483648,species=\"random\">
##FILTER=<ID=Filter_0,Description=\"generated vcf filter field\">
##FILTER=<ID=Filter_1,Description=\"generated vcf filter field\">
##INFO=<ID=info_Integer_1,Number=1,Type=Integer,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Integer_2,Number=2,Type=Integer,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Integer_A,Number=A,Type=Integer,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Integer_R,Number=R,Type=Integer,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Integer_G,Number=G,Type=Integer,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Integer_.,Number=.,Type=Integer,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Float_1,Number=1,Type=Float,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Float_2,Number=2,Type=Float,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Float_A,Number=A,Type=Float,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Float_R,Number=R,Type=Float,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Float_G,Number=G,Type=Float,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Float_.,Number=.,Type=Float,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Flag_0,Number=0,Type=Flag,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Character_1,Number=1,Type=Character,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Character_2,Number=2,Type=Character,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Character_A,Number=A,Type=Character,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Character_R,Number=R,Type=Character,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Character_G,Number=G,Type=Character,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_Character_.,Number=.,Type=Character,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_String_1,Number=1,Type=String,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_String_2,Number=2,Type=String,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_String_A,Number=A,Type=String,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_String_R,Number=R,Type=String,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_String_G,Number=G,Type=String,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##INFO=<ID=info_String_.,Number=.,Type=String,Description=\"generated vcf info field\",Source=\"biotest\",Version=\"0.1.0\">
##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">
##FORMAT=<ID=format_Integer_1,Number=1,Type=Integer,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Integer_2,Number=2,Type=Integer,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Integer_A,Number=A,Type=Integer,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Integer_R,Number=R,Type=Integer,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Integer_G,Number=G,Type=Integer,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Integer_.,Number=.,Type=Integer,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Float_1,Number=1,Type=Float,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Float_2,Number=2,Type=Float,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Float_A,Number=A,Type=Float,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Float_R,Number=R,Type=Float,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Float_G,Number=G,Type=Float,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Float_.,Number=.,Type=Float,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Character_1,Number=1,Type=Character,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Character_2,Number=2,Type=Character,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Character_A,Number=A,Type=Character,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Character_R,Number=R,Type=Character,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Character_G,Number=G,Type=Character,Description=\"generated vcf info field\">
##FORMAT=<ID=format_Character_.,Number=.,Type=Character,Description=\"generated vcf info field\">
##FORMAT=<ID=format_String_1,Number=1,Type=String,Description=\"generated vcf info field\">
##FORMAT=<ID=format_String_2,Number=2,Type=String,Description=\"generated vcf info field\">
##FORMAT=<ID=format_String_A,Number=A,Type=String,Description=\"generated vcf info field\">
##FORMAT=<ID=format_String_R,Number=R,Type=String,Description=\"generated vcf info field\">
##FORMAT=<ID=format_String_G,Number=G,Type=String,Description=\"generated vcf info field\">
##FORMAT=<ID=format_String_.,Number=.,Type=String,Description=\"generated vcf info field\">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	sample_0	sample_1
YAR028W	509242864	.	a	ATg	6	Filter_0	info_Integer_1=-1867486102;info_Integer_2=1180908493,1041698941;info_Integer_A=-207506013;info_Integer_R=-1221871784,-1356802777;info_Integer_G=-496257853,2127853583,-1498117417,-45419278,1783408501;info_Integer_.=2082620030,-344161839,-1022296779,-1007334133;info_Float_1=68.286865;info_Float_2=-96.154594,-23.433853;info_Float_A=-48.782158;info_Float_R=-46.15216,-92.639305;info_Float_G=-7.5115204,74.78337,1.5983124,-8.867523,77.741455;info_Float_.=26.825455;Flag_0info_Character_1=i;info_Character_2=r,[;info_Character_A=g;info_Character_R=M,D;info_Character_G=h,w,\\,v,o;info_Character_.=G;info_String_1=p]ZoXMTgQo;info_String_2=uVGn`JweVD,DUYytzAny[;info_String_A=_POshsqbSj;info_String_R=AdbZcRFrrQ,_[VS^RtSvz;info_String_G=MeTjonYVIn,jLIi`oWogn,tTH\\QXXOiA,LJLnuPtf`S,r^aaSswsvY;info_String_.=CzkT\\Wk_sG	GT:format_Integer_1:format_Integer_2:format_Integer_A:format_Integer_R:format_Integer_G:format_Integer_.:format_Float_1:format_Float_2:format_Float_A:format_Float_R:format_Float_G:format_Float_.:format_Character_1:format_Character_2:format_Character_A:format_Character_R:format_Character_G:format_Character_.:format_String_1:format_String_2:format_String_A:format_String_R:format_String_G:format_String_.	0/1:-1876597949:706761235,-251943823:394859496:-1947058767,424473864:1331697703,-73747609,1645597043,-1553292366,-1685240226:300184417:18.381859:55.763123,-25.909782:-23.853012:-65.84661,-26.444412:12.577988,-87.76228,-3.4822464,-95.66553,55.56636:-35.16729,6.755356:H:Y,N:m:[,Q:B,C,g,L,`:c,x:xXYm`NnOG[:K`QKgogYxZ,uNAMyDqpgZ:liSmUzRvGG:XBgqxa[aBw,_ZxxkAFA[o:`OIdJgjZDS,tKauvtaIhw,mmrIgNXcbh,Rd]QWyFOgu,kSjBlBKigq:znOIm[gGXi,[j\\RlwOmAi	0/1:1178180247:-284426189,-998625419:1871179132:2059063854,-2098693212:1608185708,-1406134851,1030174330,-2031052594,1598302707:-419749875,-1478145995,-1699207585,-1247215944:-58.38821:-62.55126,8.762314:-74.02904:-24.794365,46.083145:13.760803,33.24704,-86.315704,60.576385,-14.547348:82.95245,46.642517,90.124435:a:r,Z:o:y,T:n,Q,F,E,n:m,n:aDWuppugIL:wOFhYRxBZH,MqOWyQIIAH:u\\QQqQyZ`t:TnZk`XSq\\I,_HmAWXBIAy:CL_`ebjENF,E`pNSPd^wz,^tZVmq_oBY,JgQ`oPn^Z\\,`bla^yzIWt:gmoGx]WbcW,VPniuT_IlS,skBLwLHlF_,fwwGspJRS\\	0/1:246980022:-1016832924,1861844708:-8173468:-1069804542,-70068572:-1451768444,-1682870970,-1829205528,-2068943681,363393119:-288960163,1831626585,1958104113:-80.60921:43.23416,-74.28625:-79.06761:53.03195,8.447456:-14.780685,-46.596863,61.897903,29.243942,-69.91906:64.31647:I:P,N:e:v,h:O,T,N,v,r:w:SEWbLtHSUi:CnsIsSMCBy,^pRIQ\\eLD]:QRzYyzV_sz:wqgYJ`TzLK,hHWZiobiKn:dAPiptpPRU,QyBPeNqLaR,rPFJcjVaEr,HHloMTrcoG,yzgqiA_WIL:`ot_PZwl^\\,Uz^rcVndZg,_IpyMneGSa
23	1165400956	.	T	t	199	.	info_Integer_1=-597222189;info_Integer_2=446843965,1432841503;info_Integer_A=-1756403175;info_Integer_R=-1210584642,1067164582;info_Integer_G=-2026752623,1524204480,2063402043,-1671581234,1992411203;info_Integer_.=387204105,-2048329790;info_Float_1=29.60765;info_Float_2=-70.24462,91.82048;info_Float_A=-57.780792;info_Float_R=-19.511703,87.46164;info_Float_G=17.362617,-10.059616,-89.640594,-70.55726,-48.635937;info_Float_.=82.884,-31.403328,-83.54941,-54.887726;Flag_0info_Character_1=];info_Character_2=Y,R;info_Character_A=p;info_Character_R=A,W;info_Character_G=i,y,I,q,i;info_Character_.=w;info_String_1=]gp_[s]vDh;info_String_2=Y\\SmynkIV^,tOuGkqHsiE;info_String_A=QDdbnppEhM;info_String_R=VgQkWCCgEH,r^aAgT^sOf;info_String_G=z\\_iwMGBRH,EQy^RJwkWd,gu]hpIwaVj,iwKORqBPP[,_ShIZ^Mr\\P;info_String_.=zxs[sGNNuy,cmnjXNUPka,QaFrhEZaIB,_TjXJMdWCM	GT:format_Integer_1:format_Integer_2:format_Integer_A:format_Integer_R:format_Integer_G:format_Integer_.:format_Float_1:format_Float_2:format_Float_A:format_Float_R:format_Float_G:format_Float_.:format_Character_1:format_Character_2:format_Character_A:format_Character_R:format_Character_G:format_Character_.:format_String_1:format_String_2:format_String_A:format_String_R:format_String_G:format_String_.	0/1:-871261733:-1500509753,2025272017:1864754769:-1127684339,-447878996:-1851298122,1367475939,1988967275,-439362500,-447904679:-907504720:-73.56331:-86.60319,-31.910011:6.785797:-95.413086,19.286415:9.942863,-23.623634,31.06224,42.57071,92.734314:-51.402973,-25.126984,73.030045:J:K,Y:K:c,^:y,P,V,b,S:U,w,M,X:IKZ]ZMDszw:apRf\\BVTcU,UOJHFcgkaj:fIjt]RZCsd:TtoRPBHoRS,sDF^wkt`MK:boQ]OQxmec,eJfBqcdaUg,To]BkSYKbI,J\\qQxtjZBq,\\nQWJTeYEf:tHujPdNde\\,EWFfR`mig_	0/1:205411993:1422316187,136922489:-1998113238:-1581743308,-1016113531:234539080,48396474,1428303612,1012371357,-608258082:807181400:-48.021435:56.736588,-5.4926376:2.772995:6.886032,49.36194:72.34972,19.888977,96.9234,-52.7704,-20.327469:-86.68194,-95.58513,40.37178:r:[,s:r:i,g:p,S,t,_,r:I,N,Z,_:RioGPuuW[_:ZM`VMm[mpf,dTaWj\\c`Pm:bjEzGJcxUg:Iaec]IAHYe,yWTlfKaF]e:JYeMoCeFYn,lwAXvGHCdL,yzGwQfB_YF,lpaF[kfilC,xgYHiD]pz`:W[nVSvJsm_,hxR`xIkWis,dNZNX`_eku	0/1:249006189:-1550023525,1431034968:364065807:630293442,-1899991908:1343119655,1148049825,1254870322,-805282094,-913065710:1033469396,811475314,-784376229,1101867431:-84.87093:84.413025,-28.448128:-35.850975:98.89691,85.76083:-7.8424683,-39.988495,-94.08006,-6.4476013,48.44284:90.68361,-75.537186:P:`,g:j:f,t:M,t,l,u,G:D,^:xYp\\]fYNUh:vzefhZ_x]E,FqtKVH]Xvn:Z\\zIZeOkBf:hYfZzwqLzB,drm^rQSCM\\:wEaPIq`oJt,ZfNBcc[uV_,`pF`wSolGO,BI\\aL`htut,^OEbgPyBTf:fEql_^pktX
ENA|LT795502|LT795502.1	525786811	.	A	ATgA	226	.	info_Integer_1=1004917273;info_Integer_2=-1087925856,-1111801609;info_Integer_A=1142924498;info_Integer_R=397636772,575245484;info_Integer_G=631457844,1508219739,2060178753,1508815851,-1692774727;info_Integer_.=915360277;info_Float_1=81.456085;info_Float_2=-97.36381,99.07503;info_Float_A=-17.968132;info_Float_R=23.030853,17.895386;info_Float_G=36.786133,-36.816742,-79.92742,13.375832,-41.70673;info_Float_.=-56.670807,-88.687706;info_Character_1=x;info_Character_2=f,X;info_Character_A=Q;info_Character_R=M,v;info_Character_G=`,B,I,K,K;info_Character_.=s,S,a,E;info_String_1=RULoNvUdVj;info_String_2=YlKPytYpDY,hwIe\\Lokil;info_String_A=\\VZSHlparH;info_String_R=PVoBxilKPl,_s`t`swTzf;info_String_G=FiINYvUJIO,LtzFxYFFJp,mMeZaQtSZU,rFHUKkG]FO,LxQTXnzEJr;info_String_.=hoXwhfvniY,A\\txGAVbNp,SbcLeVnkYI	GT:format_Integer_1:format_Integer_2:format_Integer_A:format_Integer_R:format_Integer_G:format_Integer_.:format_Float_1:format_Float_2:format_Float_A:format_Float_R:format_Float_G:format_Float_.:format_Character_1:format_Character_2:format_Character_A:format_Character_R:format_Character_G:format_Character_.:format_String_1:format_String_2:format_String_A:format_String_R:format_String_G:format_String_.	0/1:-1992801692:305092475,-1179215612:-1739296736:-388696167,-659498070:1503808064,-183677921,-849522112,-1806185994,-1784485194:-498574773:14.566895:10.018013,-16.879608:99.71886:0.29120636,-9.131622:80.328705,-26.403976,-37.213943,57.87976,61.69644:58.0925,-70.47134,-59.34813:X:N,q:S:a,_:c,W,U,b,Y:m:^QkYoWratI:ryX^JlAyCx,WGZntpNsOo:FRryqZFoMj:gCO^BOI[ml,VJqiy[VWym:nbtqw^\\zmA,ZiBBJm[Vbv,aNMll`xnfr,nIJf`wjzny,i[qz[mHs_N:rHzB`UssLW,apsPd_lrip,Uih`ROsUql,tnBQQdhtwm	0/1:-24901120:240741600,-335142169:1743578406:-898920674,473452936:266099587,818869222,-1461529615,1643094296,-2054606423:641472069,-1850726656,-386681464,266081312:97.276596:48.907135,-99.147675:-17.34481:36.995285,-15.711685:-74.65256,-87.14964,0.5836029,36.94156,-83.004906:90.52608,5.7993164:b:`,L:w:l,M:G,m,s,V,n:i,p:ZMBpgnUaaU:[fe_qYtEAS,HcYIbeHcgz:kanq^dgO[M:`v^vmOCeeJ,XRDAZIQsWC:czkmbtrEXT,JHm^^jZjSZ,MrQ[yeKALl,hoEeih^Nvf,MYQGSi^Zux:XI_a[M^G[_,czNvmEmcXT,zwNxaRelTy,\\UHxM[KJGU	0/1:60157897:319490021,-742515096:-1289964762:1628004982,235029603:2020442014,308460461,1558271982,1627368865,894042318:1994633465,-1579924515:-10.664841:-62.51185,53.523605:94.14816:2.9510727,44.83983:-5.713913,-74.449394,-56.378246,-46.97802,13.483833:5.34816,65.6853,-95.33172,-58.07209:U:F,W:\\:A,E:t,j,c,F,J:H,R,S,^:`ZVfoyxdDK:gntf_rQo]a,mHMNJLO[`K:]PjxDCRfYV:MMGtGvm[wr,eQeumfsRZL:EqInOsdeDW,xOQBKswphI,nU_PiY]xef,cAHdxvRbFC,kvJ[v^kdcb:fyKXA\\hjfv
ENA|LT795502|LT795502.1	1506498921	.	A	gT	99	Filter_0	info_Integer_1=1074860489;info_Integer_2=-6784655,1952022752;info_Integer_A=-1765522773;info_Integer_R=1316333577,-554518728;info_Integer_G=-440746192,417172829,1208578807,-1256320970,168283749;info_Integer_.=-67150747,-701563860,1708267257;info_Float_1=-85.47166;info_Float_2=33.09308,37.761444;info_Float_A=99.544266;info_Float_R=-4.276779,27.070168;info_Float_G=-93.02027,-68.755196,-18.597626,-82.3945,94.890884;info_Float_.=45.63858;info_Character_1=R;info_Character_2=B,W;info_Character_A=l;info_Character_R=z,E;info_Character_G=Z,h,F,D,D;info_Character_.=n,G,v;info_String_1=ojZkSfujYX;info_String_2=`ZrZJtq_hx,StcGnLjWNS;info_String_A=k[feQ[mqyE;info_String_R=Grr[rGo^md,GkiXanc\\K\\;info_String_G=Yhij\\pOPji,yYlsCnJSCY,VggsEuC]ad,G^jiYbvsbn,IJmJvG`jzs;info_String_.=CKtxVQFr]_,G^jAnQaGyI,yvzleXG`vO	GT:format_Integer_1:format_Integer_2:format_Integer_A:format_Integer_R:format_Integer_G:format_Integer_.:format_Float_1:format_Float_2:format_Float_A:format_Float_R:format_Float_G:format_Float_.:format_Character_1:format_Character_2:format_Character_A:format_Character_R:format_Character_G:format_Character_.:format_String_1:format_String_2:format_String_A:format_String_R:format_String_G:format_String_.	0/1:-2039921838:1260782784,-2144365597:1110295788:-158846729,1495837063:390766793,1114219927,-790406568,1652554877,-1144133980:-610126444,1736640977:0.7073364:-56.6103,13.725807:-8.711052:-35.14733,55.534668:-41.20214,-69.47873,15.234543,1.8139114,-81.88782:-86.727234:]:f,Q:I:u,_:b,k,C,[,T:b,S,O:hGS[GPZUZu:uQuJZtwYq_,SyDvF`v_[[:ol]TtBXxVP:mFMNUVM`Ir,XbBPeoBkYj:`xGS_`zgey,v\\]bxaFPdJ,lGqnBWyHQI,ynpDFGuSsm,^bGxsDdgIl:q`HG\\bwqSl,GrnuUSzgVy,cRSPjljk_Q,TWbW]MISyd	0/1:925068425:1074963297,820496013:2032912248:-456701844,1354651711:-1215180367,1123368027,-680845673,-332079579,604760814:476241956:-25.982353:-75.11304,97.80142:62.201385:39.84816,-3.5477142:-50.861835,-43.965935,-45.22519,44.636627,64.44443:-27.258255,-56.71892,81.974884:D:g,j:o:C,D:T,v,A,[,r:c,L,I,I:OJBBSJbN`T:DdPsffHJuV,AJInhMhoiR:__FbESuepO:SDF_mGG^JG,YFfWtuVWFc:ObAtvWdiHC,nRn[JyLGrn,AvnzUN`iJP,BD\\thZTCSk,SuJCDkzPGU:U]SgYvoNeJ	0/1:180052409:3916924,-608184065:406358148:1618596409,-143985416:781007994,-768878726,1593943437,-803117731,-914254344:446901758,117973804,20424962:-9.037872:-71.62204,-38.234306:-38.99839:-78.47328,-74.93701:57.11064,53.769928,22.832726,-1.6777267,96.80972:77.72031,85.200424:J:Z,Y:S:V,u:O,],w,n,Q:Z:Hesgu[drFG:pWRnQtXSiX,D`cgIgjITG:D[lL^EIPfl:AOXfKDsetT,GbyjVXojJF:AcGyoIohdU,zIRGXEkwRv,JeuIkD^`cs,MIpZxRusP_,DvXBProTn_:X[sDTJasXv,[nehoK`q^y
chrMT	900574305	.	c	a	208	Filter_0	info_Integer_1=-523627641;info_Integer_2=828853617,538841733;info_Integer_A=-1070289656;info_Integer_R=1177092376,1248528320;info_Integer_G=1338006213,-939491184,2031520519,1625981257,-1542813010;info_Integer_.=1020802949,-325766450,-1975174725;info_Float_1=86.21178;info_Float_2=55.274292,57.992126;info_Float_A=98.6465;info_Float_R=-56.676746,-78.452255;info_Float_G=-15.1058655,-32.05681,-23.85817,25.53675,-44.79227;info_Float_.=-8.955811,49.62912,72.4005;Flag_0info_Character_1=x;info_Character_2=B,r;info_Character_A=i;info_Character_R=\\,D;info_Character_G=g,v,l,W,T;info_Character_.=g;info_String_1=j`zU\\K`PLc;info_String_2=l_rhk[Kr]b,rFB_aSUBR`;info_String_A=ZQYIsGAof_;info_String_R=HwrUBliWel,scCvWxgE[r;info_String_G=kY`HMgmH_p,IUvLZ]sdba,nVQJ_Fh`ET,QnYUFz`ShS,KXwJfcYmsw;info_String_.=Q]wexXkHyr,fL\\NGDMlkW,jVpWnME[tp,Zz\\hrFyx`]	GT:format_Integer_1:format_Integer_2:format_Integer_A:format_Integer_R:format_Integer_G:format_Integer_.:format_Float_1:format_Float_2:format_Float_A:format_Float_R:format_Float_G:format_Float_.:format_Character_1:format_Character_2:format_Character_A:format_Character_R:format_Character_G:format_Character_.:format_String_1:format_String_2:format_String_A:format_String_R:format_String_G:format_String_.	0/1:-1648034443:845207498,1438334805:-821666363:-532302872,-784878946:-1660896800,2008926111,1279825538,-233248668,-1578146061:-1728381686,-1354962949,-2095339305:4.250908:1.8742523,-78.206894:-22.634148:-61.518906,13.15873:-64.15179,17.086502,-25.609276,57.059555,-92.1911:26.885536,-27.22528,-51.00875:F:l,e:M:y,e:^,X,g,x,h:K:LXIC\\KLZsD:Nd[vIMiHJA,^ConDldYtT:CBqgBJnzRq:nOgdeNbqKd,rTYmYwJcQu:WMlQs]gO[a,Kk[sJ[UoxT,HT]XWH^ZTF,IIJXSmrLHg,qo`OJ_hgav:tSEKSQWXRL	0/1:-1351553081:219969512,1955232553:917524488:1134530757,-362836542:-2127154133,-1470782646,-1443121280,488596267,-1560382672:-1061588830,843145750,978368685:67.93184:-0.71754456,86.030624:-60.117985:36.45627,-81.18977:53.986176,92.98938,-65.36742,7.6014786,46.42549:27.035995,-57.711315,99.20256:M:E,b:k:v,o:\\,y,j,N,J:n,J,_:EvnT^AAXoY:cdVJ^MhXbh,y[[LcBZavI:f^EAgXrRhj:A`FeAFUqKi,fsOnJ\\kmaj:JyK`vg^yGK,G`eXThODuq,gRMJ`naYA^,WkywfqqL`h,ItKK^GJvpU:brd_fl`zxc	0/1:-334893903:1812791387,-1702573904:91913024:-1715303171,1720214253:-1065363642,-1781482473,1593677428,-1611378854,-1463000308:-918589861,802242226,-512257664:29.155685:36.117004,53.34468:-73.653984:-47.224236,80.070786:26.598068,-14.040497,75.17987,-24.15435,-7.163788:-31.709908,-72.07062,53.230316,-11.984253:f:c,r:F:Y,B:m,Q,O,J,S:e,x,V:wsZzDGtnRs:rr^refYy\\D,xWTSt]bRdz:pBWEGavAaK:ROjxoClYNb,ppopUzLGgP:LCytHxxfrF,dGG^fvX^iR,uqkLhoqbuy,yFAbgESl[^,SlKPAuDoaN:hbu_MpWRtk,kJqJRFtD`K
X	508903144	.	T	taAG	107	Filter_1	info_Integer_1=-442012684;info_Integer_2=1242798393,-893635990;info_Integer_A=-1049853993;info_Integer_R=242988245,-245551581;info_Integer_G=992362638,-556141956,-1436766801,1237135939,-1164555077;info_Integer_.=-1890267838;info_Float_1=50.181763;info_Float_2=5.1533203,32.221054;info_Float_A=31.930801;info_Float_R=18.487122,-4.3887863;info_Float_G=-98.10066,-69.57614,91.27092,60.39116,13.878372;info_Float_.=-46.815468,-65.40532;info_Character_1=_;info_Character_2=C,c;info_Character_A=w;info_Character_R=v,g;info_Character_G=v,Q,r,`,W;info_Character_.=\\,j,W;info_String_1=Ol_Gd[f\\tt;info_String_2=DX_Nqhsbft,ZvYZmhftHw;info_String_A=bXt^\\wzwfQ;info_String_R=ANOP[Zjcef,[mLDzYe^Xa;info_String_G=i_XoxRH]Up,N\\qKskBEfm,vceQjVrtTu,_LnQ_[ngn],yd[ZFNmECq;info_String_.=FPpMF]TI[C,skPWfxtNBS,uEV]cCazM[	GT:format_Integer_1:format_Integer_2:format_Integer_A:format_Integer_R:format_Integer_G:format_Integer_.:format_Float_1:format_Float_2:format_Float_A:format_Float_R:format_Float_G:format_Float_.:format_Character_1:format_Character_2:format_Character_A:format_Character_R:format_Character_G:format_Character_.:format_String_1:format_String_2:format_String_A:format_String_R:format_String_G:format_String_.	0/1:-1784107859:-939663,-275551415:1120891123:-769476243,636006815:-1200543946,215752478,-1484326861,-561668774,-176043456:1567908440,-504633662,-461948338:41.60312:89.26175,-28.585121:-25.16079:-93.47296,-97.84482:-64.203835,62.569122,-16.536377,24.606033,-83.769844:-0.48690033,50.683716,-61.86049:u:p,x:G:O,q:J,g,s,K,g:c,l,a,w:mgLADffOAW:OFYzsNT]DN,knpK_\\ZlLw:NIGi]zaPz\\:vcAptCt\\VT,z[VAZFjS[p:tcGuXLiEpv,fCLE[^Bzbh,rkGwaRnhxi,fTMvHtFRwN,RPYaXfd^BF:DOnEKUN[]V,ppBNNWrhhK	0/1:1910075420:-934542610,-878954512:1171320700:1208017031,-340691680:712157209,1065873060,-1366658844,942622778,-783364205:1637019880,-955981111,1346196180,1597225767:-26.175598:93.89627,-80.47638:-29.046753:-60.67419,85.3492:-41.516758,-69.87106,22.452782,21.140648,61.20903:-78.91676,26.818916,-59.74772,-27.64888:k:n,]:U:T,X:B,r,c,R,G:Z:yGqzsPBWTl:AyQbJXbTNE,wQIyxKzeXn:F\\_wSzKzz_:GPJEnUVNAi,OGt^nVFblY:Npi`aeQSMU,_lhlMXNtTB,[QXjBWuj]d,rKRDMyolVK,QciXXonSdu:ycSYShz\\Gk,\\hYbIs`WDD,CfueEstMtZ	0/1:323372379:1922884232,266253196:-2038955647:-206847340,703053779:740621164,1717070470,2056797316,-1709077983,174222777:-229529326,1150748796:12.667084:-49.801563,43.236115:-82.59476:73.92726,70.36862:16.87722,-79.93007,70.9064,95.77089,-38.30352:86.8264:O:D,k:V:j,u:i,I,z,_,J:u:HEuDirTQc]:[`cTQh_wUo,gJqeDnKyu`:TexBlzobtU:YTlxHhXaRn,DAIwrYQqxu:LK`xEnzWq[,zZmLwxn\\m`,rujYx[cES^,ELVaIdJFDY,Z`Fuq`aaii:LTi`XfaE^C,jgR\\cCazHR,v_dFUwq^p^,wrsXkkAV\\I
23	2057099842	.	g	a	105	.	info_Integer_1=-1463169227;info_Integer_2=1864557327,-1832965500;info_Integer_A=-1397690738;info_Integer_R=230018794,906575350;info_Integer_G=1606595129,-440932389,310954072,-1735028992,-71170678;info_Integer_.=-161655144,935253047,9786785;info_Float_1=-99.35045;info_Float_2=-42.93885,-82.69522;info_Float_A=83.79291;info_Float_R=-85.69043,-11.00209;info_Float_G=-60.064484,-27.798103,94.52054,-38.2653,3.591034;info_Float_.=70.61771,-63.402893,30.473663;Flag_0info_Character_1=g;info_Character_2=V,g;info_Character_A=V;info_Character_R=h,j;info_Character_G=G,R,J,\\,F;info_Character_.=`;info_String_1=ukwTtXgA^`;info_String_2=o_mXjM^xZX,WgiO^rhwGP;info_String_A=QGOHr_eINM;info_String_R=JvS[necmAA,BnmyIEEpWY;info_String_G=EPGPQVsCly,YXuaHkTy\\r,l\\kp_Z_cmw,yk`oYPWJTC,jPmpS\\c\\[i;info_String_.=pqZPLvIWXJ,u_QaCDUzqA,dNRpmBctzH	GT:format_Integer_1:format_Integer_2:format_Integer_A:format_Integer_R:format_Integer_G:format_Integer_.:format_Float_1:format_Float_2:format_Float_A:format_Float_R:format_Float_G:format_Float_.:format_Character_1:format_Character_2:format_Character_A:format_Character_R:format_Character_G:format_Character_.:format_String_1:format_String_2:format_String_A:format_String_R:format_String_G:format_String_.	0/1:883085881:-1163194169,396734570:-864830302:-1015935718,416512239:1055659899,378650980,1719415308,-591094434,-602013467:1562372336,38255854:-96.96286:-22.160027,2.9108505:-76.4101:-31.626724,74.70122:-10.465622,92.86656,66.5076,-47.610092,73.46921:-35.273743,71.3008,-21.861176:N:Z,x:w:D,Q:g,[,i,i,f:b,u,q:rOrdrrnAAf:fkbpBSUynu,ks[PFA_asT:JK\\[phzgTx:sIMyglMmy[,zdTHmmDXr`:mJZqxRqayy,nzIi[BVIeR,TJX_tcRRuR,nwx\\LjKCPX,EMgDVxWAhQ:urxfXYFYWh,yxg_KbMfiV,O^NfHdYiTT,RQLQTrA`[D	0/1:712995789:-341947022,602595428:496080894:-317586736,-1599924675:1901874239,-1170280909,-1426570445,1489049109,-2000710204:448535337,-1814547565:99.9409:73.24715,-61.679005:42.36447:-67.20648,-37.822533:-44.426228,0.8399048,61.94937,67.76169,-47.838783:96.54474:I:l,g:W:f,T:C,j,],k,a:y,G,Q:nuKQNGtG`H:rk_WztmXlA,rCVaSXG`vY:XNJj\\lgAgb:qKSozwOYIa,K\\rGODc_`q:i_rCsVxkls,YErisA[XyI,nDyzaAV^te,NlxAGcHuIw,CYzN[Gykcx:bDyCEM_Ntk,hVlhY\\KH\\V,TyRCuSB`sn,XEuXmmuCrD	0/1:-1170229269:-1356256190,225279207:869161357:771625610,-569908878:293513696,-619213311,-1755999259,1604615807,1087899712:-745470529,2144376132,-1224677810:-28.347206:-60.524513,-69.2384:-86.569954:-58.52585,-74.32852:-59.802223,77.79262,69.73915,72.24533,-7.9125137:34.921783:N:h,i:u:i,l:L,g,L,B,g:S,c:^y[zODvjKw:NpMKSd_yN],vkeZUtFWRR:oqiWbtGZPN:ItYyF]PgmM,e\\qT_MdBPH:p[n_BBNiqU,my^WxdBmGo,RB__ZCtWd\\,bx_t`szKbQ,qQuZ`\\jq[C:LzEcfVrmKJ,\\QgmcLzgxq
NC_000015.10	278483743	.	c	Gc	68	Filter_0	info_Integer_1=-1771300013;info_Integer_2=611485162,1796725452;info_Integer_A=971438374;info_Integer_R=698255143,905472298;info_Integer_G=-200904731,1733482657,-1601571925,-95180709,852757134;info_Integer_.=1226331110,1800309665;info_Float_1=-8.41539;info_Float_2=-91.844246,33.56476;info_Float_A=-70.37154;info_Float_R=-88.31048,49.067856;info_Float_G=-56.826736,-72.017075,58.757156,-87.95636,27.40886;info_Float_.=-93.33689,-11.933228;info_Character_1=V;info_Character_2=F,H;info_Character_A=];info_Character_R=],k;info_Character_G=b,j,c,v,k;info_Character_.=W,J,`;info_String_1=[sg[NfQUjS;info_String_2=tuIDx]qY`n,sYihzjCcDX;info_String_A=rWeIJoLqif;info_String_R=Lc\\nOEn`SI,ohc]_`UFau;info_String_G=ts\\z[cGhVY,FWhsZospCl,EibJWC`AtQ,mNRPCdUKvw,lVzUvcofUf;info_String_.=ivsywIpK\\E,dw[sIibpcF,ngkSonUgLJ,OXEnvoSKPb	GT:format_Integer_1:format_Integer_2:format_Integer_A:format_Integer_R:format_Integer_G:format_Integer_.:format_Float_1:format_Float_2:format_Float_A:format_Float_R:format_Float_G:format_Float_.:format_Character_1:format_Character_2:format_Character_A:format_Character_R:format_Character_G:format_Character_.:format_String_1:format_String_2:format_String_A:format_String_R:format_String_G:format_String_.	0/1:1053018922:-184938975,-1760026633:-857964358:-2102716665,1815665606:-1175872690,-347280558,1231790968,-790356303,-222139945:-1345714777,160079922:-82.493515:64.684265,-99.60785:-19.693016:-3.5498886,-99.20609:-5.4513702,-81.74608,-6.946541,-87.49165,-31.873795:-0.010063171:x:n,^:R:b,h:h,c,],\\,X:I:LWSccGJLM_:BFr_AeHgBF,HkaUOy`jqz:FGMQKwoeFA:^WCLcfxttz,YAipFMM\\Sa:WfUeg^ehSy,Rn^uqvYSmS,sEKSkLrC\\p,QDmf[JGzbG,QiZIx`^pZN:Kmi[\\ChDrN,OJdBvVq[Af	0/1:1914159851:1114283704,-2014859484:1560783535:1865312897,256404857:863585279,-267380020,-134015412,856986131,1668297008:-1579607045,1788206035:91.887955:-27.877113,-57.696247:-67.211655:-94.020226,87.17949:96.09091,-27.167084,99.71431,-84.021736,20.75412:50.480682:r:e,q:w:t,E:R,V,i,I,k:o:jO[Cax\\_zE:beNr\\DPEAj,[rxvLKUNpg:Em^Y^JfmRt:I_pzdiZKvX,KNhZiCPeWc:^rxclcxiEF,bUblZrRMet,KXXitlqZ^r,tDrkWBjrjr,IybJjtMXvv:Qavvd`qH]Z,lXvCHRinb`,EYSpZQdvya	0/1:2112335424:1117304177,1186842567:1518339830:1538701932,1525880826:1156301305,293295010,829335070,-1308097481,567438411:-876069694:-71.12594:80.75633,-92.32674:-51.611877:-83.43153,2.3728333:-16.384102,18.902657,34.962082,26.661896,29.297455:6.421852,-9.252121:u:R,F:\\:],v:A,B,y,p,c:v:miPNAjQGGF:`cKYG[lHBt,wbp^hin^Rk:^kE[kkegww:\\gzDFBIToj,SR]K\\pHId`:KuB]TPhgby,XbLVwenUxV,qxSSu^ko^v,TcVjGbYkIp,_Y`cUrzUBV:dc[XIJDWB_,R_YE]uzONR
NC_016845.1	1273217582	.	A	cA	185	Filter_1	info_Integer_1=1702504238;info_Integer_2=-1300020074,-1771363986;info_Integer_A=-666582393;info_Integer_R=-1483769984,-1241578554;info_Integer_G=1976807172,-1260807615,-108510257,1277543943,1016305186;info_Integer_.=1829528682,-928482172,-429726805,-2007283327;info_Float_1=70.20416;info_Float_2=94.22778,49.014664;info_Float_A=77.67261;info_Float_R=69.01376,-85.50122;info_Float_G=22.049858,-31.612656,67.47859,42.012314,-0.50154114;info_Float_.=-79.06835,36.144714,-11.66687,-33.392593;Flag_0info_Character_1=Y;info_Character_2=s,w;info_Character_A=Y;info_Character_R=E,S;info_Character_G=J,C,C,^,N;info_Character_.=s;info_String_1=RnqfrhRxGK;info_String_2=QCczZsqSMX,UadliszTvD;info_String_A=UeAmTxgIJs;info_String_R=RpC[yfli[m,UMXScERIAT;info_String_G=Piyg[YSyn],wrVNLOsrsd,CuaYlZzSG`,hHfxMnZBYb,fELmQbwhQV;info_String_.=MHfN_MSgEe,_REWJxavTD,SkkKdTmDLG,R[oCTWMP\\K	GT:format_Integer_1:format_Integer_2:format_Integer_A:format_Integer_R:format_Integer_G:format_Integer_.:format_Float_1:format_Float_2:format_Float_A:format_Float_R:format_Float_G:format_Float_.:format_Character_1:format_Character_2:format_Character_A:format_Character_R:format_Character_G:format_Character_.:format_String_1:format_String_2:format_String_A:format_String_R:format_String_G:format_String_.	0/1:-1879771452:-2024174827,895533931:25454566:-1413372025,-1730678484:1651476894,522323445,-801323168,-692607812,1081910993:-1359195475:64.78084:98.23514,-95.097374:97.43535:-65.953636,7.431885:74.098724,-84.2887,-56.68762,-86.44216,96.5443:32.076004:K:G,\\:G:W,z:h,o,j,X,Y:e,G,x:nbjlHj^`q]:aa^uI^^SoQ,JyJ\\ARpaJg:MniIYiZryL:qDr]fJV]eR,iuDxSPv[oY:qLjrYY]bPA,rejiDo^By[,`mxXgnjkPa,_grwZxX`kA,\\OLO_zFEeT:xJR]YarNNn	0/1:380883717:-62474379,-1769613882:-829056637:57433667,1227514553:68322866,1729576571,953880816,-1186526990,-1862887320:-1537612724:-98.82541:24.12381,-75.1378:-59.668877:57.233078,80.90968:96.60881,-88.86273,-30.641846,21.3125,-24.467636:-39.953922,-20.188927,99.38843,-5.183769:u:l,b:v:H,D:v,V,q,f,y:L,[:I]cUryB`WS:hRJHmG[JFU,[kaZP^eEbq:X]cDdXDjYF:tDkOUi`GjU,dwC`DsgfEO:lQmOhgcIB],ZNRGbnuLAI,j]CjtQNOW\\,cyS`hBmOSp,[E[YiTVZbZ:aGTIEgKEx]	0/1:-279336022:-1084797147,1134420570:-567169692:-1283635727,-362348689:1152882561,1764606448,29946627,-1092833737,-1928697170:-1487395248,-1139333107,431473979,158203585:92.27652:-53.75874,35.170288:-12.784073:-46.073364,63.364838:-34.01854,-96.420715,-46.4582,-62.54904,-3.9217987:75.17345,-85.619095,-76.64981,66.046524:Q:Q,j:w:h,s:q,R,x,w,e:B,G,V,b:swXpP]Tmxr:S^AnbSXQXf,LQJBTUgAex:LciDfNSEuH:Ls]QTpWGBO,X[Jns_eDFr:QKqHwAUcRa,bzPOGoHhNR,Ea^NYFQRqd,ClLx\\^fCXL,\\ROGUiBQUj:styjseYDT_,aEHA[zxJlW,eLIiDr_aGn,RTsLkpJjgO
ENA|LT795502|LT795502.1	566884162	.	t	c	22	.	info_Integer_1=-63306296;info_Integer_2=1391506844,-1503768112;info_Integer_A=340548256;info_Integer_R=-1286314818,288781403;info_Integer_G=-800469678,-1311787939,-793948174,1533475939,755254594;info_Integer_.=-1341990003;info_Float_1=-76.227356;info_Float_2=-54.977512,-39.39898;info_Float_A=-35.61332;info_Float_R=-70.32056,-42.79394;info_Float_G=67.78093,43.006317,92.26671,-48.16651,4.3726654;info_Float_.=-60.336803,-45.87288,92.96947,-43.244385;info_Character_1=[;info_Character_2=c,J;info_Character_A=R;info_Character_R=o,d;info_Character_G=h,`,F,\\,q;info_Character_.=T;info_String_1=q^HZe_mW_C;info_String_2=FPSDvSVXAd,YbrjDSdRXm;info_String_A=IxDTHZYoq[;info_String_R=OsOWlbXzO\\,hAhG_b\\Ifw;info_String_G=jb^GYiHZRT,_[`_aqmUIf,PtWWNPUINQ,WkqQaaxSee,jRMUC_IYwu;info_String_.=ZVqn\\yRJEI,`vlpPiWkLZ,aVHocDfVJv	GT:format_Integer_1:format_Integer_2:format_Integer_A:format_Integer_R:format_Integer_G:format_Integer_.:format_Float_1:format_Float_2:format_Float_A:format_Float_R:format_Float_G:format_Float_.:format_Character_1:format_Character_2:format_Character_A:format_Character_R:format_Character_G:format_Character_.:format_String_1:format_String_2:format_String_A:format_String_R:format_String_G:format_String_.	0/1:389250658:-1173892904,-995837010:380428736:-350796083,-1946061625:-1985077526,-956832721,-2103216081,1213731248,-1361646347:212134446:-93.3871:73.895645,-82.49681:-59.703255:-53.21877,-11.0794525:98.62854,-40.406464,36.850067,-61.214233,28.269058:-80.0885,25.734207,92.746826,24.650955:Q:l,w:j:t,Z:K,X,a,E,S:i:VAXYF^LWPG:SudBRfeYRI,axYzALsh[m:gWvHMgghOt:cIIIEUOOnN,Q`yNRLvwIx:HeiQgtTGFY,A[RlKUJYGM,EDyo[bNg]Z,[DQbbRhs\\H,DNoj_HFJZ]:u[WuJ]OfAC,ToajkjZMqO	0/1:1247618239:1495558316,1270330192:-1812953658:2099386438,-1719636933:-1719318579,-2036965806,-1361738579,-438246128,154780382:-2087289599:99.94664:-24.0057,11.140228:-54.74951:41.22667,-17.469597:62.76808,-47.069477,-82.23286,97.09668,34.973145:-90.37955,-0.9262085,87.107376,69.280334:y:M,T:Z:Z,`:K,y,f,N,c:s:DmKvSoUTTo:zjlBpBcCYU,wEhOIx\\sXm:YYmcVdAtGt:UlifazYxMd,snMxUXwcD^:AHixNQliMD,JdpvgRsGQe,fPSRoIIRVL,AHM[ETIVla,GXPIPLRtqa:pvvKB`NrwP,lTl[KlJinZ	0/1:-1942376961:1024730712,39811746:1702586628:1394978346,832590142:1113681009,1611955235,-169392027,706295232,-1382855589:1043164434,456932470,-1198813064:74.17302:79.9146,-59.669567:-14.853073:-39.896774,-26.528046:71.47615,-73.95854,14.603233,-59.66177,-35.773087:8.042595,-81.39966:i:\\,`:s:Y,S:f,y,L,s,e:W,a,s:vLy\\C]]Bkb:cU[]e_icry,peVDqFyCOL:pmMspaUUXk:uIhBoRPWTP,^jGL[\\`Ei]:CFKAibZSAV,]jyZA_dhSu,UVV]AtIjJu,Than`WdhfE,GzqGzeGtmq:vj\\WHa^Crz,lzLRWOruNj,[V\\LWB]XnM
";

    #[test]
    fn init() {
        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap();
        let schema = schema::from_header(&header, false).unwrap();

        let mut data = Name2Data::new(10, &schema);
        let mut col_names = data.0.keys().cloned().collect::<Vec<String>>();
        col_names.sort();

        assert_eq!(
            col_names,
            vec![
                "alternate",
                "chromosome",
                "filter",
                "format_sample_0_GT",
                "format_sample_0_format_Character_.",
                "format_sample_0_format_Character_1",
                "format_sample_0_format_Character_2",
                "format_sample_0_format_Character_A",
                "format_sample_0_format_Character_G",
                "format_sample_0_format_Character_R",
                "format_sample_0_format_Float_.",
                "format_sample_0_format_Float_1",
                "format_sample_0_format_Float_2",
                "format_sample_0_format_Float_A",
                "format_sample_0_format_Float_G",
                "format_sample_0_format_Float_R",
                "format_sample_0_format_Integer_.",
                "format_sample_0_format_Integer_1",
                "format_sample_0_format_Integer_2",
                "format_sample_0_format_Integer_A",
                "format_sample_0_format_Integer_G",
                "format_sample_0_format_Integer_R",
                "format_sample_0_format_String_.",
                "format_sample_0_format_String_1",
                "format_sample_0_format_String_2",
                "format_sample_0_format_String_A",
                "format_sample_0_format_String_G",
                "format_sample_0_format_String_R",
                "format_sample_1_GT",
                "format_sample_1_format_Character_.",
                "format_sample_1_format_Character_1",
                "format_sample_1_format_Character_2",
                "format_sample_1_format_Character_A",
                "format_sample_1_format_Character_G",
                "format_sample_1_format_Character_R",
                "format_sample_1_format_Float_.",
                "format_sample_1_format_Float_1",
                "format_sample_1_format_Float_2",
                "format_sample_1_format_Float_A",
                "format_sample_1_format_Float_G",
                "format_sample_1_format_Float_R",
                "format_sample_1_format_Integer_.",
                "format_sample_1_format_Integer_1",
                "format_sample_1_format_Integer_2",
                "format_sample_1_format_Integer_A",
                "format_sample_1_format_Integer_G",
                "format_sample_1_format_Integer_R",
                "format_sample_1_format_String_.",
                "format_sample_1_format_String_1",
                "format_sample_1_format_String_2",
                "format_sample_1_format_String_A",
                "format_sample_1_format_String_G",
                "format_sample_1_format_String_R",
                "identifier",
                "info_info_Character_.",
                "info_info_Character_1",
                "info_info_Character_2",
                "info_info_Character_A",
                "info_info_Character_G",
                "info_info_Character_R",
                "info_info_Flag_0",
                "info_info_Float_.",
                "info_info_Float_1",
                "info_info_Float_2",
                "info_info_Float_A",
                "info_info_Float_G",
                "info_info_Float_R",
                "info_info_Integer_.",
                "info_info_Integer_1",
                "info_info_Integer_2",
                "info_info_Integer_A",
                "info_info_Integer_G",
                "info_info_Integer_R",
                "info_info_String_.",
                "info_info_String_1",
                "info_info_String_2",
                "info_info_String_A",
                "info_info_String_G",
                "info_info_String_R",
                "position",
                "quality",
                "reference"
            ]
        );

        match data.get_mut("chromosome") {
            Some(ColumnData::String(a)) => assert_eq!(
                a.finish(),
                arrow::array::StringBuilder::with_capacity(10, 10 * 10).finish()
            ),
            _ => panic!("Column chromosome not match type"),
        }
    }

    #[test]
    fn add_record() {
        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap();

        let schema = schema::from_header(&header, false).unwrap();
        let schema_map: rustc_hash::FxHashMap<String, Field> = schema
            .all_fields()
            .into_iter()
            .map(|f| (f.name().to_string(), f.clone()))
            .collect::<rustc_hash::FxHashMap<String, Field>>();

        let mut data = Name2Data::new(10, &schema);

        let mut iterator = reader.records(&header);
        let record = iterator.next().unwrap().unwrap();

        data.add_record(record, &header, &schema_map).unwrap();
        match data.get("chromosome") {
            Some(ColumnData::String(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_slice(), b"YAR028W");
                assert_eq!(a.offsets_slice(), &[0, 7]);
            }
            _ => panic!("Column chromosome does not match type"),
        }
        match data.get("position") {
            Some(ColumnData::Int(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_slice(), &[509242864]);
            }
            _ => panic!("Column position does not match type"),
        }
        match data.get("identifier") {
            Some(ColumnData::ListString(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_ref().values_slice(), b"");
                assert_eq!(a.offsets_slice(), &[0, 0]);
            }
            _ => panic!("Column identifier does not match type"),
        }
        match data.get("reference") {
            Some(ColumnData::String(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_slice(), b"A");
                assert_eq!(a.offsets_slice(), &[0, 1]);
            }
            _ => panic!("Column reference does not match type"),
        }
        match data.get("alternate") {
            Some(ColumnData::String(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_slice(), b"ATG");
                assert_eq!(a.offsets_slice(), &[0, 3]);
            }
            _ => panic!("Column alternate does not match type"),
        }
        match data.get("quality") {
            Some(ColumnData::Float(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_slice(), &[6.]);
            }
            _ => panic!("Column quality does not match type"),
        }
        match data.get("filter") {
            Some(ColumnData::ListString(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_ref().len(), 1);
                assert_eq!(a.values_ref().values_slice(), b"Filter_0");
                assert_eq!(a.values_ref().offsets_slice(), &[0, 8]);
                assert_eq!(a.offsets_slice(), &[0, 1]);
            }
            _ => panic!("Column filter does not match type"),
        }
        match data.get("info_info_Integer_1") {
            Some(ColumnData::Int(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_slice(), &[-1867486102]);
            }
            _ => panic!("Column info_info_Integer_1 does not match type"),
        }
        match data.get("info_info_Integer_2") {
            Some(ColumnData::ListInt(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_ref().len(), 2);
                assert_eq!(a.values_ref().values_slice(), &[1180908493, 1041698941]);
                assert_eq!(a.offsets_slice(), &[0, 2]);
                assert_eq!(a.offsets_slice(), &[0, 2]);
            }
            _ => panic!("Column info_info_Integer_2 does not match type"),
        }
        match data.get("info_info_Integer_A") {
            Some(ColumnData::Int(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_slice(), &[-207506013]);
            }
            _ => panic!("Column info_info_Integer_A does not match type"),
        }
        match data.get("info_info_Integer_R") {
            Some(ColumnData::ListInt(a)) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a.values_ref().len(), 2);
                assert_eq!(a.values_ref().values_slice(), &[-1221871784, -1356802777]);
                assert_eq!(a.offsets_slice(), &[0, 2]);
                assert_eq!(a.offsets_slice(), &[0, 2]);
            }
            _ => panic!("Column info_info_Integer_R does not match type"),
        }
    }
}
