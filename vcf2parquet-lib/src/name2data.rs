//! Struct to link name and data

/* std use */

/* crate use */
use arrow2::array::MutableArray;
use arrow2::array::MutablePrimitiveArray;
use arrow2::array::TryPush;

/* project use */

///Alias of [std::collections::HashMap] that associate a column name and [ColumnData], a proxy of arrow2 datastructure
#[derive(Debug)]
pub struct Name2Data(rustc_hash::FxHashMap<String, ColumnData>);

impl Name2Data {
    /// Create a new Name2Data, vcf header is required to add info and genotype column
    /// length parameter is used to preallocate memory
    pub fn new(length: usize, schema: &arrow2::datatypes::Schema) -> Self {
        let mut name2data = rustc_hash::FxHashMap::default();
        for field in schema.fields.iter() {
            name2data.insert(
                field.name.clone(),
                ColumnData::new(&field.data_type, length),
            );
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
        schema: &arrow2::datatypes::Schema,
    ) -> std::result::Result<(), arrow2::error::Error> {
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
        schema: &arrow2::datatypes::Schema,
        alt_id: usize,
        allele_count: usize,
    ) -> std::result::Result<(), arrow2::error::Error> {
        let info = record.info();

        for key in header.infos().keys() {
            let key_name = format!("info_{}", key);
            let info_def = header.infos().get(key).unwrap();
            if let Some(column) = self.0.get_mut(&key_name) {
                match info.get(key) {
                    Some(value) => match value {
                        Some(noodles::vcf::record::info::field::Value::Flag) => {
                            column.push_bool(Some(true));
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
                                            *array_val.get(alt_id).unwrap(),
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
                                                *array_val.get(alt_id).unwrap(),
                                            ])?;
                                        } else {
                                            eprintln!(
                                                "Field {} declared as G but found array of size {}",
                                                key,
                                                array_val.len()
                                            );
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
                                            *array_val.get(alt_id).unwrap(),
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
                                                *array_val.get(alt_id).unwrap(),
                                            ])?;
                                        } else {
                                            eprintln!(
                                                "Field {} declared as G but found array of size {}",
                                                key,
                                                array_val.len()
                                            );
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
                                            Some(array_val.get(alt_id).unwrap().clone().unwrap()),
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
                                                array_val.get(alt_id).unwrap().clone(),
                                            ])?;
                                        } else {
                                            eprintln!(
                                                "Field {} declared as G but found array of size {}",
                                                key,
                                                array_val.len()
                                            );
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
                                        Some(array_val.get(alt_id).unwrap().unwrap().to_string()),
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
                                                array_val.get(alt_id).unwrap().unwrap().to_string(),
                                            ),
                                        ])?;
                                    } else {
                                        eprintln!(
                                            "Field {} declared as G but found array of size {}",
                                            key,
                                            array_val.len()
                                        );
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
                        None => column.push_null(),
                    },
                    None => {
                        if info_def.ty()
                            == noodles::vcf::header::record::value::map::info::Type::Flag
                        {
                            column.push_bool(Some(false));
                        } else {
                            //Handle missing info field, only matters for FixedSizeList
                            for field in schema.fields.iter() {
                                if field.name == key_name {
                                    match field.data_type {
                                        arrow2::datatypes::DataType::FixedSizeList(
                                            ref field_type,
                                            fixed_size,
                                        ) => match &field_type.data_type() {
                                            arrow2::datatypes::DataType::Boolean => column
                                                .push_vecbool(vec![None; fixed_size as usize])?,
                                            arrow2::datatypes::DataType::Int32 => column
                                                .push_veci32(vec![None; fixed_size as usize])?,

                                            arrow2::datatypes::DataType::Float32 => column
                                                .push_vecf32(vec![None; fixed_size as usize])?,

                                            arrow2::datatypes::DataType::Utf8 => column
                                                .push_vecstring(vec![None; fixed_size as usize])?,

                                            _ => column.push_null(),
                                        },
                                        _ => column.push_null(), //Otherwise, just push null
                                    }
                                }
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
        schema: &arrow2::datatypes::Schema,
        alt_id: usize,
        allele_count: usize,
    ) -> std::result::Result<(), arrow2::error::Error> {
        for key in header.formats().keys() {
            for (idx, sample) in header.sample_names().iter().enumerate() {
                let key_name = format!("format_{}_{}", sample, key);
                let format_def = header.formats().get(key).unwrap();
                if let Some(column) = self.0.get_mut(&key_name) {
                    if let Some(format_field) = record.genotypes().get_index(idx) {
                        match format_field.get(key) {
                            Some(value) => match value {
                                Some(
                                    noodles::vcf::record::genotypes::sample::Value::Integer(
                                        value,
                                    ),
                                ) => {
                                    column.push_i32(Some(*value));
                                }
                                Some(
                                    noodles::vcf::record::genotypes::sample::Value::Float(
                                        value,
                                    ),
                                ) => {
                                    column.push_f32(Some(*value));
                                }
                                Some(
                                    noodles::vcf::record::genotypes::sample::Value::String(
                                        value,
                                    ),
                                ) => {
                                    column.push_string(value.to_string());
                                }
                                Some(
                                    noodles::vcf::record::genotypes::sample::Value::Character(
                                        value,
                                    ),
                                ) => {
                                    column.push_string(value.to_string());
                                }
                                Some(
                                    noodles::vcf::record::genotypes::sample::Value::Array(arr),
                                ) => match arr.clone() {
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
                                                *array_val.get(alt_id).unwrap(),
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
                                                    *array_val.get(alt_id).unwrap(),
                                                ])?;
                                            } else {
                                                eprintln!(
                                                    "Field {} declared as G but found array of size {}",
                                                    key,
                                                    array_val.len()
                                                );
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
                                                *array_val.get(alt_id).unwrap(),
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
                                                    *array_val.get(alt_id).unwrap(),
                                                ])?;
                                            } else {
                                                eprintln!(
                                                    "Field {} declared as G but found array of size {}",
                                                    key,
                                                    array_val.len()
                                                );
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
                                                Some(array_val.get(alt_id).unwrap().clone().unwrap()),
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
                                                    array_val.get(alt_id).unwrap().clone(),
                                                ])?;
                                            } else {
                                                eprintln!(
                                                    "Field {} declared as G but found array of size {}",
                                                    key,
                                                    array_val.len()
                                                );
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
                                                Some(array_val.get(alt_id).unwrap().unwrap().to_string()),
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
                                                        array_val.get(alt_id).unwrap().unwrap().to_string(),
                                                    ),
                                                ])?;
                                            } else {
                                                eprintln!(
                                                    "Field {} declared as G but found array of size {}",
                                                    key,
                                                    array_val.len()
                                                );
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
                                None => column.push_null(),
                            },
                            None => column.push_null(),
                        }
                    } else {
                        //Handle missing format field, only matters for FixedSizeList
                        for field in schema.fields.iter() {
                            if field.name == key_name {
                                match field.data_type {
                                    arrow2::datatypes::DataType::FixedSizeList(
                                        ref field_type,
                                        fixed_size,
                                    ) => {
                                        match &field_type.data_type() {
                                            arrow2::datatypes::DataType::Boolean => column
                                                .push_vecbool(vec![None; fixed_size as usize])?,
                                            arrow2::datatypes::DataType::Int32 => column
                                                .push_veci32(vec![None; fixed_size as usize])?,

                                            arrow2::datatypes::DataType::Float32 => column
                                                .push_vecf32(vec![None; fixed_size as usize])?,

                                            arrow2::datatypes::DataType::Utf8 => column
                                                .push_vecstring(vec![None; fixed_size as usize])?,

                                            _ => column.push_null(),
                                        }
                                    }
                                    _ => column.push_null(),
                                }
                            }
                        }
                    }
                }
            }
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
    pub fn new(arrow_type: &arrow2::datatypes::DataType, length: usize) -> Self {
        match arrow_type {
            arrow2::datatypes::DataType::Boolean => {
                ColumnData::Bool(arrow2::array::MutableBooleanArray::with_capacity(length))
            }
            arrow2::datatypes::DataType::Int32 => ColumnData::Int(
                arrow2::array::MutablePrimitiveArray::<i32>::with_capacity(length),
            ),
            arrow2::datatypes::DataType::Float32 => ColumnData::Float(
                arrow2::array::MutablePrimitiveArray::<f32>::with_capacity(length),
            ),
            arrow2::datatypes::DataType::Utf8 => ColumnData::String(
                arrow2::array::MutableUtf8Array::<i32>::with_capacity(length),
            ),
            arrow2::datatypes::DataType::List(field) => match field.data_type() {
                arrow2::datatypes::DataType::Boolean => {
                    ColumnData::ListBool(arrow2::array::MutableListArray::<
                        i32,
                        arrow2::array::MutableBooleanArray,
                    >::with_capacity(length))
                }
                arrow2::datatypes::DataType::Int32 => {
                    ColumnData::ListInt(arrow2::array::MutableListArray::<
                        i32,
                        MutablePrimitiveArray<i32>,
                    >::with_capacity(length))
                }
                arrow2::datatypes::DataType::Float32 => {
                    ColumnData::ListFloat(arrow2::array::MutableListArray::<
                        i32,
                        MutablePrimitiveArray<f32>,
                    >::with_capacity(length))
                }
                arrow2::datatypes::DataType::Utf8 => {
                    ColumnData::ListString(arrow2::array::MutableListArray::<
                        i32,
                        arrow2::array::MutableUtf8Array<i32>,
                    >::with_capacity(length))
                }
                _ => todo!(),
            },
            arrow2::datatypes::DataType::FixedSizeList(field, _) => match field.data_type() {
                arrow2::datatypes::DataType::Boolean => {
                    ColumnData::ListBool(arrow2::array::MutableListArray::<
                        i32,
                        arrow2::array::MutableBooleanArray,
                    >::with_capacity(length))
                }
                arrow2::datatypes::DataType::Int32 => {
                    ColumnData::ListInt(arrow2::array::MutableListArray::<
                        i32,
                        MutablePrimitiveArray<i32>,
                    >::with_capacity(length))
                }
                arrow2::datatypes::DataType::Float32 => {
                    ColumnData::ListFloat(arrow2::array::MutableListArray::<
                        i32,
                        MutablePrimitiveArray<f32>,
                    >::with_capacity(length))
                }
                arrow2::datatypes::DataType::Utf8 => {
                    ColumnData::ListString(arrow2::array::MutableListArray::<
                        i32,
                        arrow2::array::MutableUtf8Array<i32>,
                    >::with_capacity(length))
                }
                _ => todo!(),
            },
            dt => unreachable!("Unsupported arrow type, please check Schema: {:?}", dt),
        }
    }
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
                    panic!("ListString {e:?}");
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnData::Bool(a) => a.len(),
            ColumnData::Int(a) => a.len(),
            ColumnData::Float(a) => a.len(),
            ColumnData::String(a) => a.len(),
            ColumnData::ListBool(a) => a.len(),
            ColumnData::ListInt(a) => a.len(),
            ColumnData::ListFloat(a) => a.len(),
            ColumnData::ListString(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
    use crate::schema;

    use super::*;

    static VCF_FILE: &[u8] = b"##fileformat=VCFv4.3
##fileDate=20220528
##source=ClinVar
##reference=GRCh38
##INFO=<ID=ALLELEID,Number=1,Type=Integer,Description=\"the ClinVar Allele ID\">
##INFO=<ID=ALLELEIDS,Number=2,Type=Integer,Description=\"the ClinVar Allele ID\">
##INFO=<ID=AF_ESP,Number=1,Type=Float,Description=\"allele frequencies from GO-ESP\">
##INFO=<ID=AF_ESPS,Number=2,Type=Float,Description=\"allele frequencies from GO-ESP\">
##INFO=<ID=DBVARI,Number=0,Type=Flag,Description=\"nsv accessions from dbVar for the variant\">
##INFO=<ID=GENEINFO,Number=1,Type=Character,Description=\"Gene(s) \">
##INFO=<ID=GENEINFOS,Number=2,Type=Character,Description=\"Gene(s) \">
##INFO=<ID=CLNVC,Number=1,Type=String,Description=\"Variant type\">
##INFO=<ID=CLNVCS,Number=2,Type=String,Description=\"Variant type\">
##FORMAT=<ID=AB,Number=1,Type=Integer,Description=\"Allelic \">
##FORMAT=<ID=ABS,Number=2,Type=Integer,Description=\"Allelic \">
##FORMAT=<ID=DC,Number=1,Type=Float,Description=\"Approximate read \">
##FORMAT=<ID=DCS,Number=2,Type=Float,Description=\"Approximate read \">
##FORMAT=<ID=GF,Number=1,Type=Character,Description=\"Genotype Quality\">
##FORMAT=<ID=GFS,Number=2,Type=Character,Description=\"Genotype Quality\">
##SAMPLE=<ID=first,Genomes=Germline,Mixture=1.,Description=\"first\">
##SAMPLE=<ID=second,Genomes=Germline,Mixture=1.,Description=\"second\">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tfirst\tsecond
1\t10\t.\tA\tC,T\t23\tPASS\tALLELEID=14;ALLELEIDS=14,15;AF_ESP=1.2;AF_ESPS=1.2,1.5;DBVARI;GENEINFO=c;GENEINFOS=c,d;CLNVC=test1;CLNVCS=test1,test2\tAB:ABS:GF:GFS:DC:DCS\t1:2,3:c:C,D:1.2:1.4,1.6\t1:2,3:c:C,D:1.2:1.4,1.6
1\t20\t.\tA\tC\t23\tq5,q10\tALLELEID=14;AF_ESP=1.2;DBVARI;GENEINFO=c;CLNVC=test1;CLNVCS=test1,test2\tAB:GF:DC\t1:c:1.2\t2:a:4.5
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
                "alternate".to_string(),
                "chromosome".to_string(),
                "filter".to_string(),
                "format_first_AB".to_string(),
                "format_first_ABS".to_string(),
                "format_first_DC".to_string(),
                "format_first_DCS".to_string(),
                "format_first_GF".to_string(),
                "format_first_GFS".to_string(),
                "format_second_AB".to_string(),
                "format_second_ABS".to_string(),
                "format_second_DC".to_string(),
                "format_second_DCS".to_string(),
                "format_second_GF".to_string(),
                "format_second_GFS".to_string(),
                "identifier".to_string(),
                "info_AF_ESP".to_string(),
                "info_AF_ESPS".to_string(),
                "info_ALLELEID".to_string(),
                "info_ALLELEIDS".to_string(),
                "info_CLNVC".to_string(),
                "info_CLNVCS".to_string(),
                "info_DBVARI".to_string(),
                "info_GENEINFO".to_string(),
                "info_GENEINFOS".to_string(),
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
        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap();

        let schema = schema::from_header(&header, false).unwrap();
        let mut data = Name2Data::new(10, &schema);

        let mut iterator = reader.records(&header);
        let record = iterator.next().unwrap().unwrap();

        data.add_record(record, &header, &schema).unwrap();
        assert_eq!(format!("{:?}", data.get("alternate")), "Some(String(MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 1, 2]), values: [67, 84] }, validity: None }))".to_string());

        assert_eq!(format!("{:?}", data.get("chromosome")), "Some(String(MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 1, 2]), values: [49, 49] }, validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("filter")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: Offsets([0, 1]), values: MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 4]), values: [80, 65, 83, 83] }, validity: None }, validity: None }))".to_string());

        assert_eq!(
            format!("{:?}", data.get("format_first_AB")),
            "Some(Int(MutablePrimitiveArray { data_type: Int32, values: [1], validity: None }))"
                .to_string()
        );
        assert_eq!(
            format!("{:?}", data.get("format_first_ABS")),
            "Some(ListInt(MutableListArray { data_type: List(Field { name: \"item\", data_type: Int32, is_nullable: true, metadata: {} }), offsets: Offsets([0, 2]), values: MutablePrimitiveArray { data_type: Int32, values: [2, 3], validity: None }, validity: None }))"
                .to_string()
        );
        assert_eq!(
            format!("{:?}", data.get("format_first_DC")),
            "Some(Float(MutablePrimitiveArray { data_type: Float32, values: [1.2], validity: None }))".to_string()
        );
        assert_eq!(
            format!("{:?}", data.get("format_first_DCS")),
            "Some(ListFloat(MutableListArray { data_type: List(Field { name: \"item\", data_type: Float32, is_nullable: true, metadata: {} }), offsets: Offsets([0, 2]), values: MutablePrimitiveArray { data_type: Float32, values: [1.4, 1.6], validity: None }, validity: None }))".to_string()
        );
        assert_eq!(format!("{:?}", data.get("format_first_GF")), "Some(String(MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 1]), values: [99] }, validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("format_first_GFS")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: Offsets([0, 2]), values: MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 1, 2]), values: [67, 68] }, validity: None }, validity: None }))".to_string());
        assert_eq!(
            format!("{:?}", data.get("format_second_AB")),
            "Some(Int(MutablePrimitiveArray { data_type: Int32, values: [1], validity: None }))"
                .to_string()
        );
        assert_eq!(
            format!("{:?}", data.get("format_second_DC")),
            "Some(Float(MutablePrimitiveArray { data_type: Float32, values: [1.2], validity: None }))".to_string()
        );
        assert_eq!(
            format!("{:?}", data.get("format_second_GF")),
            "Some(String(MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 1]), values: [99] }, validity: None }))".to_string()
        );
        assert_eq!(format!("{:?}", data.get("identifier")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: Offsets([0, 1]), values: MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 0]), values: [] }, validity: Some([0b_______0]) }, validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("info_AF_ESP")), "Some(Float(MutablePrimitiveArray { data_type: Float32, values: [1.2], validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("info_AF_ESPS")), "Some(ListFloat(MutableListArray { data_type: List(Field { name: \"item\", data_type: Float32, is_nullable: true, metadata: {} }), offsets: Offsets([0, 2]), values: MutablePrimitiveArray { data_type: Float32, values: [1.2, 1.5], validity: None }, validity: None }))".to_string());
        assert_eq!(
            format!("{:?}", data.get("info_ALLELEID")),
            "Some(Int(MutablePrimitiveArray { data_type: Int32, values: [14], validity: None }))"
                .to_string()
        );
        assert_eq!(
            format!("{:?}", data.get("info_ALLELEIDS")),
            "Some(ListInt(MutableListArray { data_type: List(Field { name: \"item\", data_type: Int32, is_nullable: true, metadata: {} }), offsets: Offsets([0, 2]), values: MutablePrimitiveArray { data_type: Int32, values: [14, 15], validity: None }, validity: None }))"
                .to_string()
        );
        assert_eq!(format!("{:?}", data.get("info_CLNVC")), "Some(String(MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 5]), values: [116, 101, 115, 116, 49] }, validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("info_CLNVCS")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: Offsets([0, 2]), values: MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 5, 10]), values: [116, 101, 115, 116, 49, 116, 101, 115, 116, 50] }, validity: None }, validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("info_DBVARI")), "Some(Bool(MutableBooleanArray { data_type: Boolean, values: [0b_______1], validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("info_GENEINFO")), "Some(String(MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 1]), values: [99] }, validity: None }))".to_string());
        assert_eq!(
            format!("{:?}", data.get("position")),
            "Some(Int(MutablePrimitiveArray { data_type: Int32, values: [10], validity: None }))"
                .to_string()
        );
        assert_eq!(format!("{:?}", data.get("quality")), "Some(Float(MutablePrimitiveArray { data_type: Float32, values: [23.0], validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("reference")), "Some(String(MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 1]), values: [65] }, validity: None }))".to_string());

        let record = iterator.next().unwrap().unwrap();
        let mut data = Name2Data::new(10, &schema);
        data.add_record(record, &header, &schema).unwrap();

        assert_eq!(format!("{:?}", data.get("alternate")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: Offsets([0, 1]), values: MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 1]), values: [67] }, validity: None }, validity: None }))".to_string());
        assert_eq!(format!("{:?}", data.get("filter")), "Some(ListString(MutableListArray { data_type: List(Field { name: \"item\", data_type: Utf8, is_nullable: true, metadata: {} }), offsets: Offsets([0, 1]), values: MutableUtf8Array { values: MutableUtf8ValuesArray { data_type: Utf8, offsets: Offsets([0, 6]), values: [113, 53, 44, 113, 49, 48] }, validity: None }, validity: None }))".to_string());
    }
}
