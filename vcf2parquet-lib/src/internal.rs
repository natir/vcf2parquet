//! Internal enum to store all avaible type

/* std use */

/* crate use */

/* project use */
use crate::*;

#[derive(Clone)]
pub enum Internal {
    None,
    Integer(i32),
    Float(f32),
    Flag(bool),
    Character(char),
    String(String),
    Vec(Vec<Internal>),
}

impl Internal {
    pub fn into_i32(&self) -> error::Result<i32> {
        match self {
            Internal::Integer(a) => Ok(*a),
            _ => Err(error::Error::NoConversion),
        }
    }

    pub fn into_string(&self) -> error::Result<String> {
        match self {
            Internal::String(a) => Ok(a.to_string()),
            _ => Err(error::Error::NoConversion),
        }
    }
}

impl From<noodles::vcf::header::info::Type> for Internal {
    fn from(info_type: noodles::vcf::header::info::Type) -> Self {
        match info_type {
            noodles::vcf::header::info::Type::Integer => Internal::Integer(0),
            noodles::vcf::header::info::Type::Float => Internal::Float(0.0),
            noodles::vcf::header::info::Type::Flag => Internal::Flag(true),
            noodles::vcf::header::info::Type::Character => Internal::Character('a'),
            noodles::vcf::header::info::Type::String => Internal::String("".to_string()),
        }
    }
}

impl From<&noodles::vcf::record::info::field::Value> for Internal {
    fn from(info_value: &noodles::vcf::record::info::field::Value) -> Self {
        match info_value {
            noodles::vcf::record::info::field::Value::Integer(val) => Internal::Integer(*val),
            noodles::vcf::record::info::field::Value::Float(val) => Internal::Float(*val),
            noodles::vcf::record::info::field::Value::Flag => Internal::Flag(true),
            noodles::vcf::record::info::field::Value::Character(val) => Internal::Character(*val),
            noodles::vcf::record::info::field::Value::String(val) => {
                Internal::String(val.to_string())
            }
            noodles::vcf::record::info::field::Value::IntegerArray(vals) => Internal::Vec(
                vals.iter()
                    .map(|x| match x {
                        Some(a) => Internal::Integer(*a),
                        None => Internal::None,
                    })
                    .collect(),
            ),
            noodles::vcf::record::info::field::Value::FloatArray(vals) => Internal::Vec(
                vals.iter()
                    .map(|x| match x {
                        Some(a) => Internal::Float(*a),
                        None => Internal::None,
                    })
                    .collect(),
            ),
            noodles::vcf::record::info::field::Value::CharacterArray(vals) => Internal::Vec(
                vals.iter()
                    .map(|x| match x {
                        Some(a) => Internal::Character(*a),
                        None => Internal::None,
                    })
                    .collect(),
            ),
            noodles::vcf::record::info::field::Value::StringArray(vals) => Internal::Vec(
                vals.iter()
                    .map(|x| match x {
                        Some(a) => Internal::String(a.to_owned()),
                        None => Internal::None,
                    })
                    .collect(),
            ),
        }
    }
}

impl From<noodles::vcf::header::format::Type> for Internal {
    fn from(format_type: noodles::vcf::header::format::Type) -> Self {
        match format_type {
            noodles::vcf::header::format::Type::Integer => Internal::Integer(0),
            noodles::vcf::header::format::Type::Float => Internal::Float(0.0),
            noodles::vcf::header::format::Type::String => Internal::String("".to_string()),
            noodles::vcf::header::format::Type::Character => Internal::Character('a'),
        }
    }
}

impl From<&noodles::vcf::record::genotypes::genotype::field::Value> for Internal {
    fn from(format_type: &noodles::vcf::record::genotypes::genotype::field::Value) -> Self {
        match format_type {
            noodles::vcf::record::genotypes::genotype::field::Value::Integer(val) => {
                Internal::Integer(*val)
            }
            noodles::vcf::record::genotypes::genotype::field::Value::Float(val) => {
                Internal::Float(*val)
            }
            noodles::vcf::record::genotypes::genotype::field::Value::Character(val) => {
                Internal::Character(*val)
            }
            noodles::vcf::record::genotypes::genotype::field::Value::String(val) => {
                Internal::String(val.to_string())
            }
            noodles::vcf::record::genotypes::genotype::field::Value::IntegerArray(vals) => {
                Internal::Vec(
                    vals.iter()
                        .map(|x| match x {
                            Some(a) => Internal::Integer(*a),
                            None => Internal::None,
                        })
                        .collect(),
                )
            }
            noodles::vcf::record::genotypes::genotype::field::Value::FloatArray(vals) => {
                Internal::Vec(
                    vals.iter()
                        .map(|x| match x {
                            Some(a) => Internal::Float(*a),
                            None => Internal::None,
                        })
                        .collect(),
                )
            }
            noodles::vcf::record::genotypes::genotype::field::Value::CharacterArray(vals) => {
                Internal::Vec(
                    vals.iter()
                        .map(|x| match x {
                            Some(a) => Internal::Character(*a),
                            None => Internal::None,
                        })
                        .collect(),
                )
            }
            noodles::vcf::record::genotypes::genotype::field::Value::StringArray(vals) => {
                Internal::Vec(
                    vals.iter()
                        .map(|x| match x {
                            Some(a) => Internal::String(a.to_owned()),
                            None => Internal::None,
                        })
                        .collect(),
                )
            }
        }
    }
}

impl<T> From<Option<&T>> for Internal
where
    T: Into<Internal>,
    T: Copy,
{
    fn from(info_value: Option<&T>) -> Self {
        match info_value {
            Some(a) => a.to_owned().into(),
            None => Internal::None,
        }
    }
}
