//! Convert noodles_vcf type in arrow type

/* std use */

/* crate use */
use arrow::datatypes as arrow_types;
use noodles::vcf::record::info::field as noodles_types;

/* project use */
use crate::*;

pub fn noodles2arrow(
    value: noodles_types::Value,
    name: &str,
    nullable: bool,
) -> error::Result<arrow_types::Field> {
    match value {
        noodles_types::Value::Integer(_) => Ok(arrow_types::Field::new(
            name,
            arrow_types::DataType::Int32,
            nullable,
        )),
        noodles_types::Value::Float(_) => Ok(arrow_types::Field::new(
            name,
            arrow_types::DataType::Float32,
            nullable,
        )),
        noodles_types::Value::Character(_) => Ok(arrow_types::Field::new(
            name,
            arrow_types::DataType::Utf8,
            nullable,
        )),
        noodles_types::Value::String(_) => Ok(arrow_types::Field::new(
            name,
            arrow_types::DataType::Utf8,
            nullable,
        )),
        noodles_types::Value::IntegerArray(_) => Ok(arrow_types::Field::new(
            name,
            arrow_types::DataType::List(Box::new(arrow_types::Field::new(
                "unknow",
                arrow_types::DataType::Int32,
                true,
            ))),
            nullable,
        )),
        noodles_types::Value::FloatArray(_) => Ok(arrow_types::Field::new(
            name,
            arrow_types::DataType::List(Box::new(arrow_types::Field::new(
                "unknow",
                arrow_types::DataType::Int32,
                true,
            ))),
            nullable,
        )),
        noodles_types::Value::CharacterArray(_) => Ok(arrow_types::Field::new(
            name,
            arrow_types::DataType::List(Box::new(arrow_types::Field::new(
                "unknow",
                arrow_types::DataType::Utf8,
                true,
            ))),
            nullable,
        )),
        noodles_types::Value::StringArray(_) => Ok(arrow_types::Field::new(
            name,
            arrow_types::DataType::List(Box::new(arrow_types::Field::new(
                "unknow",
                arrow_types::DataType::Utf8,
                true,
            ))),
            nullable,
        )),
        _ => Err(error::Error::NoConversion),
    }
}
