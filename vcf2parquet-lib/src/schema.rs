//! Construct parquet schema corresponding to vcf

/* std use */

/* crate use */

/* project use */
use crate::*;

pub fn from_header(header: &noodles::vcf::Header) -> error::Result<arrow::datatypes::Schema> {
    arrow::datatypes::Schema::try_merge(vec![base()?, info(header)?, format(header)?])
        .map_err(|e| e.into())
}

fn base() -> error::Result<arrow::datatypes::Schema> {
    let chrom = arrow::datatypes::Field::new("chromosome", arrow::datatypes::DataType::Utf8, false);
    let pos = arrow::datatypes::Field::new("position", arrow::datatypes::DataType::Int32, false);
    let id = arrow::datatypes::Field::new(
        "identifiers",
        arrow::datatypes::DataType::List(Box::new(arrow::datatypes::Field::new(
            "identifier",
            arrow::datatypes::DataType::Utf8,
            false,
        ))),
        false,
    );

    let reference =
        arrow::datatypes::Field::new("reference", arrow::datatypes::DataType::Utf8, false);
    let alternative = arrow::datatypes::Field::new(
        "alternatives",
        arrow::datatypes::DataType::List(Box::new(arrow::datatypes::Field::new(
            "alternative",
            arrow::datatypes::DataType::Utf8,
            false,
        ))),
        false,
    );
    let qual = arrow::datatypes::Field::new("quality", arrow::datatypes::DataType::Int32, false);

    Ok(arrow::datatypes::Schema::new(vec![
        chrom,
        pos,
        id,
        reference,
        alternative,
        qual,
    ]))
}

fn info(header: &noodles::vcf::header::Header) -> error::Result<arrow::datatypes::Schema> {
    let mut fields = vec![];

    for (key, info) in header.infos() {
        match info.number() {
            noodles::vcf::header::Number::Count(0) => {
                fields.push(arrow::datatypes::Field::new(
                    key.as_ref(),
                    Internal::from(info.ty()).into(),
                    //info.ty().from().into(),
                    true,
                ));
            }
            noodles::vcf::header::Number::Count(1) => {
                fields.push(arrow::datatypes::Field::new(
                    key.as_ref(),
                    Internal::from(info.ty()).into(),
                    false,
                ));
            }
            _ => fields.push(arrow::datatypes::Field::new(
                key.as_ref(),
                arrow::datatypes::DataType::List(Box::new(arrow::datatypes::Field::new(
                    key.as_ref(),
                    Internal::from(info.ty()).into(),
                    false,
                ))),
                false,
            )),
        }
    }

    Ok(arrow::datatypes::Schema::new(fields))
}

fn format(header: &noodles::vcf::header::Header) -> error::Result<arrow::datatypes::Schema> {
    let mut fields = vec![];

    for (key, format) in header.formats() {
        match format.number() {
            noodles::vcf::header::Number::Count(0) => fields.push(arrow::datatypes::Field::new(
                key.as_ref(),
                Internal::from(format.ty()).into(),
                true,
            )),
            noodles::vcf::header::Number::Count(1) => fields.push(arrow::datatypes::Field::new(
                key.as_ref(),
                Internal::from(format.ty()).into(),
                false,
            )),
            _ => fields.push(arrow::datatypes::Field::new(
                key.as_ref(),
                arrow::datatypes::DataType::List(Box::new(arrow::datatypes::Field::new(
                    key.as_ref(),
                    Internal::from(format.ty()).into(),
                    false,
                ))),
                false,
            )),
        }
    }

    Ok(arrow::datatypes::Schema::new(fields))
}

#[derive(Clone)]
pub enum Internal {
    Integer(i32),
    Float(f32),
    Flag(String),
    Character(String),
    String(String),
}

impl From<noodles::vcf::header::info::Type> for Internal {
    fn from(info_type: noodles::vcf::header::info::Type) -> Self {
        match info_type {
            noodles::vcf::header::info::Type::Integer => Internal::Integer(0),
            noodles::vcf::header::info::Type::Float => Internal::Float(0.0),
            noodles::vcf::header::info::Type::Flag => Internal::Flag("".to_string()),
            noodles::vcf::header::info::Type::Character => Internal::Character("".to_string()),
            noodles::vcf::header::info::Type::String => Internal::String("".to_string()),
        }
    }
}

impl From<noodles::vcf::header::format::Type> for Internal {
    fn from(format_type: noodles::vcf::header::format::Type) -> Self {
        match format_type {
            noodles::vcf::header::format::Type::Integer => Internal::Integer(0),
            noodles::vcf::header::format::Type::Float => Internal::Float(0.0),
            noodles::vcf::header::format::Type::String => Internal::String("".to_string()),
            noodles::vcf::header::format::Type::Character => Internal::Character("".to_string()),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<arrow::datatypes::DataType> for Internal {
    fn into(self) -> arrow::datatypes::DataType {
        match self {
            Internal::Integer(_) => arrow::datatypes::DataType::Int32,
            Internal::Float(_) => arrow::datatypes::DataType::Float32,
            Internal::Flag(_) => arrow::datatypes::DataType::Utf8,
            Internal::Character(_) => arrow::datatypes::DataType::Utf8,
            Internal::String(_) => arrow::datatypes::DataType::Utf8,
        }
    }
}
