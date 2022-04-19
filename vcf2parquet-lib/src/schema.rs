//! Construct parquet schema corresponding to vcf

/* std use */

/* crate use */

/* project use */
use crate::*;

pub fn from_header(header: &noodles::vcf::Header) -> error::Result<arrow2::datatypes::Schema> {
    let mut columns = Vec::new();

    // required column
    columns.extend(required_column());

    // info field
    columns.extend(info(header));

    // genotype field
    columns.extend(genotype(header));

    Ok(arrow2::datatypes::Schema::from(columns))
}

fn required_column() -> Vec<arrow2::datatypes::Field> {
    vec![
        arrow2::datatypes::Field::new("chromosome", arrow2::datatypes::DataType::Utf8, false),
        arrow2::datatypes::Field::new("position", arrow2::datatypes::DataType::Int32, false),
        arrow2::datatypes::Field::new(
            "identifier",
            arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                "id",
                arrow2::datatypes::DataType::Utf8,
                false,
            ))),
            false,
        ),
        arrow2::datatypes::Field::new("reference", arrow2::datatypes::DataType::Utf8, false),
        arrow2::datatypes::Field::new(
            "alternate",
            arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                "alternate",
                arrow2::datatypes::DataType::Utf8,
                false,
            ))),
            false,
        ),
        arrow2::datatypes::Field::new("quality", arrow2::datatypes::DataType::Float32, true),
        arrow2::datatypes::Field::new(
            "filter",
            arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                "filter",
                arrow2::datatypes::DataType::Utf8,
                false,
            ))),
            false,
        ),
    ]
}

fn info(header: &noodles::vcf::Header) -> Vec<arrow2::datatypes::Field> {
    let mut fields = Vec::new();

    for (name, value) in header.infos() {
        let key = format!("info_{}", name);

        let arrow_type = match value.ty() {
            noodles::vcf::header::info::Type::Integer => arrow2::datatypes::DataType::Int32,
            noodles::vcf::header::info::Type::Float => arrow2::datatypes::DataType::Float32,
            noodles::vcf::header::info::Type::Flag => arrow2::datatypes::DataType::Boolean,
            noodles::vcf::header::info::Type::Character => arrow2::datatypes::DataType::Utf8,
            noodles::vcf::header::info::Type::String => arrow2::datatypes::DataType::Utf8,
        };

        match value.number() {
            noodles::vcf::header::Number::Count(0) => {
                fields.push(arrow2::datatypes::Field::new(&key, arrow_type, false))
            }
            noodles::vcf::header::Number::Count(1) => {
                fields.push(arrow2::datatypes::Field::new(&key, arrow_type, false))
            }
            _ => fields.push(arrow2::datatypes::Field::new(
                &key,
                arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                    &key, arrow_type, false,
                ))),
                false,
            )),
        }
    }

    fields
}

fn genotype(header: &noodles::vcf::Header) -> Vec<arrow2::datatypes::Field> {
    let mut fields = Vec::new();

    for sample in header.sample_names() {
        for (name, value) in header.formats() {
            let key = format!("format_{}_{}", sample, name);

            let arrow_type = match value.ty() {
                noodles::vcf::header::format::Type::Integer => arrow2::datatypes::DataType::Int32,
                noodles::vcf::header::format::Type::Float => arrow2::datatypes::DataType::Float32,
                noodles::vcf::header::format::Type::Character => arrow2::datatypes::DataType::Utf8,
                noodles::vcf::header::format::Type::String => arrow2::datatypes::DataType::Utf8,
            };

            match value.number() {
                noodles::vcf::header::Number::Count(0) => {
                    fields.push(arrow2::datatypes::Field::new(key, arrow_type, false))
                }
                noodles::vcf::header::Number::Count(1) => {
                    fields.push(arrow2::datatypes::Field::new(key, arrow_type, false))
                }
                _ => fields.push(arrow2::datatypes::Field::new(
                    &key,
                    arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                        &key, arrow_type, false,
                    ))),
                    false,
                )),
            }
        }
    }

    fields
}
