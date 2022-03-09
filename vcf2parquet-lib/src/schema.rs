//! Construct parquet schema corresponding to vcf

/* std use */

/* crate use */

/* project use */
use crate::*;

pub fn from_header(header: &noodles::vcf::Header) -> error::Result<arrow2::datatypes::Schema> {
    let mut columns = Vec::new();

    // required column
    columns.extend(required_column());

    // filter value
    columns.extend(filter());

    // info field
    //columns.extend(info(header));

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
                true,
            ))),
            true,
        ),
        arrow2::datatypes::Field::new("reference", arrow2::datatypes::DataType::Utf8, false),
        arrow2::datatypes::Field::new(
            "alternate",
            arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                "alt",
                arrow2::datatypes::DataType::Utf8,
                true,
            ))),
            false,
        ),
        arrow2::datatypes::Field::new("quality", arrow2::datatypes::DataType::Float32, true),
    ]
}

fn filter() -> Vec<arrow2::datatypes::Field> {
    vec![arrow2::datatypes::Field::new(
        "filter",
        arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
            "filter",
            arrow2::datatypes::DataType::Utf8,
            true,
        ))),
        false,
    )]
}

fn info(header: &noodles::vcf::Header) -> Vec<arrow2::datatypes::Field> {
    let mut fields = Vec::new();

    for (name, value) in header.infos() {
        let arrow_type = match value.ty() {
            noodles::vcf::header::info::Type::Integer => arrow2::datatypes::DataType::Int32,
            noodles::vcf::header::info::Type::Float => arrow2::datatypes::DataType::Float32,
            noodles::vcf::header::info::Type::Flag => arrow2::datatypes::DataType::Utf8,
            noodles::vcf::header::info::Type::Character => arrow2::datatypes::DataType::Utf8,
            noodles::vcf::header::info::Type::String => arrow2::datatypes::DataType::Utf8,
        };

        match value.number() {
            noodles::vcf::header::Number::Count(1) => fields.push(arrow2::datatypes::Field::new(
                name.to_string(),
                arrow_type,
                false,
            )),
            _ => fields.push(arrow2::datatypes::Field::new(
                name.to_string(),
                arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                    name.to_string(),
                    arrow_type,
                    true,
                ))),
                false,
            )),
        }
    }

    fields
}
