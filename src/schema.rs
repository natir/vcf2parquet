//! Construct parquet schema corresponding to vcf

/* std use */
use std::sync::Arc;

/* crate use */

/* project use */
use crate::*;

/// Generate a parquet schema corresponding to vcf header
pub fn from_header(
    header: &noodles::vcf::Header,
    info_optional: bool,
) -> error::Result<arrow::datatypes::Schema> {
    let mut columns = Vec::new();

    // required column
    columns.extend(required_column());

    // info field
    columns.extend(info(header, info_optional));

    // genotype field
    columns.extend(genotype(header));

    Ok(arrow::datatypes::Schema::new(columns))
}

fn required_column() -> Vec<arrow::datatypes::Field> {
    vec![
        arrow::datatypes::Field::new("chromosome", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("position", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new(
            "identifier",
            arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                "identifier",
                arrow::datatypes::DataType::Utf8,
                false,
            ))),
            false,
        ),
        arrow::datatypes::Field::new("reference", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("alternate", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("quality", arrow::datatypes::DataType::Float32, true),
        arrow::datatypes::Field::new(
            "filter",
            arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                "filter",
                arrow::datatypes::DataType::Utf8,
                false,
            ))),
            false,
        ),
    ]
}

fn info(header: &noodles::vcf::Header, info_optional: bool) -> Vec<arrow::datatypes::Field> {
    let mut fields = Vec::new();

    for (name, value) in header.infos() {
        let key = format!("info_{name}");

        let arrow_type = match value.ty() {
            noodles::vcf::header::record::value::map::info::Type::Integer => {
                arrow::datatypes::DataType::Int32
            }
            noodles::vcf::header::record::value::map::info::Type::Float => {
                arrow::datatypes::DataType::Float32
            }
            noodles::vcf::header::record::value::map::info::Type::Flag => {
                arrow::datatypes::DataType::Boolean
            }
            noodles::vcf::header::record::value::map::info::Type::Character => {
                arrow::datatypes::DataType::Utf8
            }
            noodles::vcf::header::record::value::map::info::Type::String => {
                arrow::datatypes::DataType::Utf8
            }
        };

        match value.number() {
            noodles::vcf::header::Number::Count(0 | 1) | noodles::vcf::header::Number::A => fields
                .push(arrow::datatypes::Field::new(
                    &key,
                    arrow_type,
                    info_optional,
                )),
            noodles::vcf::header::Number::R => fields.push(arrow::datatypes::Field::new(
                &key,
                arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                    &key,
                    arrow_type,
                    info_optional,
                ))),
                info_optional,
            )),
            noodles::vcf::header::Number::Count(_n) => fields.push(arrow::datatypes::Field::new(
                &key,
                arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                    &key,
                    arrow_type,
                    info_optional,
                ))),
                info_optional,
            )),
            noodles::vcf::header::Number::G => fields.push(arrow::datatypes::Field::new(
                &key,
                arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                    &key,
                    arrow_type,
                    info_optional,
                ))),
                info_optional,
            )),

            noodles::vcf::header::Number::Unknown => fields.push(arrow::datatypes::Field::new(
                &key,
                arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                    &key,
                    arrow_type,
                    info_optional,
                ))),
                info_optional,
            )),
        }
    }

    fields
}

fn genotype(header: &noodles::vcf::Header) -> Vec<arrow::datatypes::Field> {
    let mut fields = Vec::new();

    for sample in header.sample_names() {
        for (name, value) in header.formats() {
            let key = format!("format_{sample}_{name}");

            let arrow_type = match value.ty() {
                noodles::vcf::header::record::value::map::format::Type::Integer => {
                    arrow::datatypes::DataType::Int32
                }
                noodles::vcf::header::record::value::map::format::Type::Float => {
                    arrow::datatypes::DataType::Float32
                }
                noodles::vcf::header::record::value::map::format::Type::Character => {
                    arrow::datatypes::DataType::Utf8
                }
                noodles::vcf::header::record::value::map::format::Type::String => {
                    arrow::datatypes::DataType::Utf8
                }
            };

            match value.number() {
                noodles::vcf::header::Number::Count(0 | 1) | noodles::vcf::header::Number::A => {
                    fields.push(arrow::datatypes::Field::new(key, arrow_type, true))
                }
                noodles::vcf::header::Number::R => fields.push(arrow::datatypes::Field::new(
                    &key,
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        &key, arrow_type, true,
                    ))),
                    true,
                )),
                noodles::vcf::header::Number::Count(_n) => {
                    fields.push(arrow::datatypes::Field::new(
                        &key,
                        arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                            &key, arrow_type, true,
                        ))),
                        true,
                    ))
                }
                noodles::vcf::header::Number::G => fields.push(arrow::datatypes::Field::new(
                    &key,
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        &key, arrow_type, true,
                    ))),
                    true,
                )),

                noodles::vcf::header::Number::Unknown => fields.push(arrow::datatypes::Field::new(
                    &key,
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        &key, arrow_type, true,
                    ))),
                    true,
                )),
            }
        }
    }

    fields
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    //
    //

    static VCF_FILE: &[u8] = b"##fileformat=VCFv4.3
##fileDate=20220528
##source=ClinVar
##reference=GRCh38
##INFO=<ID=Flag,Number=0,Type=Flag,Description=\"flag\">
##INFO=<ID=Info1,Number=1,Type=Float,Description=\"1 float\">
##INFO=<ID=Info_fixed,Number=3,Type=Integer,Description=\"3 integer\">
##INFO=<ID=Info_A,Number=A,Type=Integer,Description=\"A integer\">
##INFO=<ID=Info_RString,Number=R,Type=Character,Description=\"R character\">
##INFO=<ID=Info_RChar,Number=R,Type=String,Description=\"R string\">
##INFO=<ID=Info_G,Number=G,Type=Integer,Description=\"G integer\">
##INFO=<ID=Info_.,Number=.,Type=Integer,Description=\"Unknown integer\">
##FORMAT=<ID=Format_1,Number=1,Type=Integer,Description=\"1 integer\">
##FORMAT=<ID=Format_fixed,Number=4,Type=Float,Description=\"4 float\">
##FORMAT=<ID=Format_A,Number=A,Type=String,Description=\"A string\">
##FORMAT=<ID=Format_R,Number=R,Type=Character,Description=\"R character\">
##FORMAT=<ID=Format_G,Number=G,Type=Integer,Description=\"G integer\">
##FORMAT=<ID=Format_.,Number=1,Type=Integer,Description=\"Unknow integer\">
##SAMPLE=<ID=first,Genomes=Germline,Mixture=1.,Description=\"first\">
##SAMPLE=<ID=second,Genomes=Germline,Mixture=1.,Description=\"second\">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tfirst\tsecond
";

    lazy_static::lazy_static! {
        static ref MINI_COLS: Vec<arrow::datatypes::Field> = vec![
            arrow::datatypes::Field::new("chromosome", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("position", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new(
                "identifier",
                arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "identifier",
                    arrow::datatypes::DataType::Utf8,
                    false,
                ))),
                false,
            ),
            arrow::datatypes::Field::new("reference", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("alternate", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("quality", arrow::datatypes::DataType::Float32, true),
            arrow::datatypes::Field::new(
                "filter",
                arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "filter",
                    arrow::datatypes::DataType::Utf8,
                    false,
                ))),
                false,
            ),
        ];

    static ref INFO_COLS: Vec<arrow::datatypes::Field> = vec![
        arrow::datatypes::Field::new("info_Flag".to_string(), arrow::datatypes::DataType::Boolean, false),
        arrow::datatypes::Field::new("info_Info1".to_string(),arrow::datatypes::DataType::Float32, false),
        arrow::datatypes::Field::new( "info_Info_fixed".to_string(), arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new("info_Info_fixed".to_string(),arrow::datatypes::DataType::Int32, false)), ),false),
        arrow::datatypes::Field::new("info_Info_A".to_string(),arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("info_Info_RString".to_string(),arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new("info_Info_RString".to_string(),arrow::datatypes::DataType::Utf8, false)), ), false),
        arrow::datatypes::Field::new("info_Info_RChar".to_string(),arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new("info_Info_RChar".to_string(),arrow::datatypes::DataType::Utf8, false)), ), false),
        arrow::datatypes::Field::new("info_Info_G".to_string(), arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new("info_Info_G".to_string(),arrow::datatypes::DataType::Int32, false)), ), false),
        arrow::datatypes::Field::new("info_Info_.".to_string(), arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new("info_Info_.".to_string(),arrow::datatypes::DataType::Int32, false))), false)
        ];

    static ref FORMAT_COLS: Vec<arrow::datatypes::Field> = vec![
                arrow::datatypes::Field::new(
                    "format_first_Format_1".to_string(),
                    arrow::datatypes::DataType::Int32,
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_first_Format_fixed".to_string(),
                    arrow::datatypes::DataType::List(Arc::new(
                        arrow::datatypes::Field::new(
                            "format_first_Format_fixed".to_string(),
                            arrow::datatypes::DataType::Float32,
                            true,

                        )
                    ),),
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_first_Format_A".to_string(),
                    arrow::datatypes::DataType::Utf8,
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_first_Format_R".to_string(),
                    arrow::datatypes::DataType::List(Arc::new(
                        arrow::datatypes::Field::new(
                            "format_first_Format_R".to_string(),
                            arrow::datatypes::DataType::Utf8,
                            true,

                        )
                    ),),
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_first_Format_G".to_string(),
                    arrow::datatypes::DataType::List(Arc::new(
                        arrow::datatypes::Field::new(
                            "format_first_Format_G".to_string(),
                            arrow::datatypes::DataType::Int32,
                            true,

                        )
                    ),),
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_first_Format_.".to_string(),
                    arrow::datatypes::DataType::Int32,
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_second_Format_1".to_string(),
                    arrow::datatypes::DataType::Int32,
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_second_Format_fixed".to_string(),
                    arrow::datatypes::DataType::List(Arc::new(
                        arrow::datatypes::Field::new(
                            "format_second_Format_fixed".to_string(),
                            arrow::datatypes::DataType::Float32,
                            true,

                        )
                    ),),
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_second_Format_A".to_string(),
                    arrow::datatypes::DataType::Utf8,
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_second_Format_R".to_string(),
                    arrow::datatypes::DataType::List(Arc::new(
                        arrow::datatypes::Field::new(
                            "format_second_Format_R".to_string(),
                            arrow::datatypes::DataType::Utf8,
                            true,

                        )
                    ),),
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_second_Format_G".to_string(),
                    arrow::datatypes::DataType::List(Arc::new(
                        arrow::datatypes::Field::new(
                            "format_second_Format_G".to_string(),
                            arrow::datatypes::DataType::Int32,
                            true,

                        )
                    ),),
                    true,

                ),
                arrow::datatypes::Field::new(
                    "format_second_Format_.".to_string(),
                    arrow::datatypes::DataType::Int32,
                    true,

                )

            ];
    }

    #[test]
    fn mini_cols() {
        assert_eq!(required_column(), *MINI_COLS)
    }

    #[test]
    fn info_cols() {
        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap();

        assert_eq!(info(&header, false), *INFO_COLS);
    }

    #[test]
    fn genotype_cols() {
        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap();

        assert_eq!(genotype(&header), *FORMAT_COLS);
    }

    #[test]
    fn all_cols() {
        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap();

        let mut data: Vec<arrow::datatypes::Field> = Vec::new();
        data.extend_from_slice(&MINI_COLS);
        data.extend_from_slice(&INFO_COLS);
        data.extend_from_slice(&FORMAT_COLS);

        assert_eq!(
            from_header(&header, false).unwrap(),
            arrow::datatypes::Schema::new(data.iter().map(|x| x.clone()).collect::<Vec<_>>()),
        );
    }
}
