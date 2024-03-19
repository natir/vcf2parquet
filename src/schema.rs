//! Construct parquet schema corresponding to vcf

/* std use */

/* crate use */

/* project use */
use crate::*;

/// Generate a parquet schema corresponding to vcf header
pub fn from_header(
    header: &noodles::vcf::Header,
    info_optional: bool,
) -> error::Result<arrow2::datatypes::Schema> {
    let mut columns = Vec::new();

    // required column
    columns.extend(required_column());

    // info field
    columns.extend(info(header, info_optional));

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
        arrow2::datatypes::Field::new("alternate", arrow2::datatypes::DataType::Utf8, false),
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

fn info(header: &noodles::vcf::Header, info_optional: bool) -> Vec<arrow2::datatypes::Field> {
    let mut fields = Vec::new();

    for (name, value) in header.infos() {
        let key = format!("info_{name}");

        let arrow_type = match value.ty() {
            noodles::vcf::header::record::value::map::info::Type::Integer => {
                arrow2::datatypes::DataType::Int32
            }
            noodles::vcf::header::record::value::map::info::Type::Float => {
                arrow2::datatypes::DataType::Float32
            }
            noodles::vcf::header::record::value::map::info::Type::Flag => {
                arrow2::datatypes::DataType::Boolean
            }
            noodles::vcf::header::record::value::map::info::Type::Character => {
                arrow2::datatypes::DataType::Utf8
            }
            noodles::vcf::header::record::value::map::info::Type::String => {
                arrow2::datatypes::DataType::Utf8
            }
        };

        match value.number() {
            noodles::vcf::header::Number::Count(0 | 1) | noodles::vcf::header::Number::A => fields
                .push(arrow2::datatypes::Field::new(
                    &key,
                    arrow_type,
                    info_optional,
                )),
            noodles::vcf::header::Number::R => fields.push(arrow2::datatypes::Field::new(
                &key,
                arrow2::datatypes::DataType::FixedSizeList(
                    Box::new(arrow2::datatypes::Field::new(
                        &key,
                        arrow_type,
                        info_optional,
                    )),
                    2,
                ),
                info_optional,
            )),
            noodles::vcf::header::Number::Count(n) => fields.push(arrow2::datatypes::Field::new(
                &key,
                arrow2::datatypes::DataType::FixedSizeList(
                    Box::new(arrow2::datatypes::Field::new(
                        &key,
                        arrow_type,
                        info_optional,
                    )),
                    n,
                ),
                false,
            )),
            noodles::vcf::header::Number::G => fields.push(arrow2::datatypes::Field::new(
                &key,
                arrow2::datatypes::DataType::FixedSizeList(
                    Box::new(arrow2::datatypes::Field::new(
                        &key,
                        arrow_type,
                        info_optional,
                    )),
                    3,
                ),
                false,
            )),

            noodles::vcf::header::Number::Unknown => fields.push(arrow2::datatypes::Field::new(
                &key,
                arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                    &key,
                    arrow_type,
                    info_optional,
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
            let key = format!("format_{sample}_{name}");

            let arrow_type = match value.ty() {
                noodles::vcf::header::record::value::map::format::Type::Integer => {
                    arrow2::datatypes::DataType::Int32
                }
                noodles::vcf::header::record::value::map::format::Type::Float => {
                    arrow2::datatypes::DataType::Float32
                }
                noodles::vcf::header::record::value::map::format::Type::Character => {
                    arrow2::datatypes::DataType::Utf8
                }
                noodles::vcf::header::record::value::map::format::Type::String => {
                    arrow2::datatypes::DataType::Utf8
                }
            };

            match value.number() {
                noodles::vcf::header::Number::Count(0 | 1) | noodles::vcf::header::Number::A => {
                    fields.push(arrow2::datatypes::Field::new(key, arrow_type, false))
                }
                noodles::vcf::header::Number::R => fields.push(arrow2::datatypes::Field::new(
                    &key,
                    arrow2::datatypes::DataType::FixedSizeList(
                        Box::new(arrow2::datatypes::Field::new(&key, arrow_type, false)),
                        2,
                    ),
                    false,
                )),
                noodles::vcf::header::Number::Count(n) => {
                    fields.push(arrow2::datatypes::Field::new(
                        &key,
                        arrow2::datatypes::DataType::FixedSizeList(
                            Box::new(arrow2::datatypes::Field::new(&key, arrow_type, false)),
                            n,
                        ),
                        false,
                    ))
                }
                noodles::vcf::header::Number::G => fields.push(arrow2::datatypes::Field::new(
                    &key,
                    arrow2::datatypes::DataType::FixedSizeList(
                        Box::new(arrow2::datatypes::Field::new(&key, arrow_type, false)),
                        3,
                    ),
                    false,
                )),

                noodles::vcf::header::Number::Unknown => {
                    fields.push(arrow2::datatypes::Field::new(
                        &key,
                        arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                            &key, arrow_type, false,
                        ))),
                        false,
                    ))
                }
            }
        }
    }

    fields
}

#[cfg(test)]
mod tests {
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
        static ref MINI_COLS: Vec<arrow2::datatypes::Field> = vec![
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
            arrow2::datatypes::Field::new("alternate", arrow2::datatypes::DataType::Utf8, false),
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
        ];

    static ref INFO_COLS: Vec<arrow2::datatypes::Field> = vec![
        arrow2::datatypes::Field { name: "info_Flag".to_string(), data_type: arrow2::datatypes::DataType::Boolean, is_nullable: false, metadata: std::collections::BTreeMap::new() }, arrow2::datatypes::Field { name: "info_Info1".to_string(), data_type: arrow2::datatypes::DataType::Float32, is_nullable: false, metadata: std::collections::BTreeMap::new() }, arrow2::datatypes::Field { name: "info_Info_fixed".to_string(), data_type: arrow2::datatypes::DataType::FixedSizeList(Box::new(arrow2::datatypes::Field { name: "info_Info_fixed".to_string(), data_type: arrow2::datatypes::DataType::Int32, is_nullable: false, metadata: std::collections::BTreeMap::new() }), 3), is_nullable: false, metadata: std::collections::BTreeMap::new() }, arrow2::datatypes::Field { name: "info_Info_A".to_string(), data_type: arrow2::datatypes::DataType::Int32, is_nullable: false, metadata: std::collections::BTreeMap::new() }, arrow2::datatypes::Field { name: "info_Info_RString".to_string(), data_type: arrow2::datatypes::DataType::FixedSizeList(Box::new(arrow2::datatypes::Field { name: "info_Info_RString".to_string(), data_type: arrow2::datatypes::DataType::Utf8, is_nullable: false, metadata: std::collections::BTreeMap::new() }), 2), is_nullable: false, metadata: std::collections::BTreeMap::new() }, arrow2::datatypes::Field { name: "info_Info_RChar".to_string(), data_type: arrow2::datatypes::DataType::FixedSizeList(Box::new(arrow2::datatypes::Field { name: "info_Info_RChar".to_string(), data_type: arrow2::datatypes::DataType::Utf8, is_nullable: false, metadata: std::collections::BTreeMap::new() }), 2), is_nullable: false, metadata: std::collections::BTreeMap::new() }, arrow2::datatypes::Field { name: "info_Info_G".to_string(), data_type: arrow2::datatypes::DataType::FixedSizeList(Box::new(arrow2::datatypes::Field { name: "info_Info_G".to_string(), data_type: arrow2::datatypes::DataType::Int32, is_nullable: false, metadata: std::collections::BTreeMap::new() }), 3), is_nullable: false, metadata: std::collections::BTreeMap::new() }, arrow2::datatypes::Field { name: "info_Info_.".to_string(), data_type: arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field { name: "info_Info_.".to_string(), data_type: arrow2::datatypes::DataType::Int32, is_nullable: false, metadata: std::collections::BTreeMap::new() })), is_nullable: false, metadata: std::collections::BTreeMap::new() }];

    static ref FORMAT_COLS: Vec<arrow2::datatypes::Field> = vec![
                arrow2::datatypes::Field {
                    name: "format_first_Format_1".to_string(),
                    data_type: arrow2::datatypes::DataType::Int32,
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_first_Format_fixed".to_string(),
                    data_type: arrow2::datatypes::DataType::FixedSizeList(Box::new(
                        arrow2::datatypes::Field {
                            name: "format_first_Format_fixed".to_string(),
                            data_type: arrow2::datatypes::DataType::Float32,
                            is_nullable: false,
                            metadata: std::collections::BTreeMap::new()
                        }
                    ),4),
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_first_Format_A".to_string(),
                    data_type: arrow2::datatypes::DataType::Utf8,
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_first_Format_R".to_string(),
                    data_type: arrow2::datatypes::DataType::FixedSizeList(Box::new(
                        arrow2::datatypes::Field {
                            name: "format_first_Format_R".to_string(),
                            data_type: arrow2::datatypes::DataType::Utf8,
                            is_nullable: false,
                            metadata: std::collections::BTreeMap::new()
                        }
                    ),2),
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_first_Format_G".to_string(),
                    data_type: arrow2::datatypes::DataType::FixedSizeList(Box::new(
                        arrow2::datatypes::Field {
                            name: "format_first_Format_G".to_string(),
                            data_type: arrow2::datatypes::DataType::Int32,
                            is_nullable: false,
                            metadata: std::collections::BTreeMap::new()
                        }
                    ),3),
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_first_Format_.".to_string(),
                    data_type: arrow2::datatypes::DataType::Int32,
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_second_Format_1".to_string(),
                    data_type: arrow2::datatypes::DataType::Int32,
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_second_Format_fixed".to_string(),
                    data_type: arrow2::datatypes::DataType::FixedSizeList(Box::new(
                        arrow2::datatypes::Field {
                            name: "format_second_Format_fixed".to_string(),
                            data_type: arrow2::datatypes::DataType::Float32,
                            is_nullable: false,
                            metadata: std::collections::BTreeMap::new()
                        }
                    ),4),
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_second_Format_A".to_string(),
                    data_type: arrow2::datatypes::DataType::Utf8,
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_second_Format_R".to_string(),
                    data_type: arrow2::datatypes::DataType::FixedSizeList(Box::new(
                        arrow2::datatypes::Field {
                            name: "format_second_Format_R".to_string(),
                            data_type: arrow2::datatypes::DataType::Utf8,
                            is_nullable: false,
                            metadata: std::collections::BTreeMap::new()
                        }
                    ),2),
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_second_Format_G".to_string(),
                    data_type: arrow2::datatypes::DataType::FixedSizeList(Box::new(
                        arrow2::datatypes::Field {
                            name: "format_second_Format_G".to_string(),
                            data_type: arrow2::datatypes::DataType::Int32,
                            is_nullable: false,
                            metadata: std::collections::BTreeMap::new()
                        }
                    ),3),
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_second_Format_.".to_string(),
                    data_type: arrow2::datatypes::DataType::Int32,
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                }

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

        let mut data: Vec<arrow2::datatypes::Field> = Vec::new();
        data.extend_from_slice(&*MINI_COLS);
        data.extend_from_slice(&*INFO_COLS);
        data.extend_from_slice(&*FORMAT_COLS);

        assert_eq!(
            from_header(&header, false).unwrap(),
            arrow2::datatypes::Schema::from(data)
        );
    }
}
