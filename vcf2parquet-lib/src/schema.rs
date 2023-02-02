//! Construct parquet schema corresponding to vcf

/* std use */

/* crate use */

/* project use */
use crate::*;

/// Generate a parquet schema corresponding to vcf header
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
        let key = format!("info_{name}");

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
            let key = format!("format_{sample}_{name}");

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

#[cfg(test)]
mod tests {
    use super::*;

    //
    //

    static VCF_FILE: &[u8] = b"##fileformat=VCFv4.3
##fileDate=20220528
##source=ClinVar
##reference=GRCh38
##INFO=<ID=ALLELEID,Number=1,Type=Integer,Description=\"the ClinVar Allele ID\">
##INFO=<ID=AF_ESP,Number=1,Type=Float,Description=\"allele frequencies from GO-ESP\">
##INFO=<ID=DBVARI,Number=0,Type=Flag,Description=\"nsv accessions from dbVar for the variant\">
##INFO=<ID=GENEINFO,Number=1,Type=Character,Description=\"Gene(s) for the variant reported as gene symbol:gene id.\">
##INFO=<ID=CLNVC,Number=2,Type=String,Description=\"Variant type\">
##FORMAT=<ID=AB,Number=R,Type=Integer,Description=\"Allelic depths for the ref and alt alleles in the order listed\">
##FORMAT=<ID=DC,Number=0,Type=Float,Description=\"Approximate read depth (reads with MQ=255 or with bad mates are filtered)\">
##FORMAT=<ID=GE,Number=1,Type=Character,Description=\"Genotype Quality\">
##FORMAT=<ID=GC,Number=3,Type=String,Description=\"Genotype\">
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
        ];

    static ref INFO_COLS: Vec<arrow2::datatypes::Field> = vec![
            arrow2::datatypes::Field {
                name: "info_ALLELEID".to_string(),
                data_type: arrow2::datatypes::DataType::Int32,
                is_nullable: false,
                metadata: std::collections::BTreeMap::new()
            },
            arrow2::datatypes::Field {
                name: "info_AF_ESP".to_string(),
                data_type: arrow2::datatypes::DataType::Float32,
                is_nullable: false,
                metadata: std::collections::BTreeMap::new()
            },
            arrow2::datatypes::Field {
                name: "info_DBVARI".to_string(),
                data_type: arrow2::datatypes::DataType::Boolean,
                is_nullable: false,
                metadata: std::collections::BTreeMap::new()
            },
            arrow2::datatypes::Field {
                name: "info_GENEINFO".to_string(),
                data_type: arrow2::datatypes::DataType::Utf8,
                is_nullable: false,
                metadata: std::collections::BTreeMap::new()
            },
            arrow2::datatypes::Field {
                name: "info_CLNVC".to_string(),
                data_type: arrow2::datatypes::DataType::List(Box::new(
                    arrow2::datatypes::Field {
                        name: "info_CLNVC".to_string(),
                        data_type: arrow2::datatypes::DataType::Utf8,
                        is_nullable: false,
                        metadata: std::collections::BTreeMap::new()
                    }
                )),
                is_nullable: false,
                metadata: std::collections::BTreeMap::new()
            }
    ];

    static ref FORMAT_COLS: Vec<arrow2::datatypes::Field> = vec![
                arrow2::datatypes::Field {
                    name: "format_first_AB".to_string(),
                    data_type: arrow2::datatypes::DataType::List(Box::new(
                        arrow2::datatypes::Field {
                            name: "format_first_AB".to_string(),
                            data_type: arrow2::datatypes::DataType::Int32,
                            is_nullable: false,
                            metadata: std::collections::BTreeMap::new()
                        }
                    )),
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_first_DC".to_string(),
                    data_type: arrow2::datatypes::DataType::Float32,
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_first_GE".to_string(),
                    data_type: arrow2::datatypes::DataType::Utf8,
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_first_GC".to_string(),
                    data_type: arrow2::datatypes::DataType::List(Box::new(
                        arrow2::datatypes::Field {
                            name: "format_first_GC".to_string(),
                            data_type: arrow2::datatypes::DataType::Utf8,
                            is_nullable: false,
                            metadata: std::collections::BTreeMap::new()
                        }
                    )),
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_second_AB".to_string(),
                    data_type: arrow2::datatypes::DataType::List(Box::new(
                        arrow2::datatypes::Field {
                            name: "format_second_AB".to_string(),
                            data_type: arrow2::datatypes::DataType::Int32,
                            is_nullable: false,
                            metadata: std::collections::BTreeMap::new()
                        }
                    )),
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_second_DC".to_string(),
                    data_type: arrow2::datatypes::DataType::Float32,
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_second_GE".to_string(),
                    data_type: arrow2::datatypes::DataType::Utf8,
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
                arrow2::datatypes::Field {
                    name: "format_second_GC".to_string(),
                    data_type: arrow2::datatypes::DataType::List(Box::new(
                        arrow2::datatypes::Field {
                            name: "format_second_GC".to_string(),
                            data_type: arrow2::datatypes::DataType::Utf8,
                            is_nullable: false,
                            metadata: std::collections::BTreeMap::new()
                        }
                    )),
                    is_nullable: false,
                    metadata: std::collections::BTreeMap::new()
                },
            ];
    }

    #[test]
    fn mini_cols() {
        assert_eq!(required_column(), *MINI_COLS)
    }

    #[test]
    fn info_cols() {
        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap().parse().unwrap();

        assert_eq!(info(&header), *INFO_COLS);
    }

    #[test]
    fn genotype_cols() {
        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap().parse().unwrap();

        assert_eq!(genotype(&header), *FORMAT_COLS);
    }

    #[test]
    fn all_cols() {
        let mut reader = noodles::vcf::Reader::new(VCF_FILE);

        let header: noodles::vcf::Header = reader.read_header().unwrap().parse().unwrap();

        let mut data: Vec<arrow2::datatypes::Field> = Vec::new();
        data.extend_from_slice(&*MINI_COLS);
        data.extend_from_slice(&*INFO_COLS);
        data.extend_from_slice(&*FORMAT_COLS);

        assert_eq!(
            from_header(&header).unwrap(),
            arrow2::datatypes::Schema::from(data)
        );
    }
}
