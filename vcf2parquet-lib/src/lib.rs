//! vcf2parquet library

/* std use */

/* crate use */

/* project use */

/* mod section */
pub mod block;
pub mod error;
pub mod internal;
pub mod schema;

pub fn noodles2arrow<R, W>(input: &mut R, output: &mut W, batch_size: usize) -> error::Result<()>
where
    R: std::io::BufRead,
    W: std::io::Write,
{
    // VCF section
    let mut reader = noodles::vcf::Reader::new(input);

    let vcf_header = reader
        .read_header()
        .map_err(error::mapping)?
        .parse()
        .map_err(error::mapping)?;
    let mut records = reader.records(&vcf_header);

    // Parquet section
    let schema = schema::from_header(&vcf_header)?;

    std::sync::Arc::new(arrow2::datatypes::Schema {
        fields: Vec::new(),
        metadata: std::collections::BTreeMap::new(),
    });

    let options = parquet2::write::WriteOptions {
        write_statistics: true,
        compression: parquet2::compression::Compression::Snappy,
        version: parquet2::write::Version::V2,
    };

    let mut writer = parquet2::write::FileWriter::new(output, schema.clone(), options, None);

    // Read and write section
    let mut record_in_batch = 0;
    let mut block: block::Block = block::Block::new(schema.clone());

    loop {
        if record_in_batch == batch_size {
            record_in_batch = 0;
        }

        match records.next() {
            Some(result) => block.add_record(&(result?)),
            None => {
                let batch: arrow2::chunk::Chunk<std::sync::Arc<dyn arrow2::array::Array>> =
                    block.try_into()?;
                //                writer.write(&batch, batch_size).map_err(error::mapping)?;
                break;
            }
        }
    }

    Ok(())
}
