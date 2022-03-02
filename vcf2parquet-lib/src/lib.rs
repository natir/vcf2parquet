//! vcf2parquet library

/* std use */

/* crate use */

/* project use */

/* mod section */
pub mod block;
pub mod error;
pub mod schema;

pub fn noodles2arrow<R, W>(input: &mut R, output: W) -> error::Result<()>
where
    R: std::io::BufRead,
    W: 'static + parquet::file::writer::ParquetWriter,
{
    let mut reader = noodles::vcf::Reader::new(input);

    let vcf_header = reader
        .read_header()
        .map_err(error::mapping)?
        .parse()
        .map_err(error::mapping)?;

    let schema = std::sync::Arc::new(schema::from_header(&vcf_header)?);
    let mut records = reader.records(&vcf_header);

    let properties = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
        output,
        std::sync::Arc::clone(&schema),
        Some(properties),
    )
    .map_err(error::mapping)?;

    let mut record_in_batch = 0;
    let mut block: block::Block = block::Block::new();

    loop {
        if record_in_batch == 10_000 {
            let batch: arrow::record_batch::RecordBatch = block.try_into()?;
            block = block::Block::new();
            writer.write(&batch).map_err(error::mapping)?;
            record_in_batch = 0;
        }

        match records.next() {
            Some(result) => block.add_record(&(result?)),
            None => {
                let batch: arrow::record_batch::RecordBatch = block.try_into()?;
                writer.write(&batch).map_err(error::mapping)?;
                break;
            }
        }
    }

    writer.close().map_err(error::mapping)?;

    Ok(())
}
