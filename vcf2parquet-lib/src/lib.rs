//! vcf2parquet library

/* std use */

/* crate use */

/* project use */

/* mod section */
pub mod error;
pub mod name2data;
pub mod record2chunk;
pub mod schema;

pub fn noodles2arrow<R, W>(input: &mut R, output: &mut W, batch_size: usize) -> error::Result<()>
where
    R: std::io::BufRead,
    W: std::io::Write,
{
    // VCF section
    let mut reader = noodles::vcf::Reader::new(input);

    let vcf_header: noodles::vcf::Header = reader
        .read_header()
        .map_err(error::mapping)?
        .parse()
        .map_err(error::mapping)?;

    // Parquet section
    let schema = schema::from_header(&vcf_header)?;

    let chunk_iterator = record2chunk::Record2Chunk::new(
        reader.records(&vcf_header),
        batch_size,
        vcf_header.clone(),
        schema.clone(),
    );

    let options = arrow2::io::parquet::write::WriteOptions {
        write_statistics: true,
        compression: arrow2::io::parquet::write::Compression::Snappy,
        version: arrow2::io::parquet::write::Version::V2,
    };

    let encodings = chunk_iterator.encodings();
    let row_groups = arrow2::io::parquet::write::RowGroupIterator::try_new(
        chunk_iterator,
        &schema,
        options,
        encodings,
    )?;

    let mut writer = arrow2::io::parquet::write::FileWriter::try_new(output, schema, options)?;

    writer.start()?;
    for group in row_groups {
        let (group, len) = group?;
        writer.write(group, len)?;
    }
    let _ = writer.end(None)?;

    Ok(())
}
