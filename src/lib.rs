//! vcf2parquet library

#![warn(missing_docs)]
/* std use */

/* crate use */
use parquet::file::properties::WriterVersion;
/* project use */

/* mod section */
pub mod cli;
pub mod columndata;
pub mod error;
pub mod name2data;
pub mod record2chunk;
pub mod schema;

/// Read `input` vcf and write parquet in `output`
pub fn vcf2parquet<R, W>(
    input: &mut R,
    output: &mut W,
    batch_size: usize,
    compression: parquet::basic::Compression,
    info_optional: bool,
    parquet_version: WriterVersion,
) -> error::Result<()>
where
    R: std::io::BufRead,
    W: std::io::Write + std::marker::Send,
{
    // VCF section
    let mut reader = noodles::vcf::Reader::new(input);

    let vcf_header: noodles::vcf::Header = reader.read_header()?;

    // Parquet section
    let schema = schema::from_header(&vcf_header, info_optional)?;
    let schema_ptr = std::sync::Arc::new(schema);

    let mut iterator = reader.records(&vcf_header);
    let chunk_iterator = record2chunk::Record2Chunk::new(
        &mut iterator,
        batch_size,
        vcf_header.clone(),
        schema_ptr.clone(),
    );

    let options = parquet::file::properties::WriterProperties::builder()
        .set_compression(compression)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .set_writer_version(parquet_version)
        .set_write_batch_size(batch_size)
        .build();

    let row_groups = arrow::array::RecordBatchIterator::new(chunk_iterator, schema_ptr.clone());

    let mut writer =
        parquet::arrow::ArrowWriter::try_new(output, schema_ptr.clone(), Some(options))?;

    for result in row_groups {
        let group = result?;
        writer.write(&group)?;
    }
    let _ = writer.close()?;

    Ok(())
}

/// Read `input` vcf and write each row group in a parquet file match with template
pub fn vcf2multiparquet<R>(
    input: &mut R,
    template: &str,
    batch_size: usize,
    compression: parquet::basic::Compression,
    info_optional: bool,
    parquet_version: WriterVersion,
) -> error::Result<()>
where
    R: std::io::BufRead,
{
    // VCF section
    let mut reader = noodles::vcf::Reader::new(input);

    let vcf_header: noodles::vcf::Header = reader.read_header()?;

    // Parquet section
    let schema = schema::from_header(&vcf_header, info_optional)?;
    let schema_ptr = std::sync::Arc::new(schema);

    let mut iterator = reader.records(&vcf_header);
    let chunk_iterator = record2chunk::Record2Chunk::new(
        &mut iterator,
        batch_size,
        vcf_header.clone(),
        schema_ptr.clone(),
    );

    let options = parquet::file::properties::WriterProperties::builder()
        .set_compression(compression)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .set_writer_version(parquet_version)
        .set_write_batch_size(batch_size)
        .build();

    let row_groups = arrow::array::RecordBatchIterator::new(chunk_iterator, schema_ptr.clone());

    for (index, result) in row_groups.enumerate() {
        let group = result?;
        let output = std::fs::File::create(template.replace("{}", &index.to_string()))?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            output,
            schema_ptr.clone(),
            Some(options.clone()),
        )?;

        writer.write(&group)?;
        writer.close()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    static VCF_FILE: &[u8] = b"##fileformat=VCFv4.3
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
1\t925952\t1019397\tG\tA\t.\t.\t.
";

    static PARQUET_FILE: &[u8] = &[
        80, 65, 82, 49, 21, 4, 21, 10, 21, 50, 76, 21, 2, 21, 0, 18, 0, 0, 31, 139, 8, 0, 0, 0, 0,
        0, 0, 255, 99, 100, 96, 96, 48, 4, 0, 151, 222, 156, 170, 5, 0, 0, 0, 21, 6, 21, 4, 21, 44,
        92, 21, 2, 21, 0, 21, 2, 21, 16, 21, 0, 21, 0, 17, 28, 54, 0, 40, 1, 49, 24, 1, 49, 17, 17,
        0, 0, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 99, 96, 2, 0, 211, 115, 215, 175, 2, 0, 0, 0,
        21, 4, 21, 8, 21, 48, 76, 21, 2, 21, 0, 18, 0, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 99,
        80, 228, 99, 0, 0, 69, 222, 72, 134, 4, 0, 0, 0, 21, 6, 21, 4, 21, 44, 92, 21, 2, 21, 0,
        21, 2, 21, 16, 21, 0, 21, 0, 17, 28, 54, 0, 40, 4, 0, 33, 14, 0, 24, 4, 0, 33, 14, 0, 17,
        17, 0, 0, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 99, 96, 2, 0, 211, 115, 215, 175, 2, 0, 0,
        0, 21, 4, 21, 22, 21, 62, 76, 21, 2, 21, 0, 18, 0, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255,
        99, 103, 96, 96, 48, 52, 48, 180, 52, 182, 52, 7, 0, 69, 88, 164, 201, 11, 0, 0, 0, 21, 6,
        21, 12, 21, 52, 92, 21, 2, 21, 0, 21, 2, 21, 16, 21, 4, 21, 4, 17, 28, 54, 0, 40, 7, 49,
        48, 49, 57, 51, 57, 55, 24, 7, 49, 48, 49, 57, 51, 57, 55, 17, 17, 0, 0, 0, 2, 0, 2, 1, 31,
        139, 8, 0, 0, 0, 0, 0, 0, 255, 99, 96, 2, 0, 211, 115, 215, 175, 2, 0, 0, 0, 21, 4, 21, 10,
        21, 50, 76, 21, 2, 21, 0, 18, 0, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 99, 100, 96, 96,
        112, 7, 0, 158, 10, 250, 19, 5, 0, 0, 0, 21, 6, 21, 4, 21, 44, 92, 21, 2, 21, 0, 21, 2, 21,
        16, 21, 0, 21, 0, 17, 28, 54, 0, 40, 1, 71, 24, 1, 71, 17, 17, 0, 0, 0, 31, 139, 8, 0, 0,
        0, 0, 0, 0, 255, 99, 96, 2, 0, 211, 115, 215, 175, 2, 0, 0, 0, 21, 4, 21, 10, 21, 50, 76,
        21, 2, 21, 0, 18, 0, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 99, 100, 96, 96, 112, 4, 0, 171,
        175, 153, 250, 5, 0, 0, 0, 21, 6, 21, 4, 21, 44, 92, 21, 2, 21, 0, 21, 2, 21, 16, 21, 0,
        21, 0, 17, 28, 54, 0, 40, 1, 65, 24, 1, 65, 17, 17, 0, 0, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0,
        255, 99, 96, 2, 0, 211, 115, 215, 175, 2, 0, 0, 0, 21, 4, 21, 0, 21, 40, 76, 21, 0, 21, 0,
        18, 0, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 6, 21, 6,
        21, 46, 92, 21, 2, 21, 2, 21, 2, 21, 16, 21, 4, 21, 0, 17, 0, 0, 2, 0, 31, 139, 8, 0, 0, 0,
        0, 0, 0, 255, 99, 0, 0, 141, 239, 2, 210, 1, 0, 0, 0, 21, 4, 21, 0, 21, 40, 76, 21, 0, 21,
        0, 18, 0, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 6, 21,
        10, 21, 50, 92, 21, 2, 21, 2, 21, 2, 21, 16, 21, 4, 21, 4, 17, 0, 0, 2, 0, 2, 0, 31, 139,
        8, 0, 0, 0, 0, 0, 0, 255, 99, 0, 0, 141, 239, 2, 210, 1, 0, 0, 0, 25, 17, 2, 25, 24, 1, 49,
        25, 24, 1, 49, 21, 2, 25, 22, 0, 0, 25, 17, 2, 25, 24, 4, 0, 33, 14, 0, 25, 24, 4, 0, 33,
        14, 0, 21, 2, 25, 22, 0, 0, 25, 17, 2, 25, 24, 7, 49, 48, 49, 57, 51, 57, 55, 25, 24, 7,
        49, 48, 49, 57, 51, 57, 55, 21, 2, 25, 22, 0, 25, 38, 2, 0, 25, 38, 0, 2, 0, 25, 17, 2, 25,
        24, 1, 71, 25, 24, 1, 71, 21, 2, 25, 22, 0, 0, 25, 17, 2, 25, 24, 1, 65, 25, 24, 1, 65, 21,
        2, 25, 22, 0, 0, 25, 17, 1, 25, 24, 0, 25, 24, 0, 21, 2, 25, 22, 2, 41, 38, 2, 0, 0, 25,
        17, 1, 25, 24, 0, 25, 24, 0, 21, 2, 25, 22, 2, 25, 38, 2, 0, 25, 38, 2, 0, 0, 25, 28, 22,
        86, 21, 112, 22, 0, 0, 25, 22, 2, 0, 25, 28, 22, 146, 2, 21, 124, 22, 0, 0, 0, 25, 28, 22,
        232, 3, 21, 144, 1, 22, 0, 0, 25, 22, 14, 0, 25, 28, 22, 198, 5, 21, 112, 22, 0, 0, 25, 22,
        2, 0, 25, 28, 22, 132, 7, 21, 112, 22, 0, 0, 25, 22, 2, 0, 25, 28, 22, 184, 8, 21, 90, 22,
        0, 0, 0, 25, 28, 22, 214, 9, 21, 94, 22, 0, 0, 25, 22, 0, 0, 21, 4, 25, 204, 72, 12, 97,
        114, 114, 111, 119, 95, 115, 99, 104, 101, 109, 97, 21, 14, 0, 21, 12, 37, 0, 24, 10, 99,
        104, 114, 111, 109, 111, 115, 111, 109, 101, 37, 0, 76, 28, 0, 0, 0, 21, 2, 37, 0, 24, 8,
        112, 111, 115, 105, 116, 105, 111, 110, 0, 53, 0, 24, 10, 105, 100, 101, 110, 116, 105,
        102, 105, 101, 114, 21, 2, 21, 6, 76, 60, 0, 0, 0, 53, 4, 24, 4, 108, 105, 115, 116, 21, 2,
        0, 21, 12, 37, 0, 24, 10, 105, 100, 101, 110, 116, 105, 102, 105, 101, 114, 37, 0, 76, 28,
        0, 0, 0, 21, 12, 37, 0, 24, 9, 114, 101, 102, 101, 114, 101, 110, 99, 101, 37, 0, 76, 28,
        0, 0, 0, 21, 12, 37, 0, 24, 9, 97, 108, 116, 101, 114, 110, 97, 116, 101, 37, 0, 76, 28, 0,
        0, 0, 21, 8, 37, 2, 24, 7, 113, 117, 97, 108, 105, 116, 121, 0, 53, 0, 24, 6, 102, 105,
        108, 116, 101, 114, 21, 2, 21, 6, 76, 60, 0, 0, 0, 53, 4, 24, 4, 108, 105, 115, 116, 21, 2,
        0, 21, 12, 37, 0, 24, 6, 102, 105, 108, 116, 101, 114, 37, 0, 76, 28, 0, 0, 0, 22, 2, 25,
        28, 25, 124, 38, 0, 28, 21, 12, 25, 53, 0, 6, 16, 25, 24, 10, 99, 104, 114, 111, 109, 111,
        115, 111, 109, 101, 21, 4, 22, 2, 22, 110, 22, 190, 1, 38, 86, 38, 8, 28, 54, 0, 40, 1, 49,
        24, 1, 49, 17, 17, 0, 76, 22, 2, 0, 0, 22, 230, 12, 21, 26, 22, 180, 10, 21, 34, 0, 38, 0,
        28, 21, 2, 25, 53, 0, 6, 16, 25, 24, 8, 112, 111, 115, 105, 116, 105, 111, 110, 21, 4, 22,
        2, 22, 120, 22, 200, 1, 38, 146, 2, 38, 198, 1, 28, 24, 4, 0, 33, 14, 0, 24, 4, 0, 33, 14,
        0, 22, 0, 40, 4, 0, 33, 14, 0, 24, 4, 0, 33, 14, 0, 17, 17, 0, 0, 22, 128, 13, 21, 22, 22,
        214, 10, 21, 46, 0, 38, 0, 28, 21, 12, 25, 53, 0, 6, 16, 25, 56, 10, 105, 100, 101, 110,
        116, 105, 102, 105, 101, 114, 4, 108, 105, 115, 116, 10, 105, 100, 101, 110, 116, 105, 102,
        105, 101, 114, 21, 4, 22, 2, 22, 154, 1, 22, 234, 1, 38, 232, 3, 38, 142, 3, 28, 54, 0, 40,
        7, 49, 48, 49, 57, 51, 57, 55, 24, 7, 49, 48, 49, 57, 51, 57, 55, 17, 17, 0, 76, 22, 14,
        25, 38, 2, 0, 25, 38, 0, 2, 0, 0, 22, 150, 13, 21, 30, 22, 132, 11, 21, 74, 0, 38, 0, 28,
        21, 12, 25, 53, 0, 6, 16, 25, 24, 9, 114, 101, 102, 101, 114, 101, 110, 99, 101, 21, 4, 22,
        2, 22, 110, 22, 190, 1, 38, 198, 5, 38, 248, 4, 28, 54, 0, 40, 1, 71, 24, 1, 71, 17, 17, 0,
        76, 22, 2, 0, 0, 22, 180, 13, 21, 28, 22, 206, 11, 21, 34, 0, 38, 0, 28, 21, 12, 25, 53, 0,
        6, 16, 25, 24, 9, 97, 108, 116, 101, 114, 110, 97, 116, 101, 21, 4, 22, 2, 22, 110, 22,
        190, 1, 38, 132, 7, 38, 182, 6, 28, 54, 0, 40, 1, 65, 24, 1, 65, 17, 17, 0, 76, 22, 2, 0,
        0, 22, 208, 13, 21, 28, 22, 240, 11, 21, 34, 0, 38, 0, 28, 21, 8, 25, 53, 0, 6, 16, 25, 24,
        7, 113, 117, 97, 108, 105, 116, 121, 21, 4, 22, 2, 22, 78, 22, 158, 1, 38, 184, 8, 38, 244,
        7, 28, 54, 2, 66, 18, 0, 76, 57, 38, 2, 0, 0, 0, 22, 236, 13, 21, 22, 22, 146, 12, 21, 38,
        0, 38, 0, 28, 21, 12, 25, 53, 0, 6, 16, 25, 56, 6, 102, 105, 108, 116, 101, 114, 4, 108,
        105, 115, 116, 6, 102, 105, 108, 116, 101, 114, 21, 4, 22, 2, 22, 82, 22, 162, 1, 38, 214,
        9, 38, 146, 9, 28, 54, 2, 66, 18, 0, 76, 22, 0, 25, 38, 2, 0, 25, 38, 2, 0, 0, 0, 22, 130,
        14, 21, 28, 22, 184, 12, 21, 46, 0, 22, 252, 5, 22, 2, 38, 8, 22, 172, 10, 20, 0, 0, 25,
        28, 24, 12, 65, 82, 82, 79, 87, 58, 115, 99, 104, 101, 109, 97, 24, 208, 5, 47, 47, 47, 47,
        47, 120, 81, 67, 65, 65, 65, 81, 65, 65, 65, 65, 65, 65, 65, 75, 65, 65, 119, 65, 67, 103,
        65, 74, 65, 65, 81, 65, 67, 103, 65, 65, 65, 66, 65, 65, 65, 65, 65, 65, 65, 81, 81, 65,
        67, 65, 65, 73, 65, 65, 65, 65, 66, 65, 65, 73, 65, 65, 65, 65, 66, 65, 65, 65, 65, 65, 99,
        65, 65, 65, 67, 48, 65, 81, 65, 65, 90, 65, 69, 65, 65, 65, 81, 66, 65, 65, 68, 85, 65, 65,
        65, 65, 112, 65, 65, 65, 65, 71, 119, 65, 65, 65, 65, 69, 65, 65, 65, 65, 101, 80, 55, 47,
        47, 120, 103, 65, 65, 65, 65, 77, 65, 65, 65, 65, 65, 65, 65, 65, 68, 68, 103, 65, 65, 65,
        65, 66, 65, 65, 65, 65, 67, 65, 65, 65, 65, 71, 122, 43, 47, 47, 43, 89, 47, 118, 47, 47,
        70, 65, 65, 65, 65, 65, 119, 65, 65, 65, 65, 65, 65, 65, 65, 70, 68, 65, 65, 65, 65, 65,
        65, 65, 65, 65, 67, 73, 47, 118, 47, 47, 66, 103, 65, 65, 65, 71, 90, 112, 98, 72, 82, 108,
        99, 103, 65, 65, 66, 103, 65, 65, 65, 71, 90, 112, 98, 72, 82, 108, 99, 103, 65, 65, 69,
        65, 65, 87, 65, 66, 65, 65, 68, 103, 65, 80, 65, 65, 81, 65, 65, 65, 65, 73, 65, 66, 65,
        65, 65, 65, 65, 89, 65, 65, 65, 65, 72, 65, 65, 65, 65, 65, 65, 65, 65, 81, 77, 89, 65, 65,
        65, 65, 65, 65, 65, 71, 65, 65, 103, 65, 66, 103, 65, 71, 65, 65, 65, 65, 65, 65, 65, 66,
        65, 65, 65, 65, 65, 65, 65, 72, 65, 65, 65, 65, 99, 88, 86, 104, 98, 71, 108, 48, 101, 81,
        65, 81, 47, 47, 47, 47, 70, 65, 65, 65, 65, 65, 119, 65, 65, 65, 65, 65, 65, 65, 65, 70,
        68, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 47, 47, 47, 47, 67, 81, 65, 65, 65, 71, 70,
        115, 100, 71, 86, 121, 98, 109, 70, 48, 90, 81, 65, 65, 65, 68, 122, 47, 47, 47, 56, 85,
        65, 65, 65, 65, 68, 65, 65, 65, 65, 65, 65, 65, 65, 65, 85, 77, 65, 65, 65, 65, 65, 65, 65,
        65, 65, 67, 122, 47, 47, 47, 56, 74, 65, 65, 65, 65, 99, 109, 86, 109, 90, 88, 74, 108, 98,
        109, 78, 108, 65, 65, 65, 65, 97, 80, 47, 47, 47, 120, 103, 65, 65, 65, 65, 77, 65, 65, 65,
        65, 65, 65, 65, 65, 68, 68, 119, 65, 65, 65, 65, 66, 65, 65, 65, 65, 67, 65, 65, 65, 65,
        70, 122, 47, 47, 47, 43, 73, 47, 47, 47, 47, 70, 65, 65, 65, 65, 65, 119, 65, 65, 65, 65,
        65, 65, 65, 65, 70, 68, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 52, 47, 47, 47, 47, 67,
        103, 65, 65, 65, 71, 108, 107, 90, 87, 53, 48, 97, 87, 90, 112, 90, 88, 73, 65, 65, 65,
        111, 65, 65, 65, 66, 112, 90, 71, 86, 117, 100, 71, 108, 109, 97, 87, 86, 121, 65, 65, 68,
        69, 47, 47, 47, 47, 71, 65, 65, 65, 65, 67, 65, 65, 65, 65, 65, 65, 65, 65, 65, 67, 72, 65,
        65, 65, 65, 65, 103, 65, 68, 65, 65, 69, 65, 65, 115, 65, 67, 65, 65, 65, 65, 67, 65, 65,
        65, 65, 65, 65, 65, 65, 65, 66, 65, 65, 65, 65, 65, 65, 103, 65, 65, 65, 66, 119, 98, 51,
        78, 112, 100, 71, 108, 118, 98, 103, 65, 65, 65, 65, 65, 81, 65, 66, 81, 65, 69, 65, 65,
        65, 65, 65, 56, 65, 66, 65, 65, 65, 65, 65, 103, 65, 69, 65, 65, 65, 65, 66, 103, 65, 65,
        65, 65, 77, 65, 65, 65, 65, 65, 65, 65, 65, 66, 82, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
        66, 65, 65, 69, 65, 65, 81, 65, 65, 65, 65, 75, 65, 65, 65, 65, 89, 50, 104, 121, 98, 50,
        49, 118, 99, 50, 57, 116, 90, 81, 65, 65, 0, 24, 25, 112, 97, 114, 113, 117, 101, 116, 45,
        114, 115, 32, 118, 101, 114, 115, 105, 111, 110, 32, 53, 51, 46, 51, 46, 48, 25, 124, 28,
        0, 0, 28, 0, 0, 28, 0, 0, 28, 0, 0, 28, 0, 0, 28, 0, 0, 28, 0, 0, 0, 7, 6, 0, 0, 80, 65,
        82, 49,
    ];

    #[test]
    fn convert_positives() {
        let mut input = std::io::BufReader::new(VCF_FILE);
        let mut output = Vec::new();

        vcf2parquet(
            &mut input,
            &mut output,
            1,
            parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::try_new(6).unwrap()),
            false,
            WriterVersion::PARQUET_2_0,
        )
        .unwrap();
        assert_eq!(output, *PARQUET_FILE);
    }

    #[test]
    fn not_a_vcf() {
        let raw_data = [b'#', b'a', b'b', b'c', 255, 0x7F, b'\n'].to_vec();
        let mut input = std::io::BufReader::new(&raw_data[..]);
        let mut output = Vec::new();

        let result = vcf2parquet(
            &mut input,
            &mut output,
            1,
            parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::try_new(6).unwrap()),
            false,
            WriterVersion::PARQUET_2_0,
        );

        assert!(result.is_err());
    }

    #[test]
    fn multi_positives() {
        let mut input = std::io::BufReader::new(VCF_FILE);
        let dir = tempfile::tempdir().unwrap();

        let format = dir
            .path()
            .join("test_{}.parquet")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string();

        vcf2multiparquet(
            &mut input,
            &format,
            1,
            parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::try_new(6).unwrap()),
            false,
            WriterVersion::PARQUET_2_0,
        )
        .unwrap();
    }
}
