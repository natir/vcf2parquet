//! vcf2parquet library

/* std use */

/* crate use */

/* project use */

/* mod section */
pub mod error;
pub mod name2data;
pub mod record2chunk;
pub mod schema;

/// Read `input` vcf and write parquet in `output`
pub fn vcf2parquet<R, W>(
    input: &mut R,
    output: &mut W,
    batch_size: usize,
    compression: arrow2::io::parquet::write::CompressionOptions,
    info_optional: bool,
) -> error::Result<()>
where
    R: std::io::BufRead,
    W: std::io::Write,
{
    // VCF section
    let mut reader = noodles::vcf::Reader::new(input);

    let vcf_header: noodles::vcf::Header = reader.read_header()?;

    // Parquet section
    let schema = schema::from_header(&vcf_header, info_optional)?;

    let mut iterator = reader.records(&vcf_header);
    let chunk_iterator = record2chunk::Record2Chunk::new(
        &mut iterator,
        batch_size,
        vcf_header.clone(),
        schema.clone(),
    );

    let options = arrow2::io::parquet::write::WriteOptions {
        write_statistics: true,
        compression,
        version: arrow2::io::parquet::write::Version::V2,
        data_pagesize_limit: Some(batch_size),
    };

    let encodings = chunk_iterator.encodings();
    let row_groups = arrow2::io::parquet::write::RowGroupIterator::try_new(
        chunk_iterator,
        &schema,
        options,
        encodings,
    )?;

    let mut writer = arrow2::io::parquet::write::FileWriter::try_new(output, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    let _ = writer.end(None)?;

    Ok(())
}

/// Read `input` vcf and write each row group in a parquet file match with template
pub fn vcf2multiparquet<R>(
    input: &mut R,
    template: &str,
    batch_size: usize,
    compression: arrow2::io::parquet::write::CompressionOptions,
    info_optional: bool,
) -> error::Result<()>
where
    R: std::io::BufRead,
{
    // VCF section
    let mut reader = noodles::vcf::Reader::new(input);

    let vcf_header: noodles::vcf::Header = reader.read_header()?;

    // Parquet section
    let schema = schema::from_header(&vcf_header, info_optional)?;

    let mut iterator = reader.records(&vcf_header);
    let chunk_iterator = record2chunk::Record2Chunk::new(
        &mut iterator,
        batch_size,
        vcf_header.clone(),
        schema.clone(),
    );

    let options = arrow2::io::parquet::write::WriteOptions {
        write_statistics: true,
        compression,
        version: arrow2::io::parquet::write::Version::V2,
        data_pagesize_limit: Some(batch_size),
    };

    let encodings = chunk_iterator.encodings();
    let row_groups = arrow2::io::parquet::write::RowGroupIterator::try_new(
        chunk_iterator,
        &schema,
        options,
        encodings,
    )?;

    for (index, group) in row_groups.enumerate() {
        let output = std::fs::File::create(template.replace("{}", &index.to_string()))?;
        let mut writer =
            arrow2::io::parquet::write::FileWriter::try_new(output, schema.clone(), options)?;

        writer.write(group?)?;
        writer.end(None)?;
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
        80, 65, 82, 49, 21, 6, 21, 10, 21, 50, 92, 21, 2, 21, 0, 21, 2, 21, 0, 21, 0, 21, 0, 17,
        28, 54, 0, 40, 1, 49, 24, 1, 49, 0, 0, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 99, 100, 96,
        96, 48, 4, 0, 151, 222, 156, 170, 5, 0, 0, 0, 21, 12, 25, 37, 0, 6, 25, 24, 10, 99, 104,
        114, 111, 109, 111, 115, 111, 109, 101, 21, 4, 22, 2, 22, 74, 22, 114, 38, 8, 60, 54, 0,
        40, 1, 49, 24, 1, 49, 0, 0, 21, 6, 21, 8, 21, 48, 92, 21, 2, 21, 0, 21, 2, 21, 0, 21, 0,
        21, 0, 17, 28, 54, 0, 40, 4, 0, 33, 14, 0, 24, 4, 0, 33, 14, 0, 0, 0, 0, 31, 139, 8, 0, 0,
        0, 0, 0, 0, 255, 99, 80, 228, 99, 0, 0, 69, 222, 72, 134, 4, 0, 0, 0, 21, 2, 25, 37, 0, 6,
        25, 24, 8, 112, 111, 115, 105, 116, 105, 111, 110, 21, 4, 22, 2, 22, 84, 22, 124, 38, 202,
        1, 60, 54, 0, 40, 4, 0, 33, 14, 0, 24, 4, 0, 33, 14, 0, 0, 0, 21, 6, 21, 30, 21, 70, 92,
        21, 2, 21, 0, 21, 2, 21, 0, 21, 4, 21, 4, 17, 28, 54, 0, 40, 7, 49, 48, 49, 57, 51, 57, 55,
        24, 7, 49, 48, 49, 57, 51, 57, 55, 0, 0, 0, 3, 0, 3, 1, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255,
        99, 103, 96, 96, 48, 52, 48, 180, 52, 182, 52, 7, 0, 69, 88, 164, 201, 11, 0, 0, 0, 21, 12,
        25, 37, 0, 6, 25, 56, 10, 105, 100, 101, 110, 116, 105, 102, 105, 101, 114, 4, 108, 105,
        115, 116, 2, 105, 100, 21, 4, 22, 2, 22, 118, 22, 158, 1, 38, 160, 3, 60, 54, 0, 40, 7, 49,
        48, 49, 57, 51, 57, 55, 24, 7, 49, 48, 49, 57, 51, 57, 55, 0, 0, 21, 6, 21, 10, 21, 50, 92,
        21, 2, 21, 0, 21, 2, 21, 0, 21, 0, 21, 0, 17, 28, 54, 0, 40, 1, 71, 24, 1, 71, 0, 0, 0, 31,
        139, 8, 0, 0, 0, 0, 0, 0, 255, 99, 100, 96, 96, 112, 7, 0, 158, 10, 250, 19, 5, 0, 0, 0,
        21, 12, 25, 37, 0, 6, 25, 24, 9, 114, 101, 102, 101, 114, 101, 110, 99, 101, 21, 4, 22, 2,
        22, 74, 22, 114, 38, 186, 5, 60, 54, 0, 40, 1, 71, 24, 1, 71, 0, 0, 21, 6, 21, 10, 21, 50,
        92, 21, 2, 21, 0, 21, 2, 21, 0, 21, 0, 21, 0, 17, 28, 54, 0, 40, 1, 65, 24, 1, 65, 0, 0, 0,
        31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 99, 100, 96, 96, 112, 4, 0, 171, 175, 153, 250, 5, 0, 0,
        0, 21, 12, 25, 37, 0, 6, 25, 24, 9, 97, 108, 116, 101, 114, 110, 97, 116, 101, 21, 4, 22,
        2, 22, 74, 22, 114, 38, 252, 6, 60, 54, 0, 40, 1, 65, 24, 1, 65, 0, 0, 21, 6, 21, 4, 21,
        44, 92, 21, 2, 21, 2, 21, 2, 21, 0, 21, 4, 21, 0, 17, 28, 54, 2, 0, 0, 0, 3, 0, 31, 139, 8,
        0, 0, 0, 0, 0, 0, 255, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 8, 25, 37, 0, 6, 25, 24, 7, 113,
        117, 97, 108, 105, 116, 121, 21, 4, 22, 2, 22, 56, 22, 96, 38, 190, 8, 60, 54, 2, 0, 0, 21,
        6, 21, 8, 21, 48, 92, 21, 2, 21, 0, 21, 2, 21, 0, 21, 4, 21, 4, 17, 28, 54, 0, 0, 0, 0, 3,
        0, 3, 0, 31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 12, 25, 37,
        0, 6, 25, 56, 6, 102, 105, 108, 116, 101, 114, 4, 108, 105, 115, 116, 6, 102, 105, 108,
        116, 101, 114, 21, 4, 22, 2, 22, 60, 22, 100, 38, 222, 9, 60, 54, 0, 0, 0, 21, 12, 25, 5,
        25, 24, 10, 99, 104, 114, 111, 109, 111, 115, 111, 109, 101, 21, 0, 22, 0, 22, 0, 22, 0,
        38, 0, 0, 21, 2, 25, 5, 25, 24, 8, 112, 111, 115, 105, 116, 105, 111, 110, 21, 0, 22, 0,
        22, 0, 22, 0, 38, 0, 0, 21, 12, 25, 5, 25, 56, 10, 105, 100, 101, 110, 116, 105, 102, 105,
        101, 114, 4, 108, 105, 115, 116, 2, 105, 100, 21, 0, 22, 0, 22, 0, 22, 0, 38, 0, 0, 21, 12,
        25, 5, 25, 24, 9, 114, 101, 102, 101, 114, 101, 110, 99, 101, 21, 0, 22, 0, 22, 0, 22, 0,
        38, 0, 0, 21, 12, 25, 5, 25, 24, 9, 97, 108, 116, 101, 114, 110, 97, 116, 101, 21, 0, 22,
        0, 22, 0, 22, 0, 38, 0, 0, 21, 8, 25, 5, 25, 24, 7, 113, 117, 97, 108, 105, 116, 121, 21,
        0, 22, 0, 22, 0, 22, 0, 38, 0, 0, 21, 12, 25, 5, 25, 56, 6, 102, 105, 108, 116, 101, 114,
        4, 108, 105, 115, 116, 6, 102, 105, 108, 116, 101, 114, 21, 0, 22, 0, 22, 0, 22, 0, 38, 0,
        0, 25, 17, 2, 25, 24, 1, 49, 25, 24, 1, 49, 21, 0, 25, 22, 0, 0, 25, 17, 2, 25, 24, 4, 0,
        33, 14, 0, 25, 24, 4, 0, 33, 14, 0, 21, 0, 25, 22, 0, 0, 25, 17, 2, 25, 24, 7, 49, 48, 49,
        57, 51, 57, 55, 25, 24, 7, 49, 48, 49, 57, 51, 57, 55, 21, 0, 25, 22, 0, 0, 25, 17, 2, 25,
        24, 1, 71, 25, 24, 1, 71, 21, 0, 25, 22, 0, 0, 25, 17, 2, 25, 24, 1, 65, 25, 24, 1, 65, 21,
        0, 25, 22, 0, 0, 25, 17, 1, 25, 24, 1, 0, 25, 24, 1, 0, 21, 0, 25, 22, 2, 0, 25, 17, 1, 25,
        24, 1, 0, 25, 24, 1, 0, 21, 0, 25, 22, 0, 0, 25, 1, 25, 8, 25, 8, 21, 0, 25, 6, 0, 25, 1,
        25, 8, 25, 8, 21, 0, 25, 6, 0, 25, 1, 25, 8, 25, 8, 21, 0, 25, 6, 0, 25, 1, 25, 8, 25, 8,
        21, 0, 25, 6, 0, 25, 1, 25, 8, 25, 8, 21, 0, 25, 6, 0, 25, 1, 25, 8, 25, 8, 21, 0, 25, 6,
        0, 25, 1, 25, 8, 25, 8, 21, 0, 25, 6, 0, 25, 28, 22, 8, 21, 114, 22, 0, 0, 0, 25, 28, 22,
        202, 1, 21, 124, 22, 0, 0, 0, 25, 28, 22, 160, 3, 21, 158, 1, 22, 0, 0, 0, 25, 28, 22, 186,
        5, 21, 114, 22, 0, 0, 0, 25, 28, 22, 252, 6, 21, 114, 22, 0, 0, 0, 25, 28, 22, 190, 8, 21,
        96, 22, 0, 0, 0, 25, 28, 22, 222, 9, 21, 100, 22, 0, 0, 0, 25, 12, 0, 25, 12, 0, 25, 12, 0,
        25, 12, 0, 25, 12, 0, 25, 12, 0, 25, 12, 0, 21, 4, 25, 204, 72, 4, 114, 111, 111, 116, 21,
        14, 0, 21, 12, 37, 0, 24, 10, 99, 104, 114, 111, 109, 111, 115, 111, 109, 101, 37, 0, 76,
        28, 0, 0, 0, 21, 2, 37, 0, 24, 8, 112, 111, 115, 105, 116, 105, 111, 110, 0, 53, 0, 24, 10,
        105, 100, 101, 110, 116, 105, 102, 105, 101, 114, 21, 2, 21, 6, 76, 60, 0, 0, 0, 53, 4, 24,
        4, 108, 105, 115, 116, 21, 2, 0, 21, 12, 37, 0, 24, 2, 105, 100, 37, 0, 76, 28, 0, 0, 0,
        21, 12, 37, 0, 24, 9, 114, 101, 102, 101, 114, 101, 110, 99, 101, 37, 0, 76, 28, 0, 0, 0,
        21, 12, 37, 0, 24, 9, 97, 108, 116, 101, 114, 110, 97, 116, 101, 37, 0, 76, 28, 0, 0, 0,
        21, 8, 37, 2, 24, 7, 113, 117, 97, 108, 105, 116, 121, 0, 53, 0, 24, 6, 102, 105, 108, 116,
        101, 114, 21, 2, 21, 6, 76, 60, 0, 0, 0, 53, 4, 24, 4, 108, 105, 115, 116, 21, 2, 0, 21,
        12, 37, 0, 24, 6, 102, 105, 108, 116, 101, 114, 37, 0, 76, 28, 0, 0, 0, 22, 2, 25, 44, 25,
        124, 38, 122, 28, 21, 12, 25, 37, 0, 6, 25, 24, 10, 99, 104, 114, 111, 109, 111, 115, 111,
        109, 101, 21, 4, 22, 2, 22, 74, 22, 114, 38, 8, 60, 54, 0, 40, 1, 49, 24, 1, 49, 0, 0, 22,
        222, 17, 21, 20, 22, 178, 14, 21, 34, 0, 38, 198, 2, 28, 21, 2, 25, 37, 0, 6, 25, 24, 8,
        112, 111, 115, 105, 116, 105, 111, 110, 21, 4, 22, 2, 22, 84, 22, 124, 38, 202, 1, 60, 54,
        0, 40, 4, 0, 33, 14, 0, 24, 4, 0, 33, 14, 0, 0, 0, 22, 242, 17, 21, 22, 22, 212, 14, 21,
        46, 0, 38, 190, 4, 28, 21, 12, 25, 37, 0, 6, 25, 56, 10, 105, 100, 101, 110, 116, 105, 102,
        105, 101, 114, 4, 108, 105, 115, 116, 2, 105, 100, 21, 4, 22, 2, 22, 118, 22, 158, 1, 38,
        160, 3, 60, 54, 0, 40, 7, 49, 48, 49, 57, 51, 57, 55, 24, 7, 49, 48, 49, 57, 51, 57, 55, 0,
        0, 22, 136, 18, 21, 24, 22, 130, 15, 21, 58, 0, 38, 172, 6, 28, 21, 12, 25, 37, 0, 6, 25,
        24, 9, 114, 101, 102, 101, 114, 101, 110, 99, 101, 21, 4, 22, 2, 22, 74, 22, 114, 38, 186,
        5, 60, 54, 0, 40, 1, 71, 24, 1, 71, 0, 0, 22, 160, 18, 21, 22, 22, 188, 15, 21, 34, 0, 38,
        238, 7, 28, 21, 12, 25, 37, 0, 6, 25, 24, 9, 97, 108, 116, 101, 114, 110, 97, 116, 101, 21,
        4, 22, 2, 22, 74, 22, 114, 38, 252, 6, 60, 54, 0, 40, 1, 65, 24, 1, 65, 0, 0, 22, 182, 18,
        21, 22, 22, 222, 15, 21, 34, 0, 38, 158, 9, 28, 21, 8, 25, 37, 0, 6, 25, 24, 7, 113, 117,
        97, 108, 105, 116, 121, 21, 4, 22, 2, 22, 56, 22, 96, 38, 190, 8, 60, 54, 2, 0, 0, 22, 204,
        18, 21, 22, 22, 128, 16, 21, 34, 0, 38, 194, 10, 28, 21, 12, 25, 37, 0, 6, 25, 56, 6, 102,
        105, 108, 116, 101, 114, 4, 108, 105, 115, 116, 6, 102, 105, 108, 116, 101, 114, 21, 4, 22,
        2, 22, 60, 22, 100, 38, 222, 9, 60, 54, 0, 0, 0, 22, 226, 18, 21, 22, 22, 162, 16, 21, 34,
        0, 22, 156, 4, 22, 2, 38, 8, 22, 180, 6, 20, 0, 0, 25, 124, 38, 0, 28, 21, 12, 25, 5, 25,
        24, 10, 99, 104, 114, 111, 109, 111, 115, 111, 109, 101, 21, 0, 22, 0, 22, 0, 22, 0, 38, 0,
        0, 22, 248, 18, 21, 6, 22, 196, 16, 21, 22, 0, 38, 0, 28, 21, 2, 25, 5, 25, 24, 8, 112,
        111, 115, 105, 116, 105, 111, 110, 21, 0, 22, 0, 22, 0, 22, 0, 38, 0, 0, 22, 254, 18, 21,
        6, 22, 218, 16, 21, 22, 0, 38, 0, 28, 21, 12, 25, 5, 25, 56, 10, 105, 100, 101, 110, 116,
        105, 102, 105, 101, 114, 4, 108, 105, 115, 116, 2, 105, 100, 21, 0, 22, 0, 22, 0, 22, 0,
        38, 0, 0, 22, 132, 19, 21, 6, 22, 240, 16, 21, 22, 0, 38, 0, 28, 21, 12, 25, 5, 25, 24, 9,
        114, 101, 102, 101, 114, 101, 110, 99, 101, 21, 0, 22, 0, 22, 0, 22, 0, 38, 0, 0, 22, 138,
        19, 21, 6, 22, 134, 17, 21, 22, 0, 38, 0, 28, 21, 12, 25, 5, 25, 24, 9, 97, 108, 116, 101,
        114, 110, 97, 116, 101, 21, 0, 22, 0, 22, 0, 22, 0, 38, 0, 0, 22, 144, 19, 21, 6, 22, 156,
        17, 21, 22, 0, 38, 0, 28, 21, 8, 25, 5, 25, 24, 7, 113, 117, 97, 108, 105, 116, 121, 21, 0,
        22, 0, 22, 0, 22, 0, 38, 0, 0, 22, 150, 19, 21, 6, 22, 178, 17, 21, 22, 0, 38, 0, 28, 21,
        12, 25, 5, 25, 56, 6, 102, 105, 108, 116, 101, 114, 4, 108, 105, 115, 116, 6, 102, 105,
        108, 116, 101, 114, 21, 0, 22, 0, 22, 0, 22, 0, 38, 0, 0, 22, 156, 19, 21, 6, 22, 200, 17,
        21, 22, 0, 22, 0, 22, 0, 38, 0, 22, 0, 20, 2, 0, 25, 28, 24, 12, 65, 82, 82, 79, 87, 58,
        115, 99, 104, 101, 109, 97, 24, 244, 6, 47, 47, 47, 47, 47, 52, 56, 67, 65, 65, 65, 69, 65,
        65, 65, 65, 56, 118, 47, 47, 47, 120, 81, 65, 65, 65, 65, 69, 65, 65, 69, 65, 65, 65, 65,
        75, 65, 65, 115, 65, 67, 65, 65, 75, 65, 65, 81, 65, 43, 80, 47, 47, 47, 119, 119, 65, 65,
        65, 65, 73, 65, 65, 103, 65, 65, 65, 65, 69, 65, 65, 99, 65, 65, 65, 65, 103, 65, 103, 65,
        65, 48, 65, 69, 65, 65, 70, 65, 66, 65, 65, 65, 77, 65, 81, 65, 65, 121, 65, 65, 65, 65,
        73, 81, 65, 65, 65, 65, 69, 65, 65, 65, 65, 55, 80, 47, 47, 47, 50, 119, 65, 65, 65, 66,
        103, 65, 65, 65, 65, 71, 65, 65, 65, 65, 65, 119, 65, 65, 65, 65, 81, 65, 66, 69, 65, 66,
        65, 65, 65, 65, 66, 65, 65, 67, 65, 65, 65, 65, 65, 119, 65, 65, 81, 65, 65, 65, 65, 81,
        65, 65, 65, 68, 115, 47, 47, 47, 47, 76, 65, 65, 65, 65, 67, 65, 65, 65, 65, 65, 89, 65,
        65, 65, 65, 66, 81, 65, 65, 65, 66, 65, 65, 69, 81, 65, 69, 65, 65, 65, 65, 69, 65, 65, 73,
        65, 65, 65, 65, 68, 65, 65, 65, 65, 65, 65, 65, 47, 80, 47, 47, 47, 119, 81, 65, 66, 65,
        65, 71, 65, 65, 65, 65, 90, 109, 108, 115, 100, 71, 86, 121, 65, 65, 68, 56, 47, 47, 47,
        47, 66, 65, 65, 69, 65, 65, 89, 65, 65, 65, 66, 109, 97, 87, 120, 48, 90, 88, 73, 65, 65,
        79, 122, 47, 47, 47, 56, 119, 65, 65, 65, 65, 73, 65, 65, 65, 65, 66, 103, 65, 65, 65, 65,
        66, 65, 119, 65, 65, 69, 65, 65, 83, 65, 65, 81, 65, 69, 65, 65, 82, 65, 65, 103, 65, 65,
        65, 65, 77, 65, 65, 65, 65, 65, 65, 68, 54, 47, 47, 47, 47, 65, 81, 65, 71, 65, 65, 89, 65,
        66, 65, 65, 72, 65, 65, 65, 65, 99, 88, 86, 104, 98, 71, 108, 48, 101, 81, 68, 115, 47, 47,
        47, 47, 76, 65, 65, 65, 65, 67, 65, 65, 65, 65, 65, 89, 65, 65, 65, 65, 66, 81, 65, 65, 65,
        66, 65, 65, 69, 81, 65, 69, 65, 65, 65, 65, 69, 65, 65, 73, 65, 65, 65, 65, 68, 65, 65, 65,
        65, 65, 65, 65, 47, 80, 47, 47, 47, 119, 81, 65, 66, 65, 65, 74, 65, 65, 65, 65, 89, 87,
        120, 48, 90, 88, 74, 117, 89, 88, 82, 108, 65, 65, 65, 65, 55, 80, 47, 47, 47, 121, 119,
        65, 65, 65, 65, 103, 65, 65, 65, 65, 71, 65, 65, 65, 65, 65, 85, 65, 65, 65, 65, 81, 65,
        66, 69, 65, 66, 65, 65, 65, 65, 66, 65, 65, 67, 65, 65, 65, 65, 65, 119, 65, 65, 65, 65,
        65, 65, 80, 122, 47, 47, 47, 56, 69, 65, 65, 81, 65, 67, 81, 65, 65, 65, 72, 74, 108, 90,
        109, 86, 121, 90, 87, 53, 106, 90, 81, 65, 65, 65, 79, 122, 47, 47, 47, 57, 111, 65, 65,
        65, 65, 88, 65, 65, 65, 65, 66, 103, 65, 65, 65, 65, 77, 65, 65, 65, 65, 69, 65, 65, 82,
        65, 65, 81, 65, 65, 65, 65, 81, 65, 65, 103, 65, 65, 65, 65, 77, 65, 65, 69, 65, 65, 65,
        65, 69, 65, 65, 65, 65, 55, 80, 47, 47, 47, 121, 119, 65, 65, 65, 65, 103, 65, 65, 65, 65,
        71, 65, 65, 65, 65, 65, 85, 65, 65, 65, 65, 81, 65, 66, 69, 65, 66, 65, 65, 65, 65, 66, 65,
        65, 67, 65, 65, 65, 65, 65, 119, 65, 65, 65, 65, 65, 65, 80, 122, 47, 47, 47, 56, 69, 65,
        65, 81, 65, 65, 103, 65, 65, 65, 71, 108, 107, 65, 65, 68, 56, 47, 47, 47, 47, 66, 65, 65,
        69, 65, 65, 111, 65, 65, 65, 66, 112, 90, 71, 86, 117, 100, 71, 108, 109, 97, 87, 86, 121,
        65, 65, 68, 115, 47, 47, 47, 47, 79, 65, 65, 65, 65, 67, 65, 65, 65, 65, 65, 89, 65, 65,
        65, 65, 65, 103, 65, 65, 65, 66, 65, 65, 69, 81, 65, 69, 65, 65, 65, 65, 69, 65, 65, 73,
        65, 65, 65, 65, 68, 65, 65, 65, 65, 65, 65, 65, 57, 80, 47, 47, 47, 121, 65, 65, 65, 65,
        65, 66, 65, 65, 65, 65, 67, 65, 65, 74, 65, 65, 81, 65, 67, 65, 65, 73, 65, 65, 65, 65, 99,
        71, 57, 122, 97, 88, 82, 112, 98, 50, 52, 65, 65, 65, 65, 65, 55, 80, 47, 47, 47, 121, 119,
        65, 65, 65, 65, 103, 65, 65, 65, 65, 71, 65, 65, 65, 65, 65, 85, 65, 65, 65, 65, 81, 65,
        66, 69, 65, 66, 65, 65, 65, 65, 66, 65, 65, 67, 65, 65, 65, 65, 65, 119, 65, 65, 65, 65,
        65, 65, 80, 122, 47, 47, 47, 56, 69, 65, 65, 81, 65, 67, 103, 65, 65, 65, 71, 78, 111, 99,
        109, 57, 116, 98, 51, 78, 118, 98, 87, 85, 65, 0, 24, 44, 65, 114, 114, 111, 119, 50, 32,
        45, 32, 78, 97, 116, 105, 118, 101, 32, 82, 117, 115, 116, 32, 105, 109, 112, 108, 101,
        109, 101, 110, 116, 97, 116, 105, 111, 110, 32, 111, 102, 32, 65, 114, 114, 111, 119, 0,
        107, 7, 0, 0, 80, 65, 82, 49,
    ];

    #[test]
    fn convert_positives() {
        let mut input = std::io::BufReader::new(&*VCF_FILE);
        let mut output = Vec::new();

        vcf2parquet(
            &mut input,
            &mut output,
            1,
            arrow2::io::parquet::write::CompressionOptions::Gzip(None),
            false,
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
            arrow2::io::parquet::write::CompressionOptions::Gzip(None),
            false,
        );

        assert!(result.is_err());
    }

    #[test]
    fn multi_positives() {
        let mut input = std::io::BufReader::new(&*VCF_FILE);
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
            arrow2::io::parquet::write::CompressionOptions::Gzip(None),
            false,
        )
        .unwrap();
    }
}
