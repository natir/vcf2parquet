//! vcf2parquet block

/* std use */

/* crate use */

/* project use */
use crate::*;
use internal::Internal;

pub struct Block {
    schema: parquet2::metadata::SchemaDescriptor,
    data: Vec<Vec<internal::Internal>>,
}

impl Block {
    pub fn new(schema: parquet2::metadata::SchemaDescriptor) -> Self {
        let mut data = Vec::with_capacity(schema.num_columns());

        data.push(Vec::with_capacity(10_000));
        data.push(Vec::with_capacity(10_000));
        data.push(Vec::with_capacity(10_000));
        data.push(Vec::with_capacity(10_000));
        data.push(Vec::with_capacity(10_000));
        data.push(Vec::with_capacity(10_000));
        data.push(Vec::with_capacity(10_000));

        Block { schema, data }
    }

    pub fn add_record(&mut self, record: &noodles::vcf::Record) {
        // chromosome
        self.data[0].push(Internal::String(record.chromosome().to_string()));

        // position
        self.data[1].push(Internal::Integer(record.position().try_into().unwrap()));

        // identifier
        if record.ids().len() == 0 {
            self.data[2].push(Internal::None);
        } else {
            self.data[2].push(Internal::Vec(
                record
                    .ids()
                    .iter()
                    .map(|x| Internal::String(x.to_string()))
                    .collect::<Vec<Internal>>(),
            ));
        }

        // reference
        self.data[3].push(Internal::String(record.reference_bases().to_string()));

        // alternative
        self.data[4].push(Internal::Vec(
            record
                .ids()
                .iter()
                .map(|x| Internal::String(x.to_string()))
                .collect::<Vec<Internal>>(),
        ));

        // quality
        if let Some(qual) = record.quality_score() {
            self.data[5].push(Internal::Integer(f32::from(qual) as i32))
        } else {
            self.data[5].push(Internal::None)
        }
    }
}

impl TryInto<arrow2::chunk::Chunk<std::sync::Arc<dyn arrow2::array::Array>>> for Block {
    type Error = error::Error;

    fn try_into(
        self,
    ) -> Result<arrow2::chunk::Chunk<std::sync::Arc<dyn arrow2::array::Array>>, Self::Error> {
        Err(error::Error::NoConversion)
    }
}
