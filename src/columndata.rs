//! Struct to link name and data

/* std use */

/* crate use */
use arrow::array::ArrayBuilder;

/* project use */

/// Stores arrow array builders for each column datatype
#[derive(Debug)]
pub enum ColumnData {
    /// Boolean column
    Bool(arrow::array::BooleanBuilder),
    /// Int32 column
    Int(arrow::array::Int32Builder),
    /// Float32 column
    Float(arrow::array::Float32Builder),
    /// String column
    String(arrow::array::StringBuilder),

    /// List of int32 column
    ListInt(arrow::array::ListBuilder<arrow::array::Int32Builder>),
    /// List of float32 column
    ListFloat(arrow::array::ListBuilder<arrow::array::Float32Builder>),
    /// List of string column
    ListString(arrow::array::ListBuilder<arrow::array::StringBuilder>),
}

impl ColumnData {
    /// Creates a new ColumnData based on arrow type, length and field name
    pub fn new(
        arrow_type: &arrow::datatypes::DataType,
        length: usize,
        field_name: &str,
        nullable: bool,
    ) -> Self {
        match arrow_type {
            arrow::datatypes::DataType::Boolean => {
                ColumnData::Bool(arrow::array::BooleanBuilder::with_capacity(length))
            }
            arrow::datatypes::DataType::Int32 => {
                ColumnData::Int(arrow::array::Int32Builder::with_capacity(length))
            }
            arrow::datatypes::DataType::Float32 => {
                ColumnData::Float(arrow::array::Float32Builder::with_capacity(length))
            }
            arrow::datatypes::DataType::Utf8 => ColumnData::String(
                arrow::array::StringBuilder::with_capacity(length, length * 10),
            ),
            arrow::datatypes::DataType::List(field) => match field.data_type() {
                arrow::datatypes::DataType::Int32 => ColumnData::ListInt(
                    arrow::array::ListBuilder::with_capacity(
                        arrow::array::Int32Builder::new(),
                        length,
                    )
                    .with_field(arrow::datatypes::Field::new(
                        field_name,
                        arrow::datatypes::DataType::Int32,
                        nullable,
                    )),
                ),
                arrow::datatypes::DataType::Float32 => ColumnData::ListFloat(
                    arrow::array::ListBuilder::with_capacity(
                        arrow::array::Float32Builder::new(),
                        length,
                    )
                    .with_field(arrow::datatypes::Field::new(
                        field_name,
                        arrow::datatypes::DataType::Float32,
                        nullable,
                    )),
                ),
                arrow::datatypes::DataType::Utf8 => ColumnData::ListString(
                    arrow::array::ListBuilder::with_capacity(
                        arrow::array::StringBuilder::new(),
                        length,
                    )
                    .with_field(arrow::datatypes::Field::new(
                        field_name,
                        arrow::datatypes::DataType::Utf8,
                        nullable,
                    )),
                ),
                _ => todo!(),
            },
            dt => unreachable!("Unsupported arrow type, please check Schema: {:?}", dt),
        }
    }

    /// Add a Null value in array
    pub fn push_null(&mut self) {
        match self {
            ColumnData::Bool(a) => a.append_null(),
            ColumnData::Int(a) => a.append_null(),
            ColumnData::Float(a) => a.append_null(),
            ColumnData::String(a) => a.append_null(),

            ColumnData::ListInt(a) => a.append_null(),
            ColumnData::ListFloat(a) => a.append_null(),
            ColumnData::ListString(a) => a.append_null(),
        }
    }

    /// Get the length of internal array
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Bool(a) => a.len(),
            ColumnData::Int(a) => a.len(),
            ColumnData::Float(a) => a.len(),
            ColumnData::String(a) => a.len(),

            ColumnData::ListInt(a) => a.len(),
            ColumnData::ListFloat(a) => a.len(),
            ColumnData::ListString(a) => a.len(),
        }
    }

    /// Check if array is empty (not used for now)
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Add a boolean value in array, if it's not a boolean array failled
    pub fn push_bool(&mut self, value: bool) {
        match self {
            ColumnData::Bool(a) => a.append_value(value),
            _ => todo!(),
        }
    }

    /// Add a i32 value in array, if it's not a integer array failled
    pub fn push_i32(&mut self, value: Option<i32>) {
        match self {
            ColumnData::Int(a) => a.append_option(value),
            _ => todo!(),
        }
    }

    /// Add a f32 value in array, if it's not a float array failled
    pub fn push_f32(&mut self, value: Option<f32>) {
        match self {
            ColumnData::Float(a) => a.append_option(value),
            _ => todo!(),
        }
    }

    /// Add a string value in array, if it's not a string array failled
    pub fn push_string(&mut self, value: String) {
        match self {
            ColumnData::String(a) => a.append_option(Some(value)),
            _ => todo!(),
        }
    }

    /// Add a vector of integer value in array, if it's not a vector of integer array failled
    pub fn push_veci32(&mut self, value: Vec<Option<i32>>) -> arrow::error::Result<()> {
        match self {
            ColumnData::ListInt(a) => {
                a.values().append_values(
                    &value
                        .iter()
                        .map(|v| v.unwrap_or_default())
                        .collect::<Vec<i32>>(),
                    &value.iter().map(|v| v.is_some()).collect::<Vec<bool>>(),
                );
                a.append(true);
                Ok(())
            }
            _ => todo!(),
        }
    }

    /// Add a vector of float value in array, if it's not a vector of float array failled
    pub fn push_vecf32(&mut self, value: Vec<Option<f32>>) -> arrow::error::Result<()> {
        match self {
            ColumnData::ListFloat(a) => {
                a.values().append_values(
                    &value
                        .iter()
                        .map(|v| v.unwrap_or_default())
                        .collect::<Vec<f32>>(),
                    &value.iter().map(|v| v.is_some()).collect::<Vec<bool>>(),
                );
                a.append(true);
                Ok(())
            }
            _ => todo!(),
        }
    }

    /// Add a vector of string value in array, if it's not a vector of string array failled
    pub fn push_vecstring(&mut self, value: Vec<Option<String>>) -> arrow::error::Result<()> {
        match self {
            ColumnData::ListString(a) => {
                for v in value {
                    a.values().append_option(v);
                }
                a.append(true);
                Ok(())
            }
            _ => todo!(),
        }
    }

    /// Convert ColumnData in Arrow2 array
    pub fn into_arc(self) -> std::sync::Arc<dyn arrow::array::Array> {
        let length = self.len();

        match self {
            ColumnData::Bool(mut a) => arrow::array::Array::slice(&a.finish(), 0, length),
            ColumnData::Int(mut a) => arrow::array::Array::slice(&a.finish(), 0, length),
            ColumnData::Float(mut a) => arrow::array::Array::slice(&a.finish(), 0, length),
            ColumnData::String(mut a) => arrow::array::Array::slice(&a.finish(), 0, length),
            ColumnData::ListInt(mut a) => arrow::array::Array::slice(&a.finish(), 0, length),
            ColumnData::ListFloat(mut a) => arrow::array::Array::slice(&a.finish(), 0, length),
            ColumnData::ListString(mut a) => arrow::array::Array::slice(&a.finish(), 0, length),
        }
    }
}
