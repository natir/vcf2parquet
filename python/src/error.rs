use std::fmt::{Debug, Formatter};

use std::io::ErrorKind;

use pyo3::prelude::*;
use pyo3::{
    create_exception,
    exceptions::{
        PyException, PyFileExistsError, PyFileNotFoundError, PyIOError, PyPermissionError,
        PyRuntimeError,
    },
};
use thiserror::Error;
use vcf2parquet_lib::error::Error as Vcf2ParquetError;

#[derive(Error)]
pub enum PyVcf2ParquetErr {
    #[error(transparent)]
    Vcf2Parquet(#[from] Vcf2ParquetError),

    #[error(transparent)]
    Niffle(#[from] niffler::Error),

    #[error("{0}")]
    Other(String),
}

impl std::convert::From<std::io::Error> for PyVcf2ParquetErr {
    fn from(value: std::io::Error) -> Self {
        PyVcf2ParquetErr::Other(format!("{value:?}"))
    }
}

impl std::convert::From<PyVcf2ParquetErr> for PyErr {
    fn from(err: PyVcf2ParquetErr) -> PyErr {
        use PyVcf2ParquetErr::*;
        match err {
            Vcf2Parquet(ref err) => match err {
                Vcf2ParquetError::NoConversion => {
                    NoConversionException::new_err(format!("{:?}", err))
                }
                Vcf2ParquetError::Arrow(err) => ArrowException::new_err(format!("{:?}", err)),
                Vcf2ParquetError::Parquet(err) => ParquetException::new_err(format!("{:?}", err)),
                Vcf2ParquetError::Io(err) => match err.kind() {
                    ErrorKind::NotFound => PyFileNotFoundError::new_err(format!("{:?}", err)),
                    ErrorKind::PermissionDenied => PyPermissionError::new_err(format!("{:?}", err)),
                    ErrorKind::AlreadyExists => PyFileExistsError::new_err(format!("{:?}", err)),
                    _ => PyIOError::new_err(format!("{:?}", err)),
                },
                Vcf2ParquetError::NoodlesHeader(err) => {
                    NoodlesHeaderException::new_err(format!("{:?}", err))
                }
                _ => PyRuntimeError::new_err(format!("{:?}", err)),
            },
            Niffle(err) => match err {
                niffler::Error::IOError(err) => match err.kind() {
                    ErrorKind::NotFound => PyFileNotFoundError::new_err(format!("{:?}", err)),
                    ErrorKind::PermissionDenied => PyPermissionError::new_err(format!("{:?}", err)),
                    ErrorKind::AlreadyExists => PyFileExistsError::new_err(format!("{:?}", err)),
                    _ => PyIOError::new_err(format!("{:?}", err)),
                },
                niffler::Error::FeatureDisabled => {
                    NoConversionException::new_err(format!("{:?}", err))
                }
                niffler::Error::FileTooShort => PyIOError::new_err(format!("{:?}", err)),
            },
            Other(err_str) => PyRuntimeError::new_err(err_str),
        }
    }
}

impl Debug for PyVcf2ParquetErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use PyVcf2ParquetErr::*;
        match self {
            Vcf2Parquet(err) => write!(f, "{err:?}"),
            Niffle(err) => write!(f, "{err:?}"),
            Other(err) => write!(f, "BindingsError: {err:?}"),
        }
    }
}

create_exception!(vcf2parquet_lib.error, PyVcf2ParquetException, PyException);

create_exception!(
    vcf2parquet_lib.error,
    NoConversionException,
    PyVcf2ParquetException
);

create_exception!(
    vcf2parquet_lib.error,
    ArrowException,
    PyVcf2ParquetException
);

create_exception!(
    vcf2parquet_lib.error,
    ParquetException,
    PyVcf2ParquetException
);

create_exception!(
    vcf2parquet_lib.error,
    NoodlesHeaderException,
    PyVcf2ParquetException
);
