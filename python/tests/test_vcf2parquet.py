import polars as pl
from pyvcf2parquet import *
import os


def test_vcf2parquet():
    convert_vcf("tests/test.vcf", "tests/test.parquet")
    df = pl.read_parquet("tests/test.parquet")

    assert df.shape == (3, 10)

    os.remove("tests/test.parquet")
