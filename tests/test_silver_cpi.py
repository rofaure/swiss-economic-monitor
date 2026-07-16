"""
Unit tests for silver CPI transformation.
"""

import pytest
import pandas as pd
from transforms.silver.silver_cpi import transform_cpi

@pytest.fixture
def bronze_cpi():
    """Load bronze CPI data as test input."""
    return pd.read_parquet("data/bronze/bronze_cpi.parquet")

def test_accepted_not_empty(bronze_cpi):
    accepted, _ = transform_cpi(bronze_cpi)
    assert len(accepted) > 0, "Accepted DataFrame should not be empty"


def test_rejected_not_empty(bronze_cpi):
    _, rejected = transform_cpi(bronze_cpi)
    assert len(rejected) > 0, "Rejected DataFrame should not be empty"


def test_total_rows_preserved(bronze_cpi):
    """Accepted + rejected should equal bronze input (no silent data loss)."""
    accepted, rejected = transform_cpi(bronze_cpi)
    assert len(accepted) + len(rejected) == len(bronze_cpi)


def test_expected_columns(bronze_cpi):
    accepted, _ = transform_cpi(bronze_cpi)
    expected_cols = ["period", "cpi_value", "series_key"]
    for col in expected_cols:
        assert col in accepted.columns, f"Missing column: {col}"


def test_no_nulls_on_key_columns(bronze_cpi):
    accepted, _ = transform_cpi(bronze_cpi)
    for col in ["period", "cpi_value", "series_key"]:
        assert accepted[col].isna().sum() == 0, f"Null values found in {col}"


def test_cpi_value_is_float(bronze_cpi):
    accepted, _ = transform_cpi(bronze_cpi)
    assert accepted["cpi_value"].dtype == float


def test_period_format(bronze_cpi):
    accepted, _ = transform_cpi(bronze_cpi)
    assert accepted["period"].str.match(r"^\d{4}-\d{2}$").all(), \
        "Period format should be YYYY-MM"
