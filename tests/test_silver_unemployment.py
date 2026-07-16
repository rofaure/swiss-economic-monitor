"""
Unit tests for silver unemployment transformation.
"""

import pytest
import pandas as pd
from transforms.silver.silver_unemployment import transform_unemployment


@pytest.fixture
def bronze_unemployment():
    """Load bronze unemployment data as test input."""
    return pd.read_parquet("data/bronze/bronze_unemployment.parquet")

def test_accepted_not_empty(bronze_unemployment):
    accepted, _ = transform_unemployment(bronze_unemployment)
    assert len(accepted) > 0, "Accepted DataFrame should not be empty"


def test_rejected_not_empty(bronze_unemployment):
    _, rejected = transform_unemployment(bronze_unemployment)
    assert len(rejected) > 0, "Rejected DataFrame should not be empty"


def test_total_rows_preserved(bronze_unemployment):
    """Accepted + rejected should equal bronze input (no silent data loss)."""
    accepted, rejected = transform_unemployment(bronze_unemployment)
    assert len(accepted) + len(rejected) == len(bronze_unemployment)


def test_expected_columns(bronze_unemployment):
    accepted, _ = transform_unemployment(bronze_unemployment)
    expected_cols = ["period", "region", "unemployment_rate", 
                     "_run_id", "_ingested_at", "_source", "_source_url"]
    for col in expected_cols:
        assert col in accepted.columns, f"Missing column: {col}"


def test_no_nulls_on_key_columns(bronze_unemployment):
    accepted, _ = transform_unemployment(bronze_unemployment)
    for col in ["period", "region", "unemployment_rate"]:
        assert accepted[col].isna().sum() == 0, f"Null values found in {col}"


def test_unemployment_rate_is_float(bronze_unemployment):
    accepted, _ = transform_unemployment(bronze_unemployment)
    assert accepted["unemployment_rate"].dtype == float


def test_period_format(bronze_unemployment):
    accepted, _ = transform_unemployment(bronze_unemployment)
    assert accepted["period"].str.match(r"^\d{4}-\d{2}$").all(), \
        "Period format should be YYYY-MM"


def test_no_m_in_period(bronze_unemployment):
    accepted, _ = transform_unemployment(bronze_unemployment)
    assert not accepted["period"].str.contains("M").any(), \
        "Period should not contain 'M' (raw format leaked into Silver)"



