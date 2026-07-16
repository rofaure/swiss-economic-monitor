"""
Unit tests for silver policy rate transformation.
"""

import pytest
import pandas as pd
from transforms.silver.silver_policy_rate import transform_policy_rate


@pytest.fixture
def bronze_policy_rate():
    """Load bronze policy rate data as test input."""
    return pd.read_parquet("data/bronze/bronze_policy_rate.parquet")


def test_accepted_not_empty(bronze_policy_rate):
    accepted, _ = transform_policy_rate(bronze_policy_rate)
    assert len(accepted) > 0, "Accepted DataFrame should not be empty"


def test_rejected_not_empty(bronze_policy_rate):
    _, rejected = transform_policy_rate(bronze_policy_rate)
    assert len(rejected) > 0, "Rejected DataFrame should not be empty"


def test_expected_columns(bronze_policy_rate):
    accepted, _ = transform_policy_rate(bronze_policy_rate)
    expected_cols = ["period", "swiss_policy_rate", "_rate_source",
                     "_run_id", "_ingested_at", "_source", "_source_url"]
    for col in expected_cols:
        assert col in accepted.columns, f"Missing column: {col}"


def test_no_nulls_on_key_columns(bronze_policy_rate):
    accepted, _ = transform_policy_rate(bronze_policy_rate)
    for col in ["period", "swiss_policy_rate", "_rate_source"]:
        assert accepted[col].isna().sum() == 0, f"Null values found in {col}"


def test_policy_rate_is_float(bronze_policy_rate):
    accepted, _ = transform_policy_rate(bronze_policy_rate)
    assert accepted["swiss_policy_rate"].dtype == float


def test_period_format(bronze_policy_rate):
    accepted, _ = transform_policy_rate(bronze_policy_rate)
    assert accepted["period"].str.match(r"^\d{4}-\d{2}$").all(), \
        "Period format should be YYYY-MM"


def test_rate_source_values(bronze_policy_rate):
    """Rate source should only contain known values."""
    accepted, _ = transform_policy_rate(bronze_policy_rate)
    valid_sources = {"LIBOR_midpoint", "SNB_policy_rate"}
    assert set(accepted["_rate_source"].unique()).issubset(valid_sources), \
        "Unexpected _rate_source value found"


def test_no_gap_at_2019_transition(bronze_policy_rate):
    """Series should be continuous at the 2019-06 transition."""
    accepted, _ = transform_policy_rate(bronze_policy_rate)
    assert "2019-05" in accepted["period"].values
    assert "2019-06" in accepted["period"].values