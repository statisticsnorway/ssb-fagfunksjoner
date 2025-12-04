# test_qualind_logger.py

import json
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# IMPORTS: adjust this to your actual module path
# ---------------------------------------------------------------------------
# Example if your code lives in src/functions/qualind.py:
# from src.functions.qualind import QualIndLogger, AutoToleranceConfig, make_wide_df
#
# For this snippet I'll assume the module is named `qualind` next to tests:
from fagfunksjoner.quality_indicator.qualind_logger import (  # type: ignore[import]
    AutoToleranceConfig,
    QualIndLogger,
    make_wide_df,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_logger_with_history(tmp_path) -> QualIndLogger:
    """Create a QualIndLogger with historical logs for 'ind_a'.

    Used to test compare_periods, auto-tolerance, and related features.
    """
    # Create JSON logs for Jan-Apr 2024 (past periods)
    values = {
        "2024-01": 10.0,
        "2024-02": 11.0,
        "2024-03": 12.0,
        "2024-04": 13.0,
    }

    for period_str, val in values.items():
        path = tmp_path / f"process_data_p{period_str}.json"
        data = {
            "ind_a": {
                "title": "Indicator A",
                "description": "Test indicator A",
                "value": val,
                "unit": "count",
                "data_period": period_str,
                "timestamp": datetime.utcnow().isoformat(),
            }
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)

    # Instantiate logger for May 2024
    logger = QualIndLogger(log_dir=tmp_path, year=2024, month=5)

    # Log current period (2024-05) via the public API, so current file is valid
    logger.log_indicator(
        "ind_a",
        {
            "title": "Indicator A",
            "description": "Test indicator A",
            "value": 14.0,
            "unit": "count",
            "data_period": "2024-05",
        },
    )

    return logger


@pytest.fixture
def empty_logger(tmp_path) -> QualIndLogger:
    """Logger with no history, useful for tests that only need in-memory operations."""
    return QualIndLogger(log_dir=tmp_path, year=2024, month=5)


# ---------------------------------------------------------------------------
# Basic construction / period formatting
# ---------------------------------------------------------------------------


def test_init_monthly_creates_correct_period_str(tmp_path):
    ql = QualIndLogger(log_dir=tmp_path, year=2024, month=3)
    assert ql.frequency == "monthly"
    assert ql.period_str == "2024-03"
    assert ql.log_file.name == "process_data_p2024-03.json"


def test_init_quarterly_creates_correct_period_str(tmp_path):
    ql = QualIndLogger(log_dir=tmp_path, year=2024, quarter=2)
    assert ql.frequency == "quarterly"
    assert ql.period_str == "2024_Q2"


def test_init_yearly_creates_correct_period_str(tmp_path):
    ql = QualIndLogger(log_dir=tmp_path, year=2024)
    assert ql.frequency == "yearly"
    assert ql.period_str == "2024"


# ---------------------------------------------------------------------------
# log_indicator / update_indicator_value / load_period_log
# ---------------------------------------------------------------------------


def test_log_and_update_indicator(tmp_path):
    ql = QualIndLogger(log_dir=tmp_path, year=2024, month=6)

    ql.log_indicator(
        "rows_total",
        {
            "title": "Total rows",
            "description": "Number of rows after filtering",
            "value": 123,
            "unit": "rows",
            "data_period": ql.period_str,
        },
    )

    # After logging, indicator should be present and written to disk
    assert "rows_total" in ql.indicators
    assert ql.log_file.exists()

    # Update value
    ql.update_indicator_value("rows_total", "value", 456)
    loaded = ql.load_period_log(ql.period_str)
    assert loaded["rows_total"]["value"] == 456


# ---------------------------------------------------------------------------
# compare_periods
# ---------------------------------------------------------------------------


def test_compare_periods_previous(sample_logger_with_history):
    ql = sample_logger_with_history

    df = ql.compare_periods(
        indicator="ind_a",
        n_periods=5,
        ref_strategy="previous",
        style=False,
    )

    # We expect 5 periods: 2024-01 .. 2024-05
    assert list(df["period"]) == ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05"]

    # Latest rel_change should be (14 - 13) / 13
    latest = df.iloc[-1]
    expected_rel = (14.0 - 13.0) / 13.0
    assert np.isclose(latest["rel_change"], expected_rel, rtol=1e-8, atol=1e-8)


def test_compare_periods_mean_history_window(sample_logger_with_history):
    ql = sample_logger_with_history

    df = ql.compare_periods(
        indicator="ind_a",
        n_periods=5,
        ref_strategy="mean",
        history_window=3,
        style=False,
    )

    # history for mean is values.shift(1).tail(history_window)
    # values: [10, 11, 12, 13, 14]
    # history: [NaN,10,11,12,13] → tail(3) = [11,12,13] → mean = 12
    baseline = 12.0

    latest = df.iloc[-1]
    # ref is constant baseline → value - abs_change = baseline
    inferred_ref = latest["value"] - latest["abs_change"]
    assert np.isclose(inferred_ref, baseline)


def test_compare_periods_median_history_window(sample_logger_with_history):
    ql = sample_logger_with_history

    df = ql.compare_periods(
        indicator="ind_a",
        n_periods=5,
        ref_strategy="median",
        history_window=3,
        style=False,
    )

    # Same history as above → [11,12,13] → median = 12
    baseline = 12.0
    latest = df.iloc[-1]
    inferred_ref = latest["value"] - latest["abs_change"]
    assert np.isclose(inferred_ref, baseline)


def test_compare_periods_specific_period(sample_logger_with_history):
    ql = sample_logger_with_history

    df = ql.compare_periods(
        indicator="ind_a",
        n_periods=5,
        ref_strategy="specific",
        specific_period="2024-02",
        style=False,
    )

    # ref is the value in 2024-02 (11.0)
    latest = df.iloc[-1]
    inferred_ref = latest["value"] - latest["abs_change"]
    assert np.isclose(inferred_ref, 11.0)


# ---------------------------------------------------------------------------
# get_tolerance_for_indicator (explicit + automatic)
# ---------------------------------------------------------------------------


def test_get_tolerance_explicit_float(empty_logger):
    ql = empty_logger
    # Simulate an indicator with a single float tol
    ql.indicators["ind_float"] = {
        "title": "X",
        "description": "",
        "value": 1.0,
        "unit": "x",
        "data_period": ql.period_str,
        "timestamp": datetime.utcnow().isoformat(),
        "tol": 0.1,
    }

    tol = ql.get_tolerance_for_indicator("ind_float")
    assert tol == {"critical": 0.1}


def test_get_tolerance_explicit_dict(empty_logger):
    ql = empty_logger
    ql.indicators["ind_dict"] = {
        "title": "X",
        "description": "",
        "value": 1.0,
        "unit": "x",
        "data_period": ql.period_str,
        "timestamp": datetime.utcnow().isoformat(),
        "tol": {"warning": 0.05, "critical": 0.2},
    }

    tol = ql.get_tolerance_for_indicator("ind_dict")
    assert tol["warning"] == 0.05
    assert tol["critical"] == 0.2


def test_get_tolerance_auto_from_history(tmp_path):
    # Build some history for indicator 'auto'
    periods = ["2024-01", "2024-02", "2024-03", "2024-04"]
    values = [100.0, 110.0, 90.0, 120.0]

    for period_str, val in zip(periods, values, strict=False):
        path = tmp_path / f"process_data_p{period_str}.json"
        data = {
            "auto": {
                "title": "Auto",
                "description": "Auto tol",
                "value": val,
                "unit": "count",
                "data_period": period_str,
                "timestamp": datetime.utcnow().isoformat(),
            }
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)

    cfg = AutoToleranceConfig(
        ref_strategy_for_sigma="previous",
        n_hist=4,
        min_points=2,
        use_mad=False,
        k_warning=1.0,
        k_critical=2.0,
        fail_on_insufficient_history=True,
    )
    ql = QualIndLogger(log_dir=tmp_path, year=2024, month=4, auto_tol_config=cfg)

    tol = ql.get_tolerance_for_indicator("auto")
    # With non-constant history and std > 0, we expect positive thresholds
    assert "warning" in tol and "critical" in tol
    assert tol["warning"] > 0
    assert tol["critical"] > tol["warning"]


# ---------------------------------------------------------------------------
# filter_breaches
# ---------------------------------------------------------------------------


def test_filter_breaches_classifies_and_filters(empty_logger, monkeypatch):
    ql = empty_logger

    # Monkeypatch tolerance lookup to avoid touching file history
    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"warning": 0.1, "critical": 0.2},
    )

    df = pd.DataFrame({"rel_change": [0.05, 0.15, 0.25]})

    all_breaches = ql.filter_breaches(df, indicator="x", tier=None)
    assert list(all_breaches["breach_tier"]) == ["warning", "critical"]

    warnings_only = ql.filter_breaches(df, indicator="x", tier="warning")
    assert list(warnings_only["breach_tier"]) == ["warning"]

    critical_only = ql.filter_breaches(df, indicator="x", tier=["critical"])
    assert list(critical_only["breach_tier"]) == ["critical"]


# ---------------------------------------------------------------------------
# check_latest_pass
# ---------------------------------------------------------------------------


def test_check_latest_pass_empty_df_treated_as_pass(empty_logger, monkeypatch):
    ql = empty_logger

    # No tolerance for this indicator
    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {},
    )

    assert ql.check_latest_pass("whatever") is True


def test_check_latest_pass_with_threshold(sample_logger_with_history, monkeypatch):
    ql = sample_logger_with_history

    # Use the same strategy here and inside check_latest_pass
    ref_strategy = "previous"

    # Set a tolerance that is *just* above the actual latest rel_change
    df = ql.compare_periods("ind_a", n_periods=5, ref_strategy=ref_strategy)
    latest_rel = abs(df["rel_change"].iloc[-1])

    # First: threshold slightly above → should pass
    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"critical": float(latest_rel) + 1e-6},
    )

    assert ql.check_latest_pass(
        "ind_a",
        tier="critical",
        ref_strategy=ref_strategy,
    )

    # Then: threshold slightly below → should fail
    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"critical": float(latest_rel) - 1e-6},
    )

    assert not ql.check_latest_pass(
        "ind_a",
        tier="critical",
        ref_strategy=ref_strategy,
    )


# ---------------------------------------------------------------------------
# collect_long_df + make_wide_df
# ---------------------------------------------------------------------------


def test_collect_long_df_returns_expected_rows(sample_logger_with_history):
    ql = sample_logger_with_history
    long = ql.collect_long_df(
        indicators=["ind_a"],
        n_periods=5,
        ref_strategy="previous",
        style=False,
    )

    assert set(long.columns) == {
        "indicator",
        "period",
        "value",
        "abs_change",
        "rel_change",
        "unit",
    }
    # 5 periods for one indicator
    assert len(long) == 5
    assert long["indicator"].nunique() == 1


def test_make_wide_df_basic():
    long_df = pd.DataFrame(
        {
            "indicator": ["a", "a", "b", "b"],
            "period": ["2024-01", "2024-02", "2024-01", "2024-02"],
            "value": [1, 2, 10, 20],
            "abs_change": [np.nan, 1, np.nan, 10],
            "rel_change": [np.nan, 1.0, np.nan, 1.0],
            "unit": ["x", "x", "y", "y"],
        }
    )

    wide = make_wide_df(long_df, change_cols=["rel_change"])

    # We expect columns: period, a_value, a_rel_change, b_value, b_rel_change (order may vary)
    cols = set(wide.columns)
    assert "period" in cols
    assert "a_value" in cols
    assert "a_rel_change" in cols
    assert "b_value" in cols
    assert "b_rel_change" in cols

    # Two periods → 2 rows
    assert len(wide) == 2


# ---------------------------------------------------------------------------
# export_kvalinds_to_excel
# ---------------------------------------------------------------------------


def test_export_kvalinds_to_excel_creates_file(sample_logger_with_history, tmp_path):
    ql = sample_logger_with_history

    # Add a tolerance so metadata/breach columns are populated
    ql.indicators["ind_a"]["tol"] = {"warning": 0.05, "critical": 0.2}

    out_dir = tmp_path / "reports"
    ql.export_kvalinds_to_excel(
        out_path=out_dir,
        indicators=["ind_a"],
        change_cols=["rel_change"],
        n_periods=5,
        ref_strategy="previous",
        include_overview=True,
        include_metadata=True,
        per_indicator_sheets=True,
    )

    final_file = out_dir / f"kvalind_report_p{ql.period_str}.xlsx"
    assert final_file.exists()

    # Quick smoke check of sheets
    xls = pd.ExcelFile(final_file)
    assert "overview_long" in xls.sheet_names
    assert "overview_wide" in xls.sheet_names
    assert "metadata" in xls.sheet_names
    # Per-indicator sheet name is sanitized; here should be something like "ind_ind_a"
    assert any(name.startswith("ind_") for name in xls.sheet_names)
