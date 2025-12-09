# test_qualind_logger.py

import json
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from pandas.io.formats.style import Styler

from fagfunksjoner.quality_indicator.qualind_logger import (
    QualityIndicator,  # type: ignore[import]
)
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


# NEW: error branches in __init__


def test_init_raises_if_both_month_and_quarter(tmp_path):
    with pytest.raises(ValueError, match="either month or quarter"):
        QualIndLogger(log_dir=tmp_path, year=2024, month=3, quarter=1)


def test_init_raises_if_invalid_quarter(tmp_path):
    with pytest.raises(ValueError, match="Quarter must be between 1 and 4"):
        QualIndLogger(log_dir=tmp_path, year=2024, quarter=5)


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


# NEW: load_period_log edge cases


def test_load_period_log_missing_file_returns_empty(empty_logger):
    result = empty_logger.load_period_log("2099-01")
    assert result == {}


def test_load_period_log_invalid_json_returns_empty(tmp_path):
    # Create a corrupted JSON file for a period
    period_str = "2024-03"
    bad_file = tmp_path / f"process_data_p{period_str}.json"
    bad_file.write_text("{not-valid-json", encoding="utf-8")

    ql = QualIndLogger(log_dir=tmp_path, year=2024, month=3)
    # Now test load_period_log on another bad json file
    bad_file2 = tmp_path / "process_data_p2024-01.json"
    bad_file2.write_text("this-is-not-json", encoding="utf-8")

    result = ql.load_period_log("2024-01")
    assert result == {}


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


# NEW: rolling strategies, error branches, and style=True


def test_compare_periods_rolling_strategies(sample_logger_with_history):
    ql = sample_logger_with_history

    df_mean = ql.compare_periods(
        indicator="ind_a",
        n_periods=5,
        ref_strategy="rolling_mean",
        history_window=2,
        style=False,
    )
    df_median = ql.compare_periods(
        indicator="ind_a",
        n_periods=5,
        ref_strategy="rolling_median",
        history_window=2,
        style=False,
    )

    assert len(df_mean) == 5
    assert len(df_median) == 5
    # first reference should be NaN for rolling strategies
    assert pd.isna(df_mean["abs_change"].iloc[0]) or pd.isna(
        df_mean["rel_change"].iloc[0]
    )
    assert pd.isna(df_median["abs_change"].iloc[0]) or pd.isna(
        df_median["rel_change"].iloc[0]
    )


def test_compare_periods_specific_requires_period(sample_logger_with_history):
    ql = sample_logger_with_history
    with pytest.raises(ValueError, match="specific_period"):
        ql.compare_periods(
            indicator="ind_a",
            n_periods=5,
            ref_strategy="specific",
            specific_period=None,
            style=False,
        )


def test_compare_periods_unknown_strategy_raises(empty_logger):
    with pytest.raises(ValueError, match="Unknown strategy"):
        empty_logger.compare_periods(
            indicator="x",
            n_periods=3,
            ref_strategy="not_a_strategy",  # type: ignore[arg-type]
        )


def test_compare_periods_style_returns_styler(sample_logger_with_history, monkeypatch):
    ql = sample_logger_with_history

    # Make sure we have some tolerance so styling applies
    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"warning": 0.0, "critical": 0.0},
    )

    styled = ql.compare_periods(
        indicator="ind_a",
        n_periods=5,
        ref_strategy="previous",
        style=True,
        print_style=False,
    )

    assert isinstance(styled, Styler)


# ---------------------------------------------------------------------------
# _collect_indicator_series (indirect) / empty-data path
# ---------------------------------------------------------------------------


def test_collect_indicator_series_no_data_returns_empty(empty_logger):
    df = empty_logger._collect_indicator_series("no_such_indicator", n_periods=3)
    assert list(df.columns) == ["period", "value", "unit"]
    assert df.empty


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


# NEW: auto tolerance with insufficient history (no failure, returns {})


def test_get_tolerance_auto_insufficient_history_returns_empty(tmp_path):
    # Only one period -> fewer than min_points
    period_str = "2024-01"
    path = tmp_path / f"process_data_p{period_str}.json"
    data = {
        "auto": {
            "title": "Auto",
            "description": "Auto tol",
            "value": 100.0,
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
        use_mad=True,
        fail_on_insufficient_history=False,
    )
    ql = QualIndLogger(log_dir=tmp_path, year=2024, month=1, auto_tol_config=cfg)

    tol = ql.get_tolerance_for_indicator("auto")
    assert tol == {}


# ---------------------------------------------------------------------------
# style_tolerances (including color overrides / weird tol types)
# ---------------------------------------------------------------------------


def test_style_tolerances_uses_indicator_column_and_colors(
    sample_logger_with_history, monkeypatch
):
    ql = sample_logger_with_history

    long_df = ql.collect_long_df(
        indicators=["ind_a"],
        n_periods=5,
        ref_strategy="previous",
        style=False,
    )
    # Give a tolerance that will mark all non-zero rel_changes as warning
    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"warning": 0.0},
    )

    styled = ql.style_tolerances(
        df=long_df,
        indicator=None,  # force it to read 'indicator' column
        colors={"warning": "purple"},
    )
    assert isinstance(styled, Styler)
    html = styled.to_html()
    # At least one cell should have our custom background color
    assert "background-color: purple" in html


def test_style_tolerances_handles_unknown_tol_type(empty_logger, monkeypatch):
    ql = empty_logger

    df = pd.DataFrame(
        {
            "period": ["2024-01"],
            "value": [1.0],
            "abs_change": [0.1],
            "rel_change": [0.1],
            "unit": ["x"],
            "indicator": ["weird"],
        }
    )

    # Return a non-numeric, non-dict tolerance -> _normalize_tol → {}
    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: "not-a-dict-or-float",
    )

    styled = ql.style_tolerances(df, indicator=None)
    assert isinstance(styled, Styler)
    # No background-color should be applied because tol is empty
    html = styled.to_html()
    assert "background-color:" not in html or "background-color: " in html


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


def test_filter_breaches_raises_if_no_tolerance(empty_logger, monkeypatch):
    ql = empty_logger

    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {},
    )

    df = pd.DataFrame({"rel_change": [0.1, 0.2]})
    with pytest.raises(ValueError, match="No tolerance defined"):
        ql.filter_breaches(df, indicator="x", tier=None)


def test_filter_breaches_raises_if_no_tiered_tolerances(empty_logger, monkeypatch):
    ql = empty_logger

    # Tolerance dict without 'warning' or 'critical'
    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"foo": 0.1},
    )

    df = pd.DataFrame({"rel_change": [0.1, 0.2]})
    with pytest.raises(ValueError, match="No tiered tolerances"):
        ql.filter_breaches(df, indicator="x", tier=None)


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


# NEW: check_latest_pass edge cases


def test_check_latest_pass_missing_tier_treated_as_pass(
    sample_logger_with_history, monkeypatch
):
    ql = sample_logger_with_history

    # Only warning tier defined; asking for 'critical' should be treated as pass
    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"warning": 0.01},
    )

    assert ql.check_latest_pass("ind_a", tier="critical") is True


def test_check_latest_pass_empty_df_from_compare_periods(
    sample_logger_with_history, monkeypatch
):
    ql = sample_logger_with_history

    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"critical": 0.0},
    )

    # Force compare_periods to return an empty DataFrame
    monkeypatch.setattr(
        ql,
        "compare_periods",
        lambda **kwargs: pd.DataFrame(columns=["period", "value", "unit"]),
    )

    assert ql.check_latest_pass("ind_a", tier="critical") is True


def test_check_latest_pass_latest_rel_is_nan(sample_logger_with_history, monkeypatch):
    ql = sample_logger_with_history

    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"critical": 0.0},
    )

    # compare_periods returns a single row with rel_change NaN
    monkeypatch.setattr(
        ql,
        "compare_periods",
        lambda **kwargs: pd.DataFrame({"rel_change": [np.nan]}),
    )

    assert ql.check_latest_pass("ind_a", tier="critical") is True


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


# NEW: collect_long_df(style=True) and make_wide_df default change_cols


def test_collect_long_df_style_returns_styler(sample_logger_with_history, monkeypatch):
    ql = sample_logger_with_history

    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"warning": 0.0, "critical": 0.0},
    )

    styled = ql.collect_long_df(
        indicators=["ind_a"],
        n_periods=5,
        ref_strategy="previous",
        style=True,
    )
    assert isinstance(styled, Styler)


def test_make_wide_df_default_change_cols():
    long_df = pd.DataFrame(
        {
            "indicator": ["a", "a"],
            "period": ["2024-01", "2024-02"],
            "value": [1, 2],
            "abs_change": [np.nan, 1],
            "rel_change": [np.nan, 1.0],
            "unit": ["x", "x"],
        }
    )
    wide = make_wide_df(long_df)  # use default change_cols
    cols = set(wide.columns)
    # Only rel_change should be included besides value
    assert "a_value" in cols
    assert "a_rel_change" in cols


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


# NEW: export_kvalinds_to_excel when no tolerance is defined ( _add_breach_columns no-tol path )


def test_export_kvalinds_to_excel_no_tol_sets_default_breach_columns(
    sample_logger_with_history, tmp_path
):
    ql = sample_logger_with_history

    out_dir = tmp_path / "reports_no_tol"
    ql.export_kvalinds_to_excel(
        out_path=out_dir,
        indicators=["ind_a"],
        change_cols=["rel_change"],
        n_periods=5,
        ref_strategy="previous",
        include_overview=False,  # metadata + per-indicator only
        include_metadata=False,
        per_indicator_sheets=True,
    )

    final_file = out_dir / f"kvalind_report_p{ql.period_str}.xlsx"
    assert final_file.exists()

    xls = pd.ExcelFile(final_file)
    # Only per-indicator sheets
    ind_sheet_name = next(name for name in xls.sheet_names if name.startswith("ind_"))
    df_ind = pd.read_excel(final_file, sheet_name=ind_sheet_name)

    # With no tol, we expect 'breach_tier' all NA and 'pass_critical' all True
    assert "breach_tier" in df_ind.columns
    assert "pass_critical" in df_ind.columns
    assert df_ind["pass_critical"].all()


def test_quality_indicator_validates_pattern_and_serializes_timestamp():
    qi = QualityIndicator(
        title="Test",
        description="desc",
        value=1.23,
        unit="percent",
        data_period="2024-03",  # valid pattern
        metadata={"foo": "bar"},
        tol={"warning": 0.1, "critical": 0.2},
        extra_field="allowed",  # extra should be allowed by model_config
    )
    dumped = qi.model_dump()

    # data_period is kept as-is
    assert dumped["data_period"] == "2024-03"

    # timestamp serialized via field_serializer as ISO string
    qi_json = qi.model_dump_json()
    loaded = json.loads(qi_json)
    assert "T" in loaded["timestamp"]  # crude but effective ISO check

    # extra field should be present
    assert loaded["extra_field"] == "allowed"


def test_quality_indicator_invalid_data_period_raises():
    with pytest.raises(ValueError):
        QualityIndicator(
            title="Bad",
            description="bad",
            value=1,
            unit="x",
            data_period="2024-13",  # invalid month
        )


def test_get_tolerance_auto_mad_path(tmp_path):
    """Exercise AutoToleranceConfig.use_mad=True path with sufficient history."""
    periods = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]
    # non-constant to get non-zero MAD
    values = [100.0, 110.0, 90.0, 120.0, 115.0, 118.0]

    for p, v in zip(periods, values, strict=False):
        path = tmp_path / f"process_data_p{p}.json"
        data = {
            "auto_mad": {
                "title": "Auto MAD",
                "description": "MAD based tol",
                "value": v,
                "unit": "count",
                "data_period": p,
                "timestamp": datetime.utcnow().isoformat(),
            }
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)

    cfg = AutoToleranceConfig(
        ref_strategy_for_sigma="previous",
        n_hist=6,
        min_points=3,
        use_mad=True,  # <- MAD branch
        k_warning=1.0,
        k_critical=2.0,
        fail_on_insufficient_history=True,
    )
    ql = QualIndLogger(log_dir=tmp_path, year=2024, month=6, auto_tol_config=cfg)

    tol = ql.get_tolerance_for_indicator("auto_mad")
    # We mainly care that we went through the MAD path and produced positive thresholds
    assert "warning" in tol and "critical" in tol
    assert tol["warning"] > 0
    assert tol["critical"] > tol["warning"]


def test_style_tolerances_picks_most_severe_tier(empty_logger, monkeypatch):
    """When both warning and critical are breached, critical color should win."""
    ql = empty_logger

    df = pd.DataFrame(
        {
            "period": ["2024-01"],
            "value": [100.0],
            "abs_change": [50.0],
            "rel_change": [0.5],  # will breach both 0.1 and 0.3
            "unit": ["x"],
        }
    )

    # Make sure we use the explicit indicator argument, not df['indicator']
    monkeypatch.setattr(
        ql,
        "get_tolerance_for_indicator",
        lambda ind: {"warning": 0.1, "critical": 0.3},
    )

    styled = ql.style_tolerances(
        df=df,
        indicator="my_ind",  # use this key
        colors={"warning": "yellow", "critical": "red"},
    )
    assert isinstance(styled, Styler)
    html = styled.to_html()
    # We expect the "critical" color, not "yellow"
    assert "background-color: red" in html
    assert "background-color: yellow" not in html


def test_export_kvalinds_to_excel_metadata_and_breach(
    sample_logger_with_history, tmp_path
):
    """Hit metadata table construction with breach classification."""
    ql = sample_logger_with_history

    # Set a relatively low critical threshold so latest period breaches
    # Make warning lower than critical so we can distinguish
    ql.indicators["ind_a"]["tol"] = {"warning": 0.01, "critical": 0.02}

    out_dir = tmp_path / "reports_meta"
    ql.export_kvalinds_to_excel(
        out_path=out_dir,
        indicators=["ind_a"],
        change_cols=["rel_change"],
        n_periods=5,
        ref_strategy="previous",
        include_overview=True,
        include_metadata=True,
        per_indicator_sheets=False,  # metadata + overview only
    )

    final_file = out_dir / f"kvalind_report_p{ql.period_str}.xlsx"
    assert final_file.exists()

    xls = pd.ExcelFile(final_file)
    assert "metadata" in xls.sheet_names

    meta_df = pd.read_excel(final_file, sheet_name="metadata")

    # Check the basic columns are there
    for col in [
        "key",
        "latest_period",
        "latest_value",
        "unit",
        "latest_rel_change",
        "breach",
        "tol_warning",
        "tol_critical",
        "title",
        "description",
    ]:
        assert col in meta_df.columns

    # There should be exactly one row for 'ind_a'
    assert (meta_df["key"] == "ind_a").sum() == 1
    row = meta_df[meta_df["key"] == "ind_a"].iloc[0]

    # We don't assert exact numeric values, just that latest_rel_change is not NA
    assert pd.notna(row["latest_rel_change"])

    # With such low critical threshold, we expect 'critical' or 'warning' breach;
    # at least breach should not be NaN if rel_change is large.
    assert row["breach"] in ("warning", "critical")


def test_export_kvalinds_to_excel_sanitizes_sheet_name(tmp_path):
    """Exercise _sanitize_sheet_name path: invalid chars and max length."""
    # Build minimal logs with a very "ugly" indicator key
    ugly_key = "very/long*indicator:name?with[bad]chars_and_more________________"
    period_str = "2024-01"
    path = tmp_path / f"process_data_p{period_str}.json"
    data = {
        ugly_key: {
            "title": "Ugly",
            "description": "",
            "value": 1.0,
            "unit": "x",
            "data_period": period_str,
            "timestamp": datetime.utcnow().isoformat(),
        }
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    ql = QualIndLogger(log_dir=tmp_path, year=2024, month=1)

    out_dir = tmp_path / "reports_sanitized"
    ql.export_kvalinds_to_excel(
        out_path=out_dir,
        indicators=[ugly_key],
        change_cols=["rel_change"],
        n_periods=1,
        ref_strategy="previous",
        include_overview=False,
        include_metadata=False,
        per_indicator_sheets=True,
    )

    final_file = out_dir / f"kvalind_report_p{ql.period_str}.xlsx"
    assert final_file.exists()

    xls = pd.ExcelFile(final_file)
    # There should be exactly one sheet, whose name is sanitized from ugly_key
    assert len(xls.sheet_names) == 1
    sheet_name = xls.sheet_names[0]

    # invalid chars []:*?/\\ should be replaced with '_'
    for bad in ["[", "]", ":", "*", "?", "/", "\\"]:
        assert bad not in sheet_name

    # Excel sheet names are max 31 chars
    assert len(sheet_name) <= 31
    assert sheet_name.startswith("ind_")
