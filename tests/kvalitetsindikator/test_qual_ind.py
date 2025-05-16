import os
import json
import tempfile
import pytest
from datetime import datetime

import pandas as pd
from pydantic import ValidationError

from fagfunksjoner.kvalitetsindikator.qual_ind import QualityIndicator, QualIndLogger, highlight_rel_change

def test_required_fields():
    # Missing required field 'title'
    data = {
        "description": "desc",
        "value": 1,
        "unit": "units",
        "data_period": "2025-05",
    }
    with pytest.raises(ValidationError):
        QualityIndicator(**data)


def test_optional_fields_and_defaults():
    # Base data
    data_base = {
        "title": "t",
        "description": "d",
        "unit": "u",
        "data_period": "2025-05",
        "value": 10,
    }
    qi = QualityIndicator(**data_base)
    assert isinstance(qi.timestamp, datetime)
    assert qi.metadata is None
    assert qi.tol is None

    # Explicit metadata and tol
    data_full = {
        **data_base,
        "metadata": {"x": 1},
        "tol": 0.2,
    }
    qi2 = QualityIndicator(**data_full)
    assert qi2.metadata == {"x": 1}
    assert qi2.tol == 0.2

    # Additional keys allowed
    extra_data = {**data_base, "extra": 123}
    qi3 = QualityIndicator(**extra_data)
    assert qi3.extra == 123


def test_data_period_pattern_valid_and_invalid():
    # Valid month
    good = {
        "title": "t",
        "description": "d",
        "unit": "u",
        "data_period": "2025-12",
        "value": 1,
    }
    qi = QualityIndicator(**good)
    assert qi.data_period == "2025-12"

    # Invalid month should raise
    bad = {**good, "data_period": "2025-13"}
    with pytest.raises(ValidationError):
        QualityIndicator(**bad)


def test_log_and_get(tmp_path):
    log_dir = tmp_path / "logs"
    logger = QualIndLogger(log_dir, year=2025, month=5)
    key = "ind1"
    data = {
        "title": "t",
        "description": "d",
        "unit": "u",
        "data_period": "2025-05",
        "value": 5,
    }
    logger.log_indicator(key, data)
    # File created
    assert logger.log_file.exists()
    # Retrieved logs
    logs = logger.get_logs("2025-05")
    assert key in logs
    assert logs[key]["value"] == 5
    # Timestamp serialized to ISO string
    raw = json.loads(logger.log_file.read_text(encoding="utf-8"))
    ts = raw[key]["timestamp"]
    assert "T" in ts


def test_update_indicator_value(tmp_path, capsys):
    log_dir = tmp_path / "logs"
    logger = QualIndLogger(log_dir, year=2025, month=5)
    key = "ind2"
    data = {
        "title": "t2",
        "description": "d2",
        "unit": "u2",
        "data_period": "2025-05",
        "value": 10,
    }
    logger.log_indicator(key, data)
    logger.update_indicator_value(key, "value", 20)
    logs = logger.get_logs("2025-05")
    assert logs[key]["value"] == 20
    # No exception for missing field; should print a warning
    logger.update_indicator_value(key, "nonexistent", 1)
    captured = capsys.readouterr()
    assert "Warning" in captured.out


def test_compare_periods(tmp_path):
    log_dir = tmp_path / "logs"
    # Create logs for months 3,4,5
    for m, v in [(5, 100), (4, 110), (3, 90)]:
        logger = QualIndLogger(log_dir, year=2025, month=m)
        logger.log_indicator(
            "ind",
            {
                "title": "t",
                "description": "d",
                "unit": "u",
                "data_period": f"2025-{m:02d}",
                "value": v,
            },
        )
    # Use a fresh logger instance for compare
    logger = QualIndLogger(log_dir, year=2025, month=5)
    df = logger.compare_periods("ind", n_periods=3)
    assert df["period"].tolist() == ["2025-03", "2025-04", "2025-05"]
    assert pd.isna(df.loc[0, "rel_change"] )
    assert pytest.approx((110 - 90) / 90) == df.loc[1, "rel_change"]


def test_highlight_rel_change_styles():
    df = pd.DataFrame({
        "indicator": ["a", "a"],
        "rel_change": [0.2, 0.05],
    })
    styler = highlight_rel_change(df, {"a": 0.1})
    rendered = styler.to_html()
    assert "background-color: orange" in rendered