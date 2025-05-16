from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pandas.io.formats.style
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, ConfigDict, Field, field_serializer


# Pydantic model for quality indicators
class QualityIndicator(BaseModel):
    """Data model for a quality indicator, validated and serialized via Pydantic."""

    title: str
    description: str
    value: float | int | str | None = None
    unit: str
    data_period: str = Field(..., pattern=r"^\d{4}(-(0[1-9]|1[0-2])|_Q[1-4])?$")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] | None = None
    tol: float | None = None

    @field_serializer("timestamp")
    def serialize_timestamp(self, v: datetime) -> str:
        """Serialize timestamp to ISO format string."""
        return v.isoformat()

    # Pydantic v2 configuration
    model_config = ConfigDict(extra="allow")


class QualIndLogger:
    """Logger for quality indicators over defined time periods (monthly, quarterly, yearly)."""

    def __init__(
        self,
        log_dir: str | Path,
        year: int | str,
        month: int | str | None = None,
        quarter: int | str | None = None,
    ) -> None:
        """Initialize with directory and period.

        Args:
            log_dir: Path to store JSON logs.
            year: Year of the period.
            month: Month (1-12) if monthly logging.
            quarter: Quarter (1-4 or 'Q1'-'Q4') if quarterly logging.

        Raises:
            ValueError: If both month and quarter are provided.
            ValueError: If quarter is outside the range 1-4.
        """
        self.log_dir = Path(log_dir)
        if month is not None and quarter is not None:
            raise ValueError("Provide either month or quarter, not both.")

        year_int = int(year)
        month_int = int(month) if month is not None else None
        quarter_int = int(str(quarter).lstrip("Q")) if quarter is not None else None

        if month_int is not None:
            self.frequency = "monthly"
            date = datetime(year_int, month_int, 1)
            self.period_str = f"{date.year}-{date.month:02d}"
        elif quarter_int is not None:
            self.frequency = "quarterly"
            if quarter_int < 1 or quarter_int > 4:
                raise ValueError("Quarter must be between 1 and 4.")
            first_month = (quarter_int - 1) * 3 + 1
            date = datetime(year_int, first_month, 1)
            self.period_str = f"{date.year}_Q{quarter_int}"
        else:
            self.frequency = "yearly"
            date = datetime(year_int, 1, 1)
            self.period_str = f"{date.year}"

        self.current_date = date
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = self.log_dir / f"process_data_p{self.period_str}.json"
        self.indicators: dict[str, Any] = {}
        if self.log_file.exists():
            try:
                with open(self.log_file, encoding="utf-8") as f:
                    self.indicators = json.load(f)
            except json.JSONDecodeError:
                print(
                    f"Warning: Could not parse JSON log file {self.log_file}, starting fresh."
                )
                self.indicators = {}

    def log_indicator(self, key: str, indicator_data: dict[str, Any]) -> None:
        """Validate and append or overwrite a quality indicator in the current period.

        Args:
            key: Unique identifier for the indicator.
            indicator_data: Raw data matching QualityIndicator schema.
        """
        qi = QualityIndicator(**indicator_data)
        serialized = json.loads(qi.model_dump_json())
        self.indicators[key] = serialized
        self._save_logs()

    def update_indicator_value(self, key: str, field: str, value: Any) -> None:
        """Update a specific field of an existing indicator and save.

        Args:
            key: Indicator identifier.
            field: Field name to update.
            value: New value.
        """
        if key not in self.indicators:
            print(f"Warning: Indicator '{key}' not found.")
            return
        if field not in self.indicators[key]:
            print(f"Warning: Field '{field}' not found for indicator '{key}'.")
            return
        self.indicators[key][field] = value
        self._save_logs()

    def _save_logs(self) -> None:
        """Write the in-memory indicators dict to the JSON log file."""
        with open(self.log_file, "w", encoding="utf-8") as f:
            json.dump(self.indicators, f, indent=4, ensure_ascii=False)

    def get_logs(self, period_str: str) -> dict[str, Any]:
        """Load logs for a given period string (e.g. '2025-05').

        Args:
            period_str: Period identifier.

        Returns:
            Dict of indicators or empty dict if none.
        """
        path = self.log_dir / f"process_data_p{period_str}.json"
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print(
                    f"Warning: Could not parse JSON for period {period_str}, returning empty."
                )
                return {}
        return {}

    def _get_prev_periods(self, n_periods: int = 5) -> list[datetime]:
        """Compute the list of previous period start dates.

        Args:
            n_periods: Number of periods back to include (current + past).

        Returns:
            List of datetime objects for each period.
        """
        periods: list[datetime] = []
        for i in range(n_periods):
            if self.frequency == "monthly":
                d = self.current_date - relativedelta(months=i)
            elif self.frequency == "quarterly":
                d = self.current_date - relativedelta(months=3 * i)
            else:
                d = self.current_date - relativedelta(years=i)
            periods.append(d)
        return periods

    def compare_periods(
        self,
        indicator: str,
        n_periods: int = 5,
    ) -> pd.DataFrame:
        """Returns a DataFrame comparing an indicator across the last n_periods, computing absolute and relative changes.

        Args:
            indicator: Key of the indicator to compare.
            n_periods: Number of periods to include.

        Returns:
            Long-format DataFrame with period, value, unit, abs_change, rel_change.
        """
        records: list[dict[str, Any]] = []
        prev_value: float | None = None
        for d in reversed(self._get_prev_periods(n_periods)):
            if self.frequency == "monthly":
                p = f"{d.year}-{d.month:02d}"
            elif self.frequency == "quarterly":
                q = ((d.month - 1) // 3) + 1
                p = f"{d.year}_Q{q}"
            else:
                p = f"{d.year}"
            entry = self.get_logs(p).get(indicator, {})
            val = entry.get("value")
            unit = entry.get("unit", "")
            rec: dict[str, Any] = {"period": p, "value": val, "unit": unit}
            if isinstance(val, (int | float)) and prev_value is not None:
                rec["abs_change"] = val - prev_value
                rec["rel_change"] = (
                    (val - prev_value) / prev_value if prev_value != 0 else None
                )
            else:
                rec["abs_change"] = None
                rec["rel_change"] = None
            records.append(rec)
            if isinstance(val, (int | float)):
                prev_value = val
        return pd.DataFrame(records)

    def compare_periods_print(
        self,
        indicator: str,
        n_periods: int = 5,
        tol: float | None = None,
    ) -> None:
        """Print comparison of an indicator with optional tolerance flags.

        Args:
            indicator: Indicator key.
            n_periods: Number of periods.
            tol: Optional relative change tolerance threshold.
        """
        df = self.compare_periods(indicator, n_periods)
        if tol is None:
            tol = self.indicators.get(indicator, {}).get("tol", 0)
        print(f"\nComparison for '{indicator}' over last {n_periods} periods")
        print(
            f"Description: {self.indicators.get(indicator, {}).get('description', '')}"
        )
        for _idx, row in df.iterrows():
            flag = ""
            if row["rel_change"] is not None and abs(row["rel_change"]) > tol:
                flag = "**"
            print(
                f"{row['period']}: {row['value']} {row['unit']} "
                f"(abs: {row['abs_change']}, rel: {row['rel_change']}) {flag}"
            )

    def compare_periods_styled(
        self, indicator: str, n_periods: int = 5
    ) -> pd.io.formats.style.Styler:
        """Return a styled DataFrame highlighting tolerance breaches.

        Args:
            indicator: Indicator key.
            n_periods: Number of periods.

        Returns:
            pandas Styler with highlights.
        """
        df = self.compare_periods(indicator, n_periods)
        tol = self.indicators.get(indicator, {}).get("tol", 0)
        return highlight_rel_change(df.assign(indicator=indicator), {indicator: tol})


def systemize_process_data(
    logger: QualIndLogger,
    indicators: list[str] | None = None,
    n_periods: int = 5,
    highlight_change: bool = False,
) -> pd.DataFrame:
    """Builds a tidy DataFrame of process indicators across periods, computing absolute and relative changes."""
    periods_dt = logger._get_prev_periods(n_periods)

    def fmt_date(d: datetime) -> str:
        if logger.frequency == "monthly":
            return f"{d.year}-{d.month:02d}"
        elif logger.frequency == "quarterly":
            q = ((d.month - 1) // 3) + 1
            return f"{d.year}_Q{q}"
        else:
            return f"{d.year}"

    rows: list[dict[str, Any]] = []
    tolerances: dict[str, float] = {}
    for d in reversed(periods_dt):
        period_str = fmt_date(d)
        logs = logger.get_logs(period_str)
        for key, entry in logs.items():
            if indicators and key not in indicators:
                continue
            rows.append(
                {
                    "period": period_str,
                    "indicator": key,
                    "value": entry.get("value"),
                    "unit": entry.get("unit"),
                }
            )
            if highlight_change and "tol" in entry:
                tolerances[key] = entry["tol"]

    df = pd.DataFrame(rows)
    df = df.sort_values(["indicator", "period"]).reset_index(drop=True)
    df["abs_change"] = df.groupby("indicator")["value"].diff()
    df["rel_change"] = df.groupby("indicator")["value"].pct_change()
    if highlight_change:
        return highlight_rel_change(df, tolerances)
    return df


def highlight_rel_change(
    df: pd.DataFrame,
    tolerance: dict[str, float],
    default_tol: float = 0.0,
) -> pd.io.formats.style.Styler:
    """Returns a Styler highlighting rows where the relative change exceeds tolerance."""
    tol_map = dict(default=default_tol, **tolerance)

    def _style_row(row: pd.Series) -> list[str]:
        """Style a single row based on rel_change vs tolerance."""
        tol = tol_map.get(row["indicator"], tol_map["default"])
        if pd.isna(row["rel_change"]) or abs(row["rel_change"]) <= tol:
            return [""] * len(row)
        return ["background-color: orange"] * len(row)

    return df.style.apply(_style_row, axis=1)
