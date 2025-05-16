from __future__ import annotations
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import pandas.io.formats.style
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field, field_serializer, ConfigDict


# Pydantic model for quality indicators
class QualityIndicator(BaseModel):
    title: str
    description: str
    value: Optional[float | int | str] = None
    unit: str
    data_period: str = Field(..., pattern=r"^\d{4}(-(0[1-9]|1[0-2])|_Q[1-4])?$")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Optional[dict[str, Any]] = None
    tol: Optional[float] = None

    @field_serializer("timestamp")
    def serialize_timestamp(self, v: datetime) -> str:
        return v.isoformat()

    # Use ConfigDict instead of class Config for Pydantic v2
    model_config = ConfigDict(
        extra="allow"
    )

class QualIndLogger:
    def __init__(
        self,
        log_dir: str | Path,
        year: int | str,
        month: int | str | None = None,
        quarter: int | str | None = None,
    ):
        self.log_dir = Path(log_dir)
        if month is not None and quarter is not None:
            raise ValueError("Provide either month or quarter, not both.")

        year_int = int(year)
        month_int = int(month) if month is not None else None
        quarter_int = (
            int(str(quarter).lstrip("Q")) if quarter is not None else None
        )

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
        # Load existing indicators, handle invalid JSON gracefully
        self.indicators: dict[str, Any] = {}
        if self.log_file.exists():
            try:
                with open(self.log_file, "r", encoding="utf-8") as f:
                    self.indicators = json.load(f)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse JSON log file {self.log_file}, starting fresh.")
                self.indicators = {}

    def log_indicator(self, key: str, indicator_data: dict[str, Any]) -> None:
        # Use Pydantic model for validation and defaults
        qi = QualityIndicator(**indicator_data)
        # Dump to JSON string then reload to get fully serialized dict
        serialized = json.loads(qi.model_dump_json())
        self.indicators[key] = serialized
        self._save_logs()

    def update_indicator_value(self, key: str, field: str, value: Any) -> None:
        if key not in self.indicators:
            print(f"Warning: Indicator '{key}' not found.")
            return
        if field not in self.indicators[key]:
            print(f"Warning: Field '{field}' not found for indicator '{key}'.")
            return
        self.indicators[key][field] = value
        self._save_logs()

    def _save_logs(self) -> None:
        with open(self.log_file, "w", encoding="utf-8") as f:
            json.dump(self.indicators, f, indent=4, ensure_ascii=False)

    def get_logs(self, period_str: str) -> dict[str, Any]:
        path = self.log_dir / f"process_data_p{period_str}.json"
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse JSON for period {period_str}, returning empty.")
                return {}
        return {}

    def _get_prev_periods(self, n_periods: int = 5) -> list[datetime]:
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
        """
        Returns a DataFrame comparing an indicator across the last n_periods,
        with absolute and relative changes computed.
        """
        # Build long-format DataFrame
        records: list[dict[str, Any]] = []
        prev_value: Optional[float] = None
        for d in reversed(self._get_prev_periods(n_periods)):
            # format period same as logger
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
            if isinstance(val, (int, float)) and prev_value is not None:
                rec["abs_change"] = val - prev_value
                rec["rel_change"] = (val - prev_value) / prev_value if prev_value != 0 else None
            else:
                rec["abs_change"] = None
                rec["rel_change"] = None
            records.append(rec)
            if isinstance(val, (int, float)):
                prev_value = val
        df = pd.DataFrame(records)
        return df

    def compare_periods_print(
        self,
        indicator: str,
        n_periods: int = 5,
        tol: Optional[float] = None,
    ) -> None:
        """
        Prints a styled table of compare_periods with highlights where
        relative change exceeds tol. If tol is None, fetch from last entry.
        """
        df = self.compare_periods(indicator, n_periods)
        # fetch tolerance
        if tol is None:
            last_entry = self.indicators.get(indicator, {})
            tol = last_entry.get("tol", 0)
        # print header
        print(f"\nComparison for '{indicator}' over last {n_periods} periods")
        print(f"Description: {self.indicators.get(indicator, {}).get('description', '')}")
        # apply simple flag
        for idx, row in df.iterrows():
            flag = ""
            if row["rel_change"] is not None and abs(row["rel_change"]) > tol:
                flag = "**"  # highlight flag
            print(
                f"{row['period']}: {row['value']} {row['unit']} "
                f"(abs: {row['abs_change']}, rel: {row['rel_change']}) {flag}"
            )

    def compare_periods_styled(self, indicator: str, n_periods: int = 5):
        df = self.compare_periods(indicator, n_periods)
        tol = self.indicators.get(indicator, {}).get("tol", 0)
        display(highlight_rel_change(df.assign(indicator=indicator), {indicator: tol}))

        
def systemize_process_data(
    logger,
    indicators: Optional[List[str]] = None,
    n_periods: int = 5,
    highlight_change=False,
) -> pd.DataFrame:
    """
    Builds a tidy DataFrame of process indicators across periods,
    computing absolute and relative changes on the fly.

    :param logger: QualIndLogger instance (must implement _get_prev_periods and get_logs).
    :param indicators: Optional list of indicator keys to include (default: all).
    :param n_periods: Number of past periods to include (including current).
    :return: Long-format DataFrame with columns
             ['period','indicator','value','unit','abs_change','rel_change'].
    """
    # 1. get list of past period datetimes
    periods_dt = logger._get_prev_periods(n_periods)

    # 2. helper to format the period string exactly like the logger uses
    def fmt_date(d: datetime) -> str:
        if logger.frequency == "monthly":
            return f"{d.year}-{d.month:02d}"
        elif logger.frequency == "quarterly":
            q = ((d.month - 1) // 3) + 1
            return f"{d.year}_Q{q}"
        else:  # yearly
            return f"{d.year}"

    # 3. collect rows
    rows = []
    tolerances = {}
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
            if highlight_change:
                if "tol" in entry.keys():
                    tolerances[key] = entry["tol"]

    # 4. build DataFrame and compute diffs
    df = pd.DataFrame(rows)
    df = df.sort_values(["indicator", "period"]).reset_index(drop=True)
    df["abs_change"] = df.groupby("indicator")["value"].diff()
    df["rel_change"] = df.groupby("indicator")["value"].pct_change()
    if highlight_change:
        return highlight_rel_change(df, tolerances)
    return df


def highlight_rel_change(
    df: pd.DataFrame, tolerance: dict[str, float], default_tol: float = 0.0
) -> pd.io.formats.style.Styler:
    """
    Returns a Styler that highlights rows where the absolute rel_change
    exceeds the specified tolerance for each indicator.

    :param df: Long-format DataFrame from systemize_process_data.
    :param tolerance: Mapping indicator -> tolerance (e.g. {'indicator_key': 0.1}).
                      Use 'default' key for a fallback tolerance.
    :param default_tol: Fallback tolerance if 'default' not in tolerance.
    """
    tol_map = dict(default=default_tol, **tolerance)

    def _style_row(row):
        tol = tol_map.get(row["indicator"], tol_map["default"])
        if pd.isna(row["rel_change"]) or abs(row["rel_change"]) <= tol:
            return [""] * len(row)
        return ["background-color: orange"] * len(row)

    return df.style.apply(_style_row, axis=1)