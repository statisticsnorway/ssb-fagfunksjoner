from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from IPython.display import display
from pydantic import BaseModel, ConfigDict, Field, field_serializer

from fagfunksjoner import logger


# Type aliases for clarity
tolerance_t = dict[str, float]
reference_strategy_t = Literal[
    "previous",
    "mean",
    "rolling_mean",
    "specific",
    "median",
    "rolling_median",
]


@dataclass
class AutoToleranceConfig:
    """Configuration for automatic tolerance inference from historical data."""

    ref_strategy_for_sigma: str = (
        "median"  # Reference strategy used only for computing the historical rel_change distribution used in tolerance estimation. This is independent of the reference strategy used when displaying or exporting comparisons.
    )
    n_hist: int = 12  # periods of history to use
    min_points: int = 6  # minimum number of non-NaN rel_change
    k_warning: float = 1.0
    k_critical: float = 2.0
    use_mad: bool = True  # median absolute deviation (mad)
    fail_on_insufficient_history: bool = False


class QualityIndicator(BaseModel):
    """Data model for a quality indicator.

    Attributes:
        title: Human-readable title.
        description: Detailed explanation of the indicator.
        value: Numeric or string value for the period.
        unit: Unit of measurement (e.g., 'percent').
        data_period: Period string (YYYY-MM, YYYY_Qn, or YYYY for yearly).
        timestamp: Time when indicator was logged (UTC).
        metadata: Optional additional context.
        tol: Single float tolerance or dict of tiered tolerances.
    """

    title: str
    description: str
    value: float | int | str | None = None
    unit: str
    data_period: str = Field(..., pattern=r"^\d{4}(-(0[1-9]|1[0-2])|_Q[1-4])?$")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] | None = None
    tol: float | tolerance_t | None = None

    @field_serializer("timestamp")
    def serialize_timestamp(self, v: datetime) -> str:
        """Serialize timestamp to ISO format string."""
        return v.isoformat()

    model_config = ConfigDict(extra="allow")


class QualIndLogger:
    """Logger for quality indicators with advanced comparison and multi-tier tolerances."""

    def __init__(
        self,
        log_dir: str | Path,
        year: int | str,
        month: int | str | None = None,
        quarter: int | str | None = None,
        auto_tol_config: AutoToleranceConfig | None = None,  # <-- add this
    ) -> None:
        """Initialize the QualIndLogger.

        Args:
            log_dir: Directory path to store JSON logs.
            year: Year of the period (e.g., 2025).
            month: Month (1-12) for monthly logging, optional.
            quarter: Quarter (1-4 or 'Qn') for quarterly logging, optional.
            auto_tol_config: Optional configuration for deriving automatic tolerances.

        Raises:
            ValueError: If both month and quarter are provided or quarter is invalid.
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
        self.auto_tol_config = auto_tol_config or AutoToleranceConfig()

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

    def load_period_log(self, period_str: str) -> dict[str, Any]:
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

    def _collect_indicator_series(
        self,
        indicator: str,
        n_periods: int,
    ) -> pd.DataFrame:
        """Helper: fetch values and units for last n_periods.

        Note:
            `n_periods` is a maximum lookback. If there are fewer logged
            periods (i.e. no JSON file), fewer rows are returned.
        """
        periods = self._get_prev_periods(n_periods)
        recs: list[dict[str, Any]] = []

        for d in reversed(periods):
            p = self._format_period(d)
            logs = self.load_period_log(p)

            # If there is no log file / no process_data for this period, skip it
            if not logs:
                continue

            # If log exists but this specific indicator is missing,
            # we still include the period with value = NA (as before)
            entry = logs.get(indicator, {})

            recs.append(
                {
                    "period": p,
                    "value": entry.get("value", pd.NA),
                    "unit": entry.get("unit", ""),
                }
            )

        if not recs:
            # No data at all - return an empty frame with the expected columns
            return pd.DataFrame(columns=["period", "value", "unit"])

        return pd.DataFrame(recs)

    def compare_periods(
        self,
        indicator: str,
        n_periods: int = 5,
        ref_strategy: reference_strategy_t = "median",
        history_window: int | None = None,
        specific_period: str | None = None,
        print_df: bool = False,
        style: bool = False,
        print_style: bool = False,
        colors: dict[str, str] | None = None,
    ) -> pd.DataFrame | pd.io.formats.style.Styler:
        """Compare an indicator across periods using different reference strategies.

        Provides options to print or style the results. If styling, automatically
        uses the tolerance(s) stored with the indicator.

        Args:
            indicator: Key of the indicator to compare.
            n_periods: Number of periods to include (current + historical).
            ref_strategy: Strategy for reference value calculation:
                - 'previous': compare to immediately prior period.
                - 'rolling_mean': compare to the mean of the n previous periods.
                - 'mean': compare to mean of the n last periods (not including latest).
                - 'specific': compare to a user-defined `specific_period`.
            history_window: Window length for computing mean/median-based reference strategies. Dafaults to n_periods if None
            specific_period: Specific period string (e.g., '2024-05' or '2024_Q2') for
                'specific'.
            print_df: If True, print the raw DataFrame to console.
            style: If True, apply multi-tier tolerance styling using the indicator's
                logged tol.
            print_style: If True and `style=True`, display the styled table in the
                notebook.
            colors: Dict mapping tier names to CSS colors (default: orange, red).

        Returns:
            DataFrame of comparison results, or a styled Styler if `style=True`.

        Raises:
            ValueError: If `ref_strategy` is 'specific' and `specific_period` is not
                provided.
        """

        def _recent_mean_baseline(values: pd.Series, history_window: int) -> pd.Series:
            # Exclude current value when computing baseline
            recent_history = values.shift(1).dropna().tail(history_window)
            baseline = recent_history.mean() if len(recent_history) else pd.NA
            return pd.Series([baseline] * len(values), index=values.index)

        if history_window is None:
            history_window = n_periods
        df = self._collect_indicator_series(indicator, n_periods)

        # Coerce to numeric; invalid and missing values become NaN
        values = pd.to_numeric(df["value"], errors="coerce")
        history = values.shift(1)  # always exclude current
        if ref_strategy == "mean":
            # Single baseline: mean of last `history_window` periods
            baseline = history.tail(history_window).mean()
            ref = pd.Series([baseline] * len(values), index=df.index)

        elif ref_strategy == "median":
            # Single baseline: median of last `history_window` periods
            baseline = history.tail(history_window).median()
            ref = pd.Series([baseline] * len(values), index=df.index)

        elif ref_strategy == "rolling_mean":
            ref = history.rolling(history_window, min_periods=1).mean()

        elif ref_strategy == "rolling_median":
            ref = history.rolling(history_window, min_periods=1).median()
        elif ref_strategy == "previous":
            ref = values.shift(1)
        elif ref_strategy == "specific":
            if not specific_period:
                raise ValueError(
                    "`specific_period` must be provided for strategy 'specific'."
                )
            mapping = dict(zip(df["period"], values, strict=False))
            ref_val = mapping.get(specific_period)
            ref = pd.Series([ref_val] * len(values), index=df.index)
        else:
            raise ValueError(f"Unknown strategy {ref_strategy}")

        df["abs_change"] = values - ref
        df["rel_change"] = (values - ref) / ref.replace({0: pd.NA})

        if style:
            styled = self.style_tolerances(
                df.assign(indicator=indicator), indicator, colors=colors
            )
            if print_style:
                display(styled)
            return styled
        if print_df:
            print(df.to_string(index=False))
        return df

    def style_tolerances(
        self,
        df: pd.DataFrame,
        indicator: str | None,
        colors: dict[str, str] | None = None,
    ) -> pd.io.formats.style.Styler:
        """Returns a styled DataFrame highlighting where relative change exceeds tolerance tiers.

        If `indicator` is None, per-row indicator is taken from df['indicator'].
        """

        # helper to normalize a raw tol into a dict
        def _normalize_tol(raw_tol: Any) -> dict[str, float]:
            if isinstance(raw_tol, int | float):
                return {"critical": float(raw_tol)}
            return dict(raw_tol) if isinstance(raw_tol, dict) else {}

        # cache thresholds per indicator to avoid repeated lookups
        tol_cache: dict[str, dict[str, float]] = {}

        # default colors
        default_colors = {"warning": "orange", "critical": "red"}
        colors_merged = default_colors.copy()
        if colors:
            colors_merged.update(colors)

        def style_row(row: pd.Series) -> list[str]:
            rel = row.get("rel_change")
            if pd.isna(rel):
                return [""] * len(row)

            # determine which indicator key to use
            ind_key = indicator or row.get("indicator")
            if ind_key is None:
                return [""] * len(row)

            # get / compute tol for this indicator
            if ind_key not in tol_cache:
                raw_tol = self.get_tolerance_for_indicator(ind_key)
                tol_cache[ind_key] = _normalize_tol(raw_tol)

            tol = tol_cache[ind_key]
            if not tol:
                return [""] * len(row)

            # --- NEW: pick the most severe breached tier ---
            breached: list[tuple[str, float]] = [
                (tier, thresh) for tier, thresh in tol.items() if abs(rel) > thresh
            ]
            if breached:
                # choose the tier with the largest threshold (most severe)
                tier, _ = max(breached, key=lambda kv: kv[1])
                color = colors_merged.get(tier, "")
                return [f"background-color: {color}"] * len(row)
            return [""] * len(row)

        return df.style.apply(style_row, axis=1)

    def filter_breaches(
        self,
        df: pd.DataFrame,
        indicator: str,
        tier: str | list[str] | None = None,
    ) -> pd.DataFrame:
        """Return rows that breach tolerance thresholds.

        Args:
            df: DataFrame from compare_periods/collect_long_df.
            indicator: Indicator key.
            tier:
                - "warning": only warning breaches
                - "critical": only critical breaches
                - ["warning", "critical"]: both
                - None: return all breached tiers.

        Returns:
            DataFrame with added 'breach_tier' column, filtered to the requested tiers.

        Raises:
            ValueError: If the indicator has no defined tolerances, or if `tier`
                contains a tier name not present in the tolerance configuration.
        """
        tol = self.get_tolerance_for_indicator(indicator)
        if not tol:
            raise ValueError(f"No tolerance defined for indicator '{indicator}'.")

        warning = tol.get("warning")
        critical = tol.get("critical")

        if warning is None and critical is None:
            raise ValueError(
                f"No tiered tolerances found for '{indicator}'. "
                "Expected at least 'warning' or 'critical'."
            )

        # Normalize tier input
        if tier is None:
            wanted_tiers = {"warning", "critical"}
        elif isinstance(tier, str):
            wanted_tiers = {tier}
        else:
            wanted_tiers = set(tier)

        abs_rel = df["rel_change"].abs()

        # Determine actual breach tier row-by-row
        def classify(val: float | pd.NA) -> str | pd.NA:
            if pd.isna(val):
                return pd.NA
            if critical is not None and val > critical:
                return "critical"
            if warning is not None and val > warning:
                return "warning"
            return pd.NA

        df = df.copy()
        df["breach_tier"] = abs_rel.apply(classify)

        # Filter based on wanted tiers
        mask = df["breach_tier"].isin(wanted_tiers)
        return df[mask].reset_index(drop=True)

    def get_tolerance_for_indicator(self, indicator: str) -> dict[str, float]:
        """Return tolerance dict for an indicator.

        Priority:
        1. Explicit tol stored with the indicator (single float or dict).
        2. Derived from historical rel_change using AutoToleranceConfig.
        """
        # 1) explicit tol wins
        raw_tol = self.indicators.get(indicator, {}).get("tol")
        if raw_tol is not None:
            if isinstance(raw_tol, int | float):
                return {"critical": float(raw_tol)}
            return dict(raw_tol)

        # 2) derive from history using auto_tol_config
        cfg = self.auto_tol_config

        hist = self.compare_periods(
            indicator,
            n_periods=cfg.n_hist,
            ref_strategy=cfg.ref_strategy_for_sigma,
            style=False,
        )
        rel = hist["rel_change"].dropna().astype(float)
        if len(rel) < cfg.min_points:
            msg = (
                f"Insufficient history to compute automatic tolerance for '{indicator}'. "
                f"Found {len(rel)} non-missing rel_change values, "
                f"but require at least {cfg.min_points}."
            )
            if cfg.fail_on_insufficient_history:
                # Raise a clear, domain-specific error instead of a cryptic TypeError
                raise RuntimeError(msg)
            logger.warning(msg)
            return {}  # too little data → no auto tol

        rel_window = rel.tail(cfg.n_hist)

        if cfg.use_mad:
            med = rel_window.median()
            mad = (rel_window - med).abs().median()
            sigma_rel = 1.4826 * mad  # Approximate sigma from MAD
        else:
            sigma_rel = rel_window.std(ddof=1)

        if not np.isfinite(sigma_rel) or sigma_rel == 0:
            return {}

        return {
            "warning": cfg.k_warning * sigma_rel,
            "critical": cfg.k_critical * sigma_rel,
        }

    def check_latest_pass(
        self,
        indicator: str,
        tier: str = "critical",
        n_periods: int = 5,
        ref_strategy: reference_strategy_t = "median",
        history_window: int | None = None,
        specific_period: str | None = None,
    ) -> bool:
        """Return True if the latest period's rel_change is within the given tolerance tier.

        Args:
            indicator: Indicator key.
            tier: Tolerance tier to use for pass/fail (e.g. "critical" or "warning").
            n_periods: Number of periods to include in the comparison.
            ref_strategy: Reference strategy for computing rel_change.
            history_window: Optional window length used by mean/median-based strategies.
                If None, defaults to n_periods inside compare_periods.
            specific_period: Specific period string for 'specific' strategy.

        Returns:
            True if the latest period passes the given tier, or if no threshold is defined.
        """
        # Resolve tolerance for this indicator
        tol = self.get_tolerance_for_indicator(indicator)
        thresh = tol.get(tier)

        # No threshold for this tier → convention: treat as pass
        if thresh is None:
            return True

        # Build comparison df for this indicator
        df = self.compare_periods(
            indicator=indicator,
            n_periods=n_periods,
            ref_strategy=ref_strategy,
            history_window=history_window,  # or history_window if you rename
            specific_period=specific_period,
            style=False,
        )

        if df.empty or "rel_change" not in df.columns:
            # No data → by convention treat as pass (or you could log a warning)
            return True

        latest_rel = df["rel_change"].iloc[-1]
        return pd.isna(latest_rel) or abs(latest_rel) <= thresh

    def _format_period(self, d: datetime) -> str:
        if self.frequency == "monthly":
            return f"{d.year}-{d.month:02d}"
        if self.frequency == "quarterly":
            q = ((d.month - 1) // 3) + 1
            return f"{d.year}_Q{q}"
        return f"{d.year}"

    def collect_long_df(
        self,
        indicators: list[str] | None = None,
        n_periods: int = 5,
        ref_strategy: reference_strategy_t = "median",
        history_window: int | None = None,
        specific_period: str | None = None,
        style: bool = False,
        colors: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """Build a tidy DataFrame of all indicators across periods.

        Computes absolute and relative change using the chosen reference strategy.
        Optionally returns a styled Styler.
        """
        if history_window is None:
            history_window = n_periods
        rows: list[dict[str, Any]] = []
        for indicator in indicators or list(self.indicators.keys()):
            df = self.compare_periods(
                indicator,
                n_periods=n_periods,
                ref_strategy=ref_strategy,
                history_window=history_window,
                specific_period=specific_period,
                style=False,
            )
            df["indicator"] = indicator
            rows.append(df)
        long = pd.concat(rows, axis=0, ignore_index=True)
        long = long[
            ["indicator", "period", "value", "abs_change", "rel_change", "unit"]
        ]
        long = long.sort_values(["indicator", "period"]).reset_index(drop=True)

        if style:
            return self.style_tolerances(long.copy(), None, colors=colors)
        return long

    def export_kvalinds_to_excel(
        self,
        out_path: str | Path,
        indicators: list[str] | None = None,
        change_cols: list[str] | None = None,
        n_periods: int = 5,
        ref_strategy: reference_strategy_t = "median",
        history_window: int | None = None,
        specific_period: str | None = None,
        colors: dict[str, str] | None = None,
        engine: str = "openpyxl",
        include_overview: bool = True,
        include_metadata: bool = True,
        per_indicator_sheets: bool = True,
    ) -> None:
        """Export quality-indicator comparisons to an Excel workbook.

        The workbook can contain:
            - overview_long: all indicators in long format
            - overview_wide: wide pivot (one column per indicatorxmetric)
            - metadata: titles, descriptions, units, tolerances, latest values
            - one sheet per indicator (optional)
        """
        if change_cols is None:
            change_cols = ["rel_change"]

        if history_window is None:
            history_window = n_periods

        out_path = Path(out_path)
        out_path.mkdir(parents=True, exist_ok=True)

        # Decide which indicators to work with
        if indicators is None:
            indicators = list(self.indicators.keys())

        # --- 1) Build long-format data for all indicators ---
        long_df = self.collect_long_df(
            indicators=indicators,
            n_periods=n_periods,
            ref_strategy=ref_strategy,
            history_window=history_window,
            specific_period=specific_period,
            style=False,
        )

        # --- 2) Wide-format overview ---
        wide_df = make_wide_df(long_df, change_cols)

        # --- 3) Metadata table ---
        if include_metadata:
            meta_rows: list[dict[str, Any]] = []
            for ind_key in indicators:
                raw = self.indicators.get(ind_key, {})
                tol = self.get_tolerance_for_indicator(ind_key)

                # Use latest non-NA row from long_df for this indicator
                df_ind = long_df[long_df["indicator"] == ind_key].copy()
                df_ind = df_ind.dropna(subset=["value"])

                if not df_ind.empty:
                    latest_row = df_ind.iloc[-1]
                    latest_period = latest_row["period"]
                    latest_value = latest_row["value"]
                    latest_rel_change = latest_row.get("rel_change", pd.NA)
                else:
                    latest_period = None
                    latest_value = None
                    latest_rel_change = pd.NA

                # Compute breach tier for the latest period (highest tier breached)
                breach = None
                if tol and pd.notna(latest_rel_change):
                    warning = tol.get("warning")
                    critical = tol.get("critical")
                    abs_rel = abs(float(latest_rel_change))

                    if critical is not None and abs_rel > critical:
                        breach = "critical"
                    elif warning is not None and abs_rel > warning:
                        breach = "warning"

                meta_rows.append(
                    {
                        "key": ind_key,
                        "latest_period": latest_period,
                        "latest_value": latest_value,
                        "unit": raw.get("unit", ""),
                        "latest_rel_change": latest_rel_change,
                        "breach": breach,
                        "tol_warning": tol.get("warning"),
                        "tol_critical": tol.get("critical"),
                        "title": raw.get("title", ""),
                        "description": raw.get("description", ""),
                    }
                )

            metadata_df = pd.DataFrame(meta_rows)

            # Reorder columns as requested
            desired_cols = [
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
            ]
            # In case something is missing for some reason, intersect with existing
            metadata_df = metadata_df[
                [c for c in desired_cols if c in metadata_df.columns]
            ]

        # --- 4) Helper to add breach_tier/pass_critical columns per indicator ---
        def _add_breach_columns(df_ind: pd.DataFrame, ind_key: str) -> pd.DataFrame:
            tol = self.get_tolerance_for_indicator(ind_key)
            if not tol:
                df_ind["breach_tier"] = pd.NA
                df_ind["pass_critical"] = True
                return df_ind

            warning = tol.get("warning")
            critical = tol.get("critical")

            abs_rel = df_ind["rel_change"].abs()

            def classify(val: float | pd.NA) -> str | pd.NA:
                if pd.isna(val):
                    return pd.NA
                if critical is not None and val > critical:
                    return "critical"
                if warning is not None and val > warning:
                    return "warning"
                return pd.NA

            df_ind["breach_tier"] = abs_rel.apply(classify)

            if critical is None:
                df_ind["pass_critical"] = True
            else:
                df_ind["pass_critical"] = abs_rel.le(critical) | abs_rel.isna()

            return df_ind

        # --- 5) Write everything to Excel ---
        final_file = out_path / f"kvalind_report_p{self.period_str}.xlsx"

        def _sanitize_sheet_name(name: str) -> str:
            # Excel sheet names: max 31 chars, no []:*?/\
            clean = re.sub(r"[\[\]:*?/\\]", "_", name)
            return clean[:31] or "sheet"

        with pd.ExcelWriter(final_file, engine=engine) as writer:
            if include_overview:
                long_df.to_excel(writer, sheet_name="overview_long", index=False)
                wide_df.to_excel(writer, sheet_name="overview_wide", index=False)

            if include_metadata:
                metadata_df.to_excel(writer, sheet_name="metadata", index=False)

            if per_indicator_sheets:
                for ind_key in indicators:
                    df_ind = (
                        long_df[long_df["indicator"] == ind_key]
                        .copy()
                        .sort_values("period")
                        .reset_index(drop=True)
                    )
                    df_ind = _add_breach_columns(df_ind, ind_key)

                    # Drop the redundant indicator column in the per-indicator sheet
                    if "indicator" in df_ind.columns:
                        df_ind = df_ind.drop(columns=["indicator"])

                    sheet_name = _sanitize_sheet_name(f"ind_{ind_key}")
                    df_ind.to_excel(writer, sheet_name=sheet_name, index=False)

        logger.info(f"Exported quality-indicator report to {final_file}")


def make_wide_df(
    long_df: pd.DataFrame,
    change_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Convert long-format indicator DataFrame to wide format.

    Parameters
    ----------
    long_df : pd.DataFrame
        Output of collect_long_df(), containing:
        ['indicator', 'period', 'value', 'abs_change', 'rel_change', 'unit']

    change_cols : list[str]
        Which change-related columns to include in the wide table.
        Default includes only 'rel_change'.

    Returns:
    -------
    pd.DataFrame
        Wide-format DataFrame with one row per period and one column per
        indicator x metric combination.
    """
    if change_cols is None:
        change_cols = ["rel_change"]
    # Keep only needed columns
    keep_cols = ["indicator", "period", "value", *change_cols]
    df = long_df[keep_cols].copy()

    # Build a composite column name for pivot
    df = df.melt(
        id_vars=["indicator", "period"],
        var_name="metric",
        value_name="val",
    )

    df["col"] = df["indicator"] + "_" + df["metric"]

    wide = (
        df.pivot(index="period", columns="col", values="val").sort_index().reset_index()
    )

    # Optional: sort columns nicely (period first)
    cols = ["period", *sorted(c for c in wide.columns if c != "period")]
    wide = wide[cols]

    return wide
