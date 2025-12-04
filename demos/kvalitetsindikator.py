from pathlib import Path

import numpy as np
import pandas as pd

from fagfunksjoner.quality_indicator import AutoToleranceConfig, QualIndLogger


# -------------------------------------------------------------------
# 1. Setup: where logs live, and synthetic monthly periods
# -------------------------------------------------------------------

LOG_DIR = Path("demo_kvalind_logs")
LOG_DIR.mkdir(exist_ok=True)

# We'll simulate 12 months of data for 2024
periods = pd.period_range("2022-01", "2024-12", freq="M")

# Synthetic indicator 1: share of missing IDs, around 3% with some noise
rng = np.random.default_rng(42)
base1 = 0.03 + 0.002 * rng.standard_normal(len(periods))
# add one spike (e.g. data issue) in September
base1[8] += 0.02  # 2024-09 spike

# Synthetic indicator 2: average number of employments per person, mild trend
base2 = 1.5 + 0.01 * np.arange(len(periods)) + 0.02 * rng.standard_normal(len(periods))

# -------------------------------------------------------------------
# 2. Log indicators for each period
#    Important: we create a new QualIndLogger for each period when logging
# -------------------------------------------------------------------

for p, v1, v2 in zip(periods, base1, base2, strict=False):
    year = p.year
    month = p.month

    logger = QualIndLogger(
        log_dir=LOG_DIR,
        year=year,
        month=month,
    )

    period_str = f"{year}-{month:02d}"

    # Indicator A: explicit tolerances (percent, relative change)
    logger.log_indicator(
        "share_missing_id",
        {
            "title": "Share of records with missing person ID",
            "description": "Number of employment records without a valid person ID divided by total.",
            "value": float(v1),
            "unit": "percent",
            "data_period": period_str,
            # Explicit tolerances: 5% relative change is warning, 10% is critical
            "tol": {"warning": 0.05, "critical": 0.10},
        },
    )

    # Indicator B: NO explicit tol, will use auto tolerance (MAD-based)
    logger.log_indicator(
        "avg_employments_per_person",
        {
            "title": "Average number of employments per person",
            "description": "Mean number of concurrent employments for active persons.",
            "value": float(v2),
            "unit": "count",
            "data_period": period_str,
            # no 'tol' key → auto tolerance will be derived later
        },
    )

print("Finished logging synthetic indicators.")

# -------------------------------------------------------------------
# 3. Analyse latest period (December 2024) with auto-tolerances enabled
# -------------------------------------------------------------------

# ## The AutoToleranceConfig contains parameters used to estimate a reasonable tolerance relative change should not exceed
#
# The values in the config below are its default values, so it doesn't actually need to be specified.
#
# If no tolerance for the relative change e.g. {'warning': 0.05, 'critical': 0.1} is logged together with the indicator, like for 'avg_employments_per_person', then the QualIndLogger will estimate its own tolerance in the relative change space.
# By default this tolerance is estimated using MAD (median absolute deviation), which is similar to standard deviation, but less volatile/sensitive to outliers. 
# * n_hist determines how many period-observation the tolerance should be estimated from and the ref_strategy_for_sigma  determines which method to use to calculate relative change which is used in the tolerance estimation.
# * k_warning and k_critical determines the sigma factor for the tolerance.
# * use_mad is a bolean argument to indicate if you want to use MAD or normal standard deviation. 
#

# +
auto_cfg = AutoToleranceConfig(
    ref_strategy_for_sigma="median",  # relative change vs previous period
    n_hist=12,
    min_points=6,  # need at least 6 points
    k_warning=1.0,  # k_w * sigma_rel for warning
    k_critical=2.0,  # k_c * sigma_rel for critical
    use_mad=True,  # robust estimate
)

logger = QualIndLogger(
    log_dir=LOG_DIR,
    year=2024,
    month=12,
    auto_tol_config=auto_cfg,
)
# -

# -------------------------------------------------------------------
# 3a. Compare one indicator across periods (tabular)
# -------------------------------------------------------------------

a = logger.compare_periods(
    "share_missing_id",
    n_periods=5,
    ref_strategy="mean",
    style=True,
    print_style=True,
)

# -------------------------------------------------------------------
# 3b. Indicator without explicit tol → auto tolerance from history
# -------------------------------------------------------------------

a = logger.compare_periods(
    "avg_employments_per_person",
    n_periods=12,
    ref_strategy="median",
    style=True,
    print_style=True,
)

# Show what auto tolerance was inferred
tol_avg = logger.get_tolerance_for_indicator("avg_employments_per_person")
print("\nAuto tolerance for avg_employments_per_person:", tol_avg)

# -------------------------------------------------------------------
# 4. Systemize all indicators (long format)
# -------------------------------------------------------------------

long_df = logger.collect_long_df(
    n_periods=12,
    ref_strategy="median",
    style=False,
)
long_df

# -------------------------------------------------------------------
# 5. Filter rows that breach a given tolerance tier
# -------------------------------------------------------------------

# For a single indicator:
breaches_warning = logger.filter_breaches(
    df=long_df,
    indicator="share_missing_id",
    # tier="warning",
)
print("\n=== Warning breaches for share_missing_id ===")
print(breaches_warning.to_string(index=False))

# -------------------------------------------------------------------
# 6. Check pass/fail for latest period
# -------------------------------------------------------------------

a = logger.compare_periods(
    "share_missing_id",
    n_periods=5,
    ref_strategy="median",
    style=True,
    print_style=True,
)

# ### If the last row in the table above is red, then we expect the assert in this next cell to fail.

indicator='share_missing_id'
assert logger.check_latest_pass(
    indicator=indicator,
    n_periods=5,
    ref_strategy = 'median'), f"Latest period for indicator '{indicator}' failed with a relative change of {round(a.data['rel_change'].to_list()[-1]*100, 1)}%"

# -------------------------------------------------------------------
# 7. Optional: export to Excel
# -------------------------------------------------------------------

# ### Export to excel. Open excel file in new browser tab to download file. 

out_dir = Path("demo_kvalind_reports")
# out_dir.mkdir(exist_ok=True)
logger.export_kvalinds_to_excel(
    out_path=out_dir,
    # indicators=None,  # all indicators
    n_periods=12,
    ref_strategy="median",
)
print(f"\nExported Excel report to: {out_dir}")


