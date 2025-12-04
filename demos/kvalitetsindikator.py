from pathlib import Path

import numpy as np
import pandas as pd

from fagfunksjoner.kvalitetsindikator.qualind import AutoToleranceConfig, QualIndLogger


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
# print("\n=== share_missing_id, previous-period rel_change ===")
# print(df_missing.to_string(index=False))

# -------------------------------------------------------------------
# 3b. Same indicator, but styled using explicit tol (warning/critical)
# -------------------------------------------------------------------

a = logger.compare_periods(
    "share_missing_id",
    n_periods=12,
    ref_strategy="median",
    style=True,
    print_style=True,  # set True in a notebook to display directly
)

# -------------------------------------------------------------------
# 3c. Indicator without explicit tol → auto tolerance from history
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

a = logger.compare_periods(
    "avg_employments_per_person",
    n_periods=25,
    ref_strategy="median",
    style=True,
    print_style=True,
)

# -------------------------------------------------------------------
# 4. Systemize all indicators (long format) and style it
# -------------------------------------------------------------------

long_df = logger.systemize_process_data(
    n_periods=12,
    ref_strategy="rolling_median",
    style=False,
)

# -------------------------------------------------------------------
# 5. Filter rows that breach a given tolerance tier
# -------------------------------------------------------------------

# For a single indicator:
breaches_warning = logger.filter_by_tolerance(
    df=long_df,
    indicator="share_missing_id",
    # tier="warning",
)
print("\n=== Warning breaches for share_missing_id ===")
print(breaches_warning.to_string(index=False))

# -------------------------------------------------------------------
# 6. Check pass/fail for latest period
# -------------------------------------------------------------------

# check_pass expects df with a single indicator in 'indicator' column
df_missing_with_ind = long_df.copy()
df_missing_with_ind["indicator"] = "share_missing_id"

all_statuses = logger.check_pass()
# e.g. {"avg_employments_per_person": True, "something_else": True, ...}
if not all(all_statuses.values()):
    failing = [k for k, v in all_statuses.items() if not v]
    print("Indicators failing:", failing)
else:
    print("All indicators passed")

# -------------------------------------------------------------------
# 7. Optional: export to Excel
# -------------------------------------------------------------------

# from fagfunksjoner.kvalitetsindikator.qualind import make_wide_df  # or wherever it's defined

out_dir = Path("demo_kvalind_reports")
# out_dir.mkdir(exist_ok=True)
logger.export_kvalinds_to_excel(
    out_path=out_dir,
    # indicators=None,  # all indicators
    # change_cols=["rel_change"],
    n_periods=12,
    # ref_strategy="median",
)
print(f"\nExported Excel report to: {out_dir}")
