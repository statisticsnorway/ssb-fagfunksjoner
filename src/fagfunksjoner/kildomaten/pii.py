import pandas as pd

from .config import KildomatConfig
from .kilde_logging import logger


def drop_configured_columns(
    df: pd.DataFrame,
    kild_config: KildomatConfig,
) -> pd.DataFrame:
    """Drop configured columns from output.

    Args:
        df: DataFrame that may contain columns configured for removal.
        kild_config: File-specific processing configuration.

    Returns:
        pd.DataFrame: The DataFrame without configured drop columns.
    """
    missing_cols = [column for column in kild_config.drop_cols if column not in df]
    if missing_cols:
        logger.warning(
            "Configured drop_cols are missing from output: %s",
            missing_cols,
        )

    drop_cols = [column for column in kild_config.drop_cols if column in df.columns]
    return df.drop(columns=drop_cols) if drop_cols else df


def _drop_original_fnr_columns(
    df: pd.DataFrame,
    kild_config: KildomatConfig,
) -> pd.DataFrame:
    original_cols = [
        f"{kild_config.fnr_col}_orig" if kild_config.fnr_col else "",
    ]
    drop_cols = [column for column in original_cols if column in df.columns]
    return df.drop(columns=drop_cols) if drop_cols else df
