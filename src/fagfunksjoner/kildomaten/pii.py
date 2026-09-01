import pandas as pd

from .fileconfig import FileConfig
from .kilde_logging import logger


def drop_sensitive_columns(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> pd.DataFrame:
    """Drop sensitive personally identifying columns from output.

    Args:
        df: DataFrame that may contain sensitive person columns.
        file_config: File-specific processing configuration.

    Returns:
        pd.DataFrame: The DataFrame without sensitive person columns.
    """
    configured_drop_cols = [*file_config.drop_cols, *file_config.sensitive_cols]
    missing_cols = [column for column in configured_drop_cols if column not in df]
    if missing_cols:
        logger.warning(
            "Configured drop_cols/sensitive_cols are missing from output: %s",
            missing_cols,
        )

    drop_cols = [column for column in configured_drop_cols if column in df.columns]
    return df.drop(columns=drop_cols) if drop_cols else df


def _drop_original_fnr_columns(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> pd.DataFrame:
    original_cols = [
        f"{file_config.fnr_col}_orig" if file_config.fnr_col else "",
    ]
    drop_cols = [column for column in original_cols if column in df.columns]
    return df.drop(columns=drop_cols) if drop_cols else df
