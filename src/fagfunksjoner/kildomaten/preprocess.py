import pandas as pd

from .fileconfig import FileConfig
from .kilde_logging import logger


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df


def _apply_configured_preprocessing(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> pd.DataFrame:
    df = _normalize_column_names(df)
    if file_config.preprocess_func:
        df = file_config.preprocess_func(df)
    if file_config.rename_map:
        file_config.rename_map = {
            k.strip(): v.strip() for k, v in file_config.rename_map.items()
        }
        missing_sources = [
            column for column in file_config.rename_map if column not in df.columns
        ]
        if missing_sources:
            logger.warning(
                "Configured rename_map source columns are missing from input: %s",
                missing_sources,
            )
        df = df.rename(columns=file_config.rename_map)
    if file_config.copy_cols_new_old:
        for new_col, old_col in file_config.copy_cols_new_old.items():
            if new_col in df.columns:
                logger.warning(
                    "Configured copy_cols_new_old target column already exists; "
                    "skipping copy to avoid overwriting: %s -> %s",
                    old_col,
                    new_col,
                )
                continue
            if old_col not in df.columns:
                logger.warning(
                    "Configured copy_cols_new_old source column is missing from "
                    "input: %s -> %s",
                    old_col,
                    new_col,
                )
                continue
            df[new_col] = df[old_col].copy()
    df = _normalize_column_names(df)
    return df
