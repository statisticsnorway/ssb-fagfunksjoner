import pandas as pd

from .fileconfig import FileConfig


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
        df = df.rename(columns=file_config.rename_map)
    if file_config.preprocess_func or file_config.rename_map:
        df = _normalize_column_names(df)
    return df
