import pandas as pd

from .globals import CANON_PERSNAVN


def drop_sensitive_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop sensitive personally identifying columns from output.

    Args:
        df: DataFrame that may contain sensitive person columns.

    Returns:
        pd.DataFrame: The DataFrame without sensitive person columns.
    """
    drop_cols = [
        c
        for c in (
            CANON_PERSNAVN,
            "pers_personnummer",
        )
        if c in df.columns
    ]
    return df.drop(columns=drop_cols) if drop_cols else df
