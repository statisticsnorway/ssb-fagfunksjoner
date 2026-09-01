import uuid

import pandas as pd


def _fill_uuid_for_missing_snr(
    df: pd.DataFrame,
    *,
    snr_col: str = "snr",
) -> tuple[pd.DataFrame, int]:
    """Fill missing SNR values with UUIDs and return the updated DataFrame and count."""
    df = df.copy()
    s = df[snr_col].astype("string[pyarrow]").str.strip()
    miss = s.isna() | (s == "")
    n = int(miss.sum())
    if n:
        df.loc[miss, snr_col] = pd.Series(
            [str(uuid.uuid4()) for _ in range(n)],
            index=df.index[miss],
            dtype="string[pyarrow]",
        )
    return df, n
