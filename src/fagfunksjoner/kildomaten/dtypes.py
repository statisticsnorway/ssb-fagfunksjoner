import pandas as pd

from .fileconfig import FileConfig


def normalize_dtypes(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> pd.DataFrame:
    """Normalize DataFrame columns to canonical pyarrow dtypes.

    Args:
        df: DataFrame to normalize.
        file_config: File-specific processing configuration.

    Returns:
        pd.DataFrame: A copy of the DataFrame with canonical pyarrow dtypes.
    """
    out = df.copy()
    out = out.convert_dtypes(dtype_backend="pyarrow")
    for col in out.columns:
        dtype = str(out[col].dtype)
        if col == file_config.snr_mark_col:
            out[col] = out[col].astype("bool[pyarrow]")
        elif dtype in ("boolean", "bool"):
            out[col] = out[col].astype("bool[pyarrow]")
        elif dtype == "Int64":
            out[col] = out[col].astype("int64[pyarrow]")
        elif dtype == "Float64":
            out[col] = out[col].astype("double[pyarrow]")
        elif "object" in dtype or "string" in dtype:
            out[col] = out[col].astype("string[pyarrow]")
    return out
