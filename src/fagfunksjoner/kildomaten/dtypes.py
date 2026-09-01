from typing import Final, Literal

import pandas as pd

from .fileconfig import FileConfig

BOOL_PYARROW_DTYPE: Final[Literal["bool[pyarrow]"]] = "bool[pyarrow]"
DOUBLE_PYARROW_DTYPE: Final[Literal["double[pyarrow]"]] = "double[pyarrow]"
INT64_PYARROW_DTYPE: Final[Literal["int64[pyarrow]"]] = "int64[pyarrow]"
STRING_PYARROW_DTYPE: Final[Literal["string[pyarrow]"]] = "string[pyarrow]"


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
        if dtype in ("boolean", "bool"):
            out[col] = out[col].astype(BOOL_PYARROW_DTYPE)
        elif dtype == "Int64":
            out[col] = out[col].astype(INT64_PYARROW_DTYPE)
        elif dtype == "Float64":
            out[col] = out[col].astype(DOUBLE_PYARROW_DTYPE)
        elif "object" in dtype or "string" in dtype:
            out[col] = out[col].astype(STRING_PYARROW_DTYPE)
    return out
