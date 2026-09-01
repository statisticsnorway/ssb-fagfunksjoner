from pathlib import Path

import pandas as pd


def _load_input(source: pd.DataFrame | str | Path) -> tuple[pd.DataFrame, Path | None]:
    if isinstance(source, pd.DataFrame):
        return source.copy(), None

    source_path = Path(source)
    if source_path.suffix.lower() != ".parquet":
        raise ValueError(f"Expected a parquet file, got: {source_path}")

    df = pd.read_parquet(source_path, dtype_backend="pyarrow")
    return df, source_path
