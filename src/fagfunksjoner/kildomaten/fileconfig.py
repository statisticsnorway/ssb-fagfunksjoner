from collections.abc import Callable
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class FileConfig:
    """Configuration for processing one source file type."""

    fnr_col: str
    pseudo_cols: list[str]
    bruk_fnrleting: bool = False
    fnrleting_cols: list[str] = field(default_factory=list)
    rename_map: dict[str, str] = field(default_factory=dict)
    drop_cols: list[str] = field(default_factory=list)
    preprocess_func: Callable[[pd.DataFrame], pd.DataFrame] | None = None
