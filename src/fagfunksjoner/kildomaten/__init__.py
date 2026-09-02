"""This module contains simplifications of the kildomaten processes on Dapla at Statistics Norway."""

from .config import KildomatConfig, WhodatSearchStrategy
from .kilde_logging import logger
from .pipeline import run_kildomaten_pipeline

__all__ = [
    "KildomatConfig",
    "WhodatSearchStrategy",
    "logger",
    "run_kildomaten_pipeline",
]
