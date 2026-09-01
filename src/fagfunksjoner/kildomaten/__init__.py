"""This module contains simplifications of the kildomaten processes on Dapla at Statistics Norway."""

from .fileconfig import FileConfig, WhodatSearchStrategy
from .kilde_logging import logger
from .pipeline import run_kildomaten_pipeline

__all__ = [
    "FileConfig",
    "WhodatSearchStrategy",
    "logger",
    "run_kildomaten_pipeline",
]
