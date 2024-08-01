"""Fagfunksjoner is a place for "loose, small functionality" produced at Statistics Norway in Python.

Often created by "fag", not IT these are often small "helper-functions" that many might be interested in.
"""

import importlib
import importlib.metadata

import toml

from fagfunksjoner.fagfunksjoner_logger import logger


# Split into function for testing
def _try_getting_pyproject_toml(e: Exception | None = None) -> str:
    if e is None:
        passed_excep: Exception = Exception("")
    else:
        passed_excep = e
    try:
        try:
            version: str = toml.load("../pyproject.toml")["tool"]["poetry"]["version"]
        except FileNotFoundError:
            version = toml.load("./pyproject.toml")["tool"]["poetry"]["version"]
    except toml.TomlDecodeError as e:
        version = "0.0.0"
        logger.exception(
            """Error from ssb-fagfunksjoner __init__, not able to get version-number, setting it to %s.
            Exception: %s.
            Tomlexception: %s""",
            version,
            str(passed_excep),
            str(e),
        )
    return version


# Gets the installed version from pyproject.toml, then there is no need to update this file
try:
    __version__ = importlib.metadata.version("ssb-fagfunksjoner")
except importlib.metadata.PackageNotFoundError as e:
    __version__ = _try_getting_pyproject_toml(e)


from fagfunksjoner.data.datadok_extract import open_path_datadok
from fagfunksjoner.data.datadok_extract import open_path_metapath_datadok
from fagfunksjoner.data.pandas_combinations import all_combos_agg
from fagfunksjoner.data.pandas_dtypes import auto_dtype
from fagfunksjoner.data.view_dataframe import view_dataframe
from fagfunksjoner.paths.project_root import ProjectRoot
from fagfunksjoner.paths.versions import get_latest_fileversions
from fagfunksjoner.paths.versions import get_next_version_number
from fagfunksjoner.prodsone.check_env import check_env
from fagfunksjoner.prodsone.check_env import linux_shortcuts
from fagfunksjoner.prodsone.saspy_ssb import saspy_df_from_path
from fagfunksjoner.prodsone.saspy_ssb import saspy_session

__all__ = [
    "ProjectRoot",
    "get_next_version_number",
    "get_latest_fileversions",
    "all_combos_agg",
    "auto_dtype",
    "check_env",
    "linux_shortcuts",
    "saspy_session",
    "saspy_df_from_path",
    "view_dataframe",
    "open_path_datadok",
    "open_path_metapath_datadok",
]
