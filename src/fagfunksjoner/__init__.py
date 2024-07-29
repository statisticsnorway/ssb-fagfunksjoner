from fagfunksjoner.paths.project_root import ProjectRoot
from fagfunksjoner.paths.versions import get_next_version_number
from fagfunksjoner.paths.versions import get_latest_fileversions
from fagfunksjoner.data.pandas_combinations import all_combos_agg
from fagfunksjoner.data.pandas_dtypes import auto_dtype
from fagfunksjoner.prodsone.check_env import check_env, linux_shortcuts
from fagfunksjoner.prodsone.saspy_ssb import saspy_session, saspy_df_from_path
from fagfunksjoner.data.view_dataframe import view_dataframe
from fagfunksjoner.data.datadok_extract import open_path_datadok, open_path_metapath_datadok

import importlib
import importlib.metadata
import toml

# Split into function for testing
def _try_getting_pyproject_toml(e: Exception | None = None) -> str:
    if e is None:
        passed_excep: Exception = Exception("")
    else:
        passed_excep = e
    try:
        version: str = toml.load("pyproject.toml")["tool"]["poetry"]["version"]
        return version
    except Exception as e:
        version_missing: str = "0.0.0"
        print(
            f"Error from ssb-fagfunksjoner __init__, not able to get version-number, setting it to {version_missing}: {passed_excep}"
        )
        return version_missing

# Gets the installed version from pyproject.toml, then there is no need to update this file
try:
    __version__ = importlib.metadata.version("ssb-fagfunksjoner")
except importlib.metadata.PackageNotFoundError as e:
    __version__ = _try_getting_pyproject_toml(e)
