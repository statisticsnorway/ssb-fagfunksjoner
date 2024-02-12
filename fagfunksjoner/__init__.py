from .paths.project_root import ProjectRoot
from .data.pandas_combinations import all_combos_agg
from .data.pandas_dtypes import auto_dtype
from .prodsone.check_env import check_env, linux_shortcuts
from .prodsone.saspy_ssb import saspy_session, saspy_df_from_path
from .data.view_dataframe import view_dataframe


import importlib
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
            f"Error from ssb-klass-pythons __init__, not able to get version-number, setting it to {version_missing}: {passed_excep}"
        )
        return version_missing

# Gets the installed version from pyproject.toml, then there is no need to update this file
try:
    __version__ = importlib.metadata.version("ssb-klass-python")
except importlib.metadata.PackageNotFoundError as e:
    __version__ = _try_getting_pyproject_toml(e)