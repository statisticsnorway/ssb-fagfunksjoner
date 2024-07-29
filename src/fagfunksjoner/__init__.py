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
            f"Error from ssb-fagfunksjoner __init__, not able to get version-number, setting it to %s. Exception: %s",
            version,
            str(passed_excep),
        )
    return version


# Gets the installed version from pyproject.toml, then there is no need to update this file
try:
    __version__ = importlib.metadata.version("ssb-fagfunksjoner")
except importlib.metadata.PackageNotFoundError as e:
    __version__ = _try_getting_pyproject_toml(e)
