"""To write "environment-aware code", we need to know where we are.

This module extracts information from the current environment,
and can help differentiate between the different places we develop code.
"""

import os

from dapla.auth import AuthClient, DaplaRegion


def check_env(raise_err: bool = True) -> str:
    """Check if you are on Dapla or in prodsone.

    Args:
        raise_err (bool): Set to False if you don't want the code to raise an error on an unrecognized environment.

    Returns:
        str: "DAPLA" if on Dapla, "PROD" if in prodsone, otherwise "UNKNOWN".

    Raises:
        OSError: If no environment indications match (Dapla or Prod), and raise_err is set to True.
    """
    try:
        current_region = AuthClient.get_dapla_region()
        if current_region in [DaplaRegion.DAPLA_LAB, DaplaRegion.BIP]:
            return "DAPLA"
    except AttributeError:
        pass

    if os.path.isdir("/ssb/bruker"):
        return "PROD"
    elif raise_err:
        raise OSError("Not on Dapla or in Prodsone, where are we dude?")

    return "UNKNOWN"


def linux_shortcuts(insert_environ: bool = False) -> dict[str, str]:
    """Manually load the "linux-forkortelser" in as dict.

    If the function can find the file they are shared in.

    Args:
        insert_environ: Set to True if you want the dict to be inserted into the
            environment variables (os.environ).

    Returns:
        dict[str, str]:  The "linux-forkortelser" as a dict

    Raises:
        ValueError: If the stamme_variabel file is wrongly formatted.
    """
    stm: dict[str, str] = {}
    with open("/etc/profile.d/stamme_variabel") as stam_var:
        for line in stam_var:
            line = line.strip()
            if line.startswith("export") and "=" in line:
                line_parts = line.replace("export ", "").split("=")
                if len(line_parts) != 2:
                    raise ValueError("Too many equal-signs?")
                first: str = line_parts[0]  # Helping mypy
                second: str = line_parts[1]  # Helping mypy
                stm[first] = second
                if insert_environ:
                    os.environ[first] = second
    return stm
