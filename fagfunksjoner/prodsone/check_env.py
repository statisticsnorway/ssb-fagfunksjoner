"""To write "environment-aware code", we need to know where we are.
This module extracts information from the current environment,
and can help differentiate between the different places we develop code."""

import os


def check_env() -> str:
    """Check if you are on Dapla or in prodsone.

    Returns
    -------
    str
        "DAPLA" if on dapla, "PROD" if you are in prodsone.

    Raises
    ------
    OSError
        If no indications match, dapla/prod may have changed (please report)
        Or you are using the function outside of dapla/prod?
    """
    if "bruker" in os.listdir("/ssb"):
        env = "PROD"
    elif "DATA_MAINTENANCE_URL" in os.environ.keys():
        if "dapla" in os.environ["DATA_MAINTENANCE_URL"]:
            env = "DAPLA"
        else:
            raise ValueError("You are confusing me with your DATA_MAINTENANCE_URL")
    else:
        raise OSError("Ikke i prodsonen, eller på Dapla?")
    return env


def linux_shortcuts(insert_environ: bool = False) -> dict:
    """Manually load the "linux-forkortelser" in as dict, 
    if the function can find the file they are shared in.

    Parameters
    ----------
    insert_environ: bool
        Set to True if you want the dict to be inserted into the
        environment variables (os.environ).

    Returns
    -------
    dict
        The "linux-forkortelser" as a dict
    """
    stm = {}
    with open("/etc/profile.d/stamme_variabel") as stam_var:
        for line in stam_var:
            line = line.strip()
            if line.startswith("export") and "=" in line:
                line_parts = line.replace("export ", "").split("=")
                if len(line_parts) != 2:
                    raise ValueError("Too many equal-signs?")
                stm[line_parts[0]] = line_parts[1]
                if insert_environ:
                    os.environ[[line_parts[0]]] = line_parts[1]
    return stm
