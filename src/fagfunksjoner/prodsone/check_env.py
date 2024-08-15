"""To write "environment-aware code", we need to know where we are.

This module extracts information from the current environment,
and can help differentiate between the different places we develop code.
"""

import os


def check_env(raise_err: bool = True) -> str:
    """Check if you are on Dapla or in prodsone.

    Args:
        raise_err: Set to False if you dont want the code to raise an error, on unrecognized environment.

    Returns:
        str: "DAPLA" if on dapla, "PROD" if you are in prodsone.

    Raises:
        OSError: If no indications match, dapla/prod may have changed (please report)
            Or you are using the function outside of dapla/prod on purpose?
    """
    jupyter_image_spec = os.environ.get("JUPYTER_IMAGE_SPEC")
    if jupyter_image_spec and "jupyterlab-dapla" in jupyter_image_spec:
        return "DAPLA"
    elif os.path.isdir("/ssb/bruker"):
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
