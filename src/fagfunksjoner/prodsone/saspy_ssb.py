"""Simplifications of saspy package for SSB use.

Helps you store password in prodsone.
Sets libnames automatically for you when just wanting to open a file,
or convert it.
"""

import getpass
import os
import re
import shutil
from pathlib import Path

import pandas as pd
import saspy

from fagfunksjoner.fagfunksjoner_logger import logger


def saspy_session() -> saspy.SASsession:
    """Get an initialized saspy.SASsession object.

    Use the default config, getting your password if you've set one.

    Returns:
        saspy.SASsession: An initialized saspy-session
    """
    brukernavn = getpass.getuser()
    authpath = "/ssb/bruker/" + brukernavn + "/.authinfo"
    if not os.path.exists(authpath):
        logger.warn(
            "Cant find the auth-file, consider running saspy_session.set_password()"
        )
        logger.info(help(set_password))
    else:
        with open(authpath) as f:
            file = f.read()
            if "IOM_Prod_Grid1" not in file:
                logger.warn(
                    "IOM_Prod_Grid1 is missing from .authinfo, try running saspy_session.set_password() again."
                )
                return
    felles = os.environ["FELLES"]
    cfgtype = "iomlinux"
    cfgfile_user = f"/ssb/bruker/{brukernavn}/sascfg.py"
    cfgfile_general = f"{felles}/sascfg.py"
    if os.path.exists(cfgfile_user):
        cfgfile = cfgfile_user
    else:
        cfgfile = cfgfile_general
    return saspy.SASsession(cfgname=cfgtype, cfgfile=cfgfile, encoding="latin1")


def set_password(password: str) -> None:
    """Pass into this function, an encrypted version of your password.

    Get the encrypted password in SAS EG, running the following code
    (swap MY PASSWORD for your actual common-password):

    proc pwencode in='MY PASSWORD' method=sas004;
    run;

    In the log-window in SAS EG you should then recieve an encrypted version of your password,
    that looks something like this {SAS004}C598BA0A77F74607464634566CCD0D7BB8EBDEEA4B73C440
    Send this as the parameter into this function.

    Args:
        password (str): Your password encrypted using SAS EG
    """
    brukernavn = getpass.getuser()
    authpath = "/ssb/bruker/" + brukernavn + "/.authinfo"

    # If file exists
    if os.path.exists(authpath):
        # Read file into new variable
        file_replaced = ""
        with open(authpath) as f:
            for line in f:
                # Replacing the specific line
                if "IOM_Prod_Grid1" in line:
                    file_replaced += (
                        "IOM_Prod_Grid1 user "
                        + brukernavn
                        + " password "
                        + password
                        + "\n"
                    )
                else:
                    file_replaced += line
        # Write out the modified authinfo-file
        with open(authpath, "w") as f:
            f.write(file_replaced)
    # If file doesnt exist, write directly
    else:
        with open(authpath, "w") as f:
            f.write("IOM_Prod_Grid1 user " + brukernavn + " password " + password)
    os.chmod(authpath, 0o600)


def swap_server(new_server: int) -> None:
    """Swap between the sas-servers you connect to with saspy.

    Args:
        new_server (int): The server number to switch to.
    """
    felles = os.environ["FELLES"]
    brukernavn = getpass.getuser()
    cfgfile_user = f"/ssb/bruker/{brukernavn}/sascfg.py"
    cfgfile_general = f"{felles}/sascfg.py"
    if not os.path.exists(cfgfile_user):
        logger.info(
            f"Making a new copy of sascfg.py in your folder /ssb/bruker/{brukernavn}"
        )
        shutil.copy(cfgfile_general, cfgfile_user)
    else:
        logger.info(
            f"Found an existing copy of sascfg.py in your folder /ssb/bruker/{brukernavn}"
        )
    with open(cfgfile_user) as f:
        content = f.read()
    new_content = []
    for line in content.split("\n"):
        if "sl-sas-work-" in line:
            line = re.sub(
                r"sl-sas-work-.*\.ssb\.no", f"sl-sas-comp-p{new_server}.ssb.no", line
            )
            logger.info(f"Setting server to {new_server} with resulting line: {line}")
        if "sl-sas-comp-p" in line:
            line = re.sub(
                r"sl-sas-comp-p.*\.ssb\.no", f"sl-sas-comp-p{new_server}.ssb.no", line
            )
            logger.info(f"Setting server to {new_server} with resulting line: {line}")
        new_content += [line]
    with open(cfgfile_user, "w") as f:
        f.write("\n".join(new_content))


def split_path_for_sas(path: Path) -> tuple[str, str, str]:
    """Split a path in three parts, mainly for having a name for the libname.

    Args:
        path (pathlib.Path): The full path to be split

    Returns:
        tuple[str]: The three parts the path has been split into.
    """
    librefpath = str(path.parents[0])
    librefname = path.parts[-2]
    filename = ".".join(path.parts[-1].split(".")[:-1])
    return librefpath, librefname, filename


def saspy_df_from_path(path: str) -> pd.DataFrame:
    """Return a pandas dataframe from the path to a sasfile, using saspy.

    Creates saspy-session, create a libref, gets the dataframe,
    terminates the connection to saspy cleanly, and returns the dataframe.

    Args:
        path (str): The full path to the sasfile you want to open with sas.

    Returns:
        pandas.DataFrame: The raw content of the sasfile straight from saspy
    """
    librefpath, librefname, filename = split_path_for_sas(Path(path))
    librefname = "sasdata"  # Simplify... blergh
    sas = saspy_session()
    try:
        _ = sas.saslib(librefname, path=librefpath)
        df = sas.sasdata2dataframe(filename, libref=librefname)
    except Exception as e:
        logger.error(e)
    finally:
        # sas.disconnect()
        sas._endsas()
    return df


def sasfile_to_parquet(
    path: str, out_path: str = "", gzip: bool = False
) -> pd.DataFrame:
    """Convert a sasfile directly to a parquetfile, using saspy and pandas.

    Args:
        path (str): The path to the in-sas-file.
        out_path (str): The path to place the parquet-file on
        gzip (bool): If you want the parquetfile gzipped or not.

    Returns:
        pandas.DataFrame: In case you want to use the content for something else.
            I mean, we already read it into memory...
    """
    path = Path(path)
    df = saspy_df_from_path(path)
    if not out_path:
        out_path = path
    else:
        out_path = Path(out_path)
        out_path = out_path.parent.joinpath(
            out_path.stem.split(".")[0]
        )  # Avoid extra file extensions

    if gzip:
        out_path = out_path.with_suffix(".parquet.gzip")
        logger.info(path, out_path)
        df.to_parquet(out_path, compression="gzip")
    else:
        out_path = out_path.with_suffix(".parquet")
        logger.info(path, out_path)
        df.to_parquet(out_path)
    logger.info(f"Outputted to {out_path}")
    return df


def cp(from_path: str, to_path: str) -> dict:
    """Use saspy and sas-server to copy files.

    Args:
    from_path (str): The path for the source file to copy
    to_path (str): The path to place the copy on

    Returns:
        dict: A key for if it succeded, and a key for holding the log as string.
    """
    return saspy_session().file_copy(from_path, to_path)
