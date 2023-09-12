"""Simplifications of saspy package for SSB use. 
Helps you store password in prodsone.
Sets libnames automatically for you when just wanting to open a file, 
or convert it."""


import getpass
import os
import typing
from pathlib import Path

import pandas as pd
import saspy


def saspy_session() -> saspy.SASsession:
    """Gives you an initialized saspy.SASsession object,
    using the default config, getting your password if youve set one.
    
    Returns
    -------
    saspy.SASsession
        An initialized saspy-session
    """
    brukernavn = getpass.getuser()
    authpath = "/ssb/bruker/" + brukernavn + "/.authinfo"
    if not os.path.exists(authpath):
        print("Cant find the auth-file, consider running saspy_session.set_password()")
        print(help(set_password))
    else:
        with open(authpath) as f:
            file = f.read()
            if "IOM_Prod_Grid1" not in file:
                print(
                    "IOM_Prod_Grid1 is missing from .authinfo, try running saspy_session.set_password() again."
                )
                return
    felles = os.environ["FELLES"]
    cfgtype = "iomlinux"
    return saspy.SASsession(
        cfgname=cfgtype, cfgfile=f"{felles}/sascfg.py", encoding="latin1"
    )


def set_password(password: str):
    """Pass into this function, an encrypted version of your password that you can get
    in SAS EG, running the following code (swap MY PASSWORD for your actual common-password):

    proc pwencode in='MY PASSWORD' method=sas004;
    run;

    In the log-window in SAS EG you should then recieve an encrypted version of your password,
    that looks something like this {SAS004}C598BA0A77F74607464634566CCD0D7BB8EBDEEA4B73C440
    Send this as the parameter into this function.

    Parameters
    ----------
    password: str
        Your password encrypted using SAS EG
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


def split_path_for_sas(path: Path) -> typing.Tuple[str, str, str]:
    """Splits a path in three parts, mainly for having a name for the libname

    Parameters
    ----------
    path: pathlib.Path
        The full path to be split

    Returns
    -------
    tuple[str]
        The three parts the path has been split into.
    """
    librefpath = str(path.parents[0])
    librefname = path.parts[-2]
    filename = ".".join(path.parts[-1].split(".")[:-1])
    return librefpath, librefname, filename


def saspy_df_from_path(path: str) -> pd.DataFrame:
    """Gives you a pandas dataframe from the path to a sasfile, using saspy.
    Creates saspy-session, create a libref, gets the dataframe,
    terminates the connection to saspy cleanly, and returns the dataframe.

    Parameters
    ----------
    path: str
        The full path to the sasfile you want to open with sas.
    
    Returns
    -------
    pandas.DataFrame
        The raw content of the sasfile straight from saspy
    """
    librefpath, librefname, filename = split_path_for_sas(Path(path))
    librefname = "sasdata"  # Simplify... blergh
    sas = saspy_session()
    try:
        libref = sas.saslib(librefname, path=librefpath)
        df = sas.sasdata2dataframe(filename, libref=librefname)
    except Exception as e:
        print(e)
    finally:
        # sas.disconnect()
        sas._endsas()
    return df


def sasfile_to_parquet(
    path: str, out_path: str = "", gzip: bool = False
) -> pd.DataFrame:
    """Converts a sasfile directly to a parquetfile, using saspy and pandas.

    Parameters
    ----------
    path: str
        The path to the in-sas-file.
    out_path: str
        The path to place the parquet-file on
    gzip: bool
        If you want the parquetfile gzipped or not.

    Returns
    -------
    pandas.DataFrame
        In case you want to use the content for something else.
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
        print(path, out_path)
        df.to_parquet(out_path, compression="gzip")
    else:
        out_path = out_path.with_suffix(".parquet")
        print(path, out_path)
        df.to_parquet(out_path)
    print(f"Outputted to {out_path}")
    return df


def cp(from_path: str, to_path: str) -> dict:
    """Uses saspy and sas-server to copy files

    Parameters
    ----------
    from_path: str
        The path for the source file to copy
    to_path: str
        The path to place the copy on

    Returns
    -------
    dict
        A key for if it succeded, and a key for holding the log as string.
    """
    return saspy_session().file_copy(from_path, to_path)
