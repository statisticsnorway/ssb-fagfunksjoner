import getpass
import os
import typing
from pathlib import Path

import pandas as pd
import saspy


def saspy_session() -> saspy.SASsession:
    """Returnerer en initialisert sas-session, koblet mot default config."""
    brukernavn = getpass.getuser()
    authpath = "/ssb/bruker/" + brukernavn + "/.authinfo"
    if not os.path.exists(authpath):
        print("Finner ikke auth-file, vurder å kjøre saspy_session.set_password()")
        print(help(set_password))
    else:
        with open(authpath) as f:
            file = f.read()
            if "IOM_Prod_Grid1" not in file:
                print(
                    "IOM_Prod_Grid1 mangler fra .authinfo, prøv å kjør saspy_session.set_password() på nytt."
                )
                return
    felles = os.environ["FELLES"]
    cfgtype = "iomlinux"
    return saspy.SASsession(
        cfgname=cfgtype, cfgfile=f"{felles}/sascfg.py", encoding="latin1"
    )


def set_password(password: str):
    """Genererer en kryptert versjon av passordet ditt i SAS EG med denne koden (husk å bytte MiTT Passord med ditt eget Linux-passord):

    proc pwencode in='MiTT Passord' method=sas004;
    run;

    I log-vinduet i SAS EG ser du da det krypterte passordet for MiTT Passord som ser slik ut {SAS004}C598BA0A77F74607464634566CCD0D7BB8EBDEEA4B73C440.
    Send dette som parameter inn i denne funksjonen"""
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
    """Deler en path i tre, for innmating i libref hovedsaklig"""
    librefpath = str(path.parents[0])
    librefname = path.parts[-2]
    filename = ".".join(path.parts[-1].split(".")[:-1])
    return librefpath, librefname, filename


def saspy_df_from_path(path: str) -> pd.DataFrame:
    """Oppretter en session, setter libref, henter dataframe fra sas.
    Terminerer koblingen til sas, og returnerer dataframen."""
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
    """Uses saspy and sas-server to copy files"""
    return saspy_session().file_copy(from_path, to_path)
