"""One of Statistics Norway's biggest internal databases is called "DB1P"
This module aims to make it easier to query this oracle database from Python."""

import getpass
import os

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv


def query_db1p(query: str) -> pd.DataFrame:
    """Function for getting a pandas dataframe from DB1P oracle.
    Send in a full SQL-query as a string.
    Requires that getpass.getuser gets the right user.
    If you dont put the variable "DB1P_PASSWORD" in a .env file in your user-folder,
    It'll ask you for a password everytime it runs.

    Parameters
    ----------
    query: str
        The SQL-query you would like to run, as a single string.
        Can be triple-quoted.

    Returns
    -------
    pandas.DataFrame
        The content returned by DB1P converted to a pandas dataframe.
    """
    load_dotenv()
    # User has option to store password in .env / environment variables
    if "DB1P_PASSWORD" in os.environ.keys():
        engine = sqlalchemy.create_engine(
            f"oracle+cx_oracle://{getpass.getuser()}:{os.environ['DB1P_PASSWORD']}@DB1P"
        )
    else:
        engine = sqlalchemy.create_engine(
            f"oracle+cx_oracle://{getpass.getuser()}:{getpass.getpass(f'Passord for {getpass.getuser()} på DB1P:')}@DB1P"
        )
    return pd.read_sql_query(query, engine)
