import pandas as pd
import sqlalchemy
import getpass
import os
from dotenv import load_dotenv

def query_db1p(query: str) -> pd.DataFrame:
    """
    Funksjon for å hente en dataframe fra DB1P oracle.
    Send inn ferdig SQL-query som en string.
    Forutsetter at getpass.getuser skaffer rett bruker, vil be om passord hver gang...
    Om du ikke setter "DB1P_PASSWORD" i en .env eller i environment-variablene.
    """
    load_dotenv()
    # User has option to store password in .env / environment variables
    if 'DB1P_PASSWORD' in os.environ.keys():
        engine = sqlalchemy.create_engine(f"oracle+cx_oracle://{getpass.getuser()}:{os.environ['DB1P_PASSWORD']}@DB1P")
    else:
        engine = sqlalchemy.create_engine(f"oracle+cx_oracle://{getpass.getuser()}:{getpass.getpass(f'Passord for {getpass.getuser()} på DB1P:')}@DB1P")
    return pd.read_sql_query(query, engine)

