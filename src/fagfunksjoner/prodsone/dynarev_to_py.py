import warnings

import pandas as pd

from fagfunksjoner.prodsone.oradb import Oracle
from fagfunksjoner.fagfunksjoner_logger import logger


def dynarev_uttrekk(
    delreg_nr: str, skjema: str, dublettsjekk: bool = False, sfu_cols: list = []
):
    """
    Fetches and processes data from the Oracle database using the Oracle class for connection management.

    Parameters:
        delreg_nr : Delregisternummer.
        skjema : Skjemanavn.
        dublettsjekk (optional) : If True, checks for and returns duplicates.
        sfu_cols (optional) : Specify a list of columns for SFU data; if True, returns all SFU columns.

    Returns:
        A dataframe or tuple of dataframes based on the input options.
    """
    db_name = input("Name of Oracle Database: ")
    oracle_conn = Oracle(db=db_name)  # Create an Oracle connection object

    try:
        # SQL query to fetch all data
        query_all_data = f"""
            SELECT *
            FROM DYNAREV.VW_SKJEMA_DATA
            WHERE delreg_nr = {delreg_nr}
              AND skjema = '{skjema}'
              AND enhets_type = 'BEDR'
              AND rad_nr = 0
              AND aktiv = 1
        """
        df_all_data = pd.DataFrame(oracle_conn.select(sql=query_all_data))
        logger.info(f"Data fetched successfully. Number of rows: {len(df_all_data)}")

        # Pivot the data
        pivot_cols = ["enhets_id", "enhets_type", "delreg_nr", "lopenr", "rad_nr"]
        df_all_data_pivot = df_all_data.pivot_table(
            index=pivot_cols, columns="felt_id", values="felt_verdi", aggfunc="first"
        ).reset_index()
        result = [
            df_all_data_pivot
        ]  # Store the original pivoted dataframe in the result list

        # Check for duplicates if required
        if dublettsjekk:
            query_dublett = f"""
                SELECT enhets_id, COUNT(*) AS antall_skjemaer
                FROM DYNAREV.VW_SKJEMA_DATA
                WHERE skjema = '{skjema}'
                  AND enhets_type = 'BEDR'
                  AND rad_nr = 0
                  AND aktiv = 1
                  AND delreg_nr = {delreg_nr}
                GROUP BY enhets_id
                HAVING COUNT(*) > 1
            """
            dublett = pd.DataFrame(oracle_conn.select(sql=query_dublett))

            if not len(dublett):
                warnings.warn(
                    "Så etter dubletter, men fant ingen, dublettdataframen er derfor tom"
                )
            else:
                logger.info("Dublett-data fetched")
            result.append(dublett)

        # Fetch SFU data if required
        if sfu_cols:
            query_sfu = f"""
                SELECT b.*
                FROM dsbbase.dlr_enhet_i_delreg_skjema a, dsbbase.dlr_enhet_i_delreg b
                WHERE a.delreg_nr = b.delreg_nr
                  AND a.ident_nr = b.ident_nr
                  AND a.enhets_type = b.enhets_type
                  AND b.prosedyre IS NULL
                  AND a.delreg_nr = {delreg_nr}
                  AND a.skjema_type = '{skjema}'
            """
            sfu = pd.DataFrame(oracle_conn.select(sql=query_sfu))

            if sfu_cols == True:
                logger.info("Taking out SFU data with all columns.")
            else:
                sfu = sfu[sfu_cols]
                logger.info("Taking out SFU data with specific columns: ", *sfu_cols)
            result.append(sfu)

        if len(result) == 1:
            return result[0]
        else:
            return tuple(result)
    except Exception as e:
        logger.warn(f"Failed to execute queries: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on failure
    finally:
        oracle_conn.close()  # Ensure the connection is closed after operations
