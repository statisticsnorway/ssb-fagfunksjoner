import pandas as pd

from fagfunksjoner.fagfunksjoner_logger import logger
from fagfunksjoner.prodsone.oradb import Oracle


def dynarev_uttrekk(
    delreg_nr: str,
    skjema: str,
    dublettsjekk: bool = False,
    sfu_cols: str | list[str] | bool | None = None,
) -> pd.DataFrame | tuple[pd.DataFrame, ...]:
    """Fetches and processes data from the Oracle database using the Oracle class for connection management.

    Args:
        delreg_nr (str): Delregisternummer.
        skjema (str): Skjemanavn.
        dublettsjekk (bool) : If True, checks for and returns duplicates.
        sfu_cols (list[str | str. | bool | None) : Specify a list of columns for SFU data, or a single column as a string.
            If True picks all columns. If None, skips getting sfu-data.

    Returns:
        pd.DataFrame | tuple[pd.DataFrame]: A dataframe, or tuple of dataframes if you wanted sfu-data / dupe-check.

    Raises:
        ValueError: If the sfu_cols parameter does not fit expectations.
    """
    db_name = input("Name of Oracle Database: ")
    oracle_conn = Oracle(db=db_name)

    # Use a try to guarantee that the oracle-connection is closed with a finally-clause.
    try:
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

        pivot_cols = ["enhets_id", "enhets_type", "delreg_nr", "lopenr", "rad_nr"]
        df_all_data_pivot = df_all_data.pivot_table(
            index=pivot_cols, columns="felt_id", values="felt_verdi", aggfunc="first"
        ).reset_index()
        result: list[pd.DataFrame] = [df_all_data_pivot]

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
            result.append(dublett)

        if sfu_cols:
            # Limit cols we are querying for by making a sql-select string
            if sfu_cols is True:
                sfu_select = "b.*"
            elif isinstance(sfu_cols, str):
                sfu_select = f"b.{sfu_cols}"
            elif isinstance(sfu_cols, list) and all(
                isinstance(item, str) for item in sfu_cols
            ):
                sfu_select = ", ".join([f"b.{col}" for col in sfu_cols])
            else:
                logger.warning("Invalid sfu_cols parameter.")
                raise ValueError("Invalid sfu_cols parameter.")

            # Use the select string to actually get the sfu-data.
            query_sfu = f"""
                SELECT {sfu_select}
                FROM dsbbase.dlr_enhet_i_delreg_skjema a, dsbbase.dlr_enhet_i_delreg b
                WHERE a.delreg_nr = b.delreg_nr
                  AND a.ident_nr = b.ident_nr
                  AND a.enhets_type = b.enhets_type
                  AND b.prosedyre IS NULL
                  AND a.delreg_nr = {delreg_nr}
                  AND a.skjema_type = '{skjema}'
            """
            logger.info(query_sfu)
            result.append(pd.DataFrame(oracle_conn.select(sql=query_sfu)))

        if len(result) == 1:
            return result[0]
        else:
            return tuple(result)
    except Exception as e:
        logger.warning(f"Failed to execute queries: {e}")
        return pd.DataFrame()
    finally:
        oracle_conn.close()
