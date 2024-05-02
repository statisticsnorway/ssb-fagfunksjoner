from fagfunksjoner.prodsone.oradb import Oracle

# Laster inn nødvendige bibliotek
import pandas as pd


# Funksjon for hente ut dynarev-data:
def dynarev_uttrekk(
    delreg_nr: str, skjema: str, dublettsjekk: bool = False, sfu_cols: list = []
):
    """
    Parameters:
        delreg_nr : Delregisternummer.
        skjema : Skjemanavn.
        dublettsjekk (optional) : If set to True, will return a data.frame of duplicates. Default is False.
        sfu_cols (optional) : Provide a list for a subset of columns. If set to True, will return all columns in SFU for that delreg_nr and skjema. Default is an empty list (no sfu output).

    Returns:
        A dataframe or list of dataframes.
    """
    # Setter opp oppkobling mot i Oracle
    conn = Oracle(input("Name of Oracle Database: "))

    # SQL for metadata
    query_meta = f"""
        SELECT DISTINCT b.felt_type, a.FELT_ID
            FROM dynarev.vw_skjema_data a,
                dynarev.vw_skjema_metadata b
            WHERE a.delreg_nr   = {delreg_nr}
            AND a.delreg_nr   = b.delreg_nr
            AND a.skjema      = '{skjema}'
            AND a.skjema      = b.skjema
            AND a.felt_id     = b.felt_id
            AND a.rad_nr      = 0
            AND a.aktiv       = 1
     """
    # Henter ut metdataene
    metadata_dynarev = pd.DataFrame(conn.select(sql=query_meta))

    # Skiller ut numeriske variabler
    filter_numeric = (metadata_dynarev["felt_type"] == "DESIMAL") | (
        metadata_dynarev["felt_type"] == "NUMBER"
    )
    filter_numeric2 = metadata_dynarev.loc[filter_numeric, "felt_id"]
    filter_numeric3 = pd.Series.tolist(filter_numeric2)
    filter_numeric4 = ",".join(map("'{}'".format, filter_numeric3))

    # Skiller ut alle ikke-numeriske variabler
    filter_char = (metadata_dynarev["felt_type"] != "DESIMAL") & (
        metadata_dynarev["felt_type"] != "NUMBER"
    )
    filter_char2 = metadata_dynarev.loc[filter_char, "felt_id"]
    filter_char3 = pd.Series.tolist(filter_char2)
    filter_char4 = ",".join(map("'{}'".format, filter_char3))

    # SQL for å hente Dynarev skjema-data for numeriske variabler
    query_numeric = f"""
        SELECT *
            FROM
            (
                SELECT
                    DELREG_NR,
                    ENHETS_TYPE,
                    ENHETS_ID,
                    LOPENR,
                    FELT_ID,
                    TO_NUMBER(FELT_VERDI,
                            '999999999D99999999999999999999999999999999999999999',
                            'NLS_NUMERIC_CHARACTERS='',.''') as FELT_VERDI,
                    RAD_NR
                FROM DYNAREV.VW_SKJEMA_DATA
                WHERE
                    skjema = '{skjema}'
                    AND enhets_type='BEDR'
                    AND rad_nr     =0
                    AND aktiv      =1
                    AND delreg_nr  IN  {delreg_nr}
                    AND felt_id   IN ({filter_numeric4})
            )
            PIVOT(
                SUM(TO_NUMBER(FELT_VERDI))
                FOR (FELT_ID) IN ({filter_numeric4})
                 )
            """

    # SQL for å hente ut Dynarev skjema-data for karakter-variabler
    query_char = f"""
        SELECT *
            FROM
            (
                SELECT
                    DELREG_NR,
                    ENHETS_TYPE,
                    ENHETS_ID,
                    LOPENR,
                    FELT_ID,
                    FELT_VERDI,
                    RAD_NR
                FROM DYNAREV.VW_SKJEMA_DATA
                WHERE
                    skjema = '{skjema}'
                    AND enhets_type='BEDR'
                    AND rad_nr     =0
                    AND aktiv      =1
                    AND delreg_nr  IN  {delreg_nr}
                    AND felt_id   IN ({filter_char4})
            )
            PIVOT(
                MAX(FELT_VERDI)
                FOR (FELT_ID) IN ({filter_char4})
                 )
            """
    # Henter ut en data.frame for char og en for num og gjør en inner join på de to
    df1 = pd.DataFrame(conn.select(sql=query_numeric))
    df2 = pd.DataFrame(conn.select(sql=query_char))
    skjema_data = pd.merge(
        df1, df2, on=["enhets_id", "enhets_type", "delreg_nr", "lopenr", "rad_nr"]
    )

    # Fjerner fnutter fra noen kolonnenavn
    skjema_data.columns = skjema_data.columns.str.replace("'", "")

    # Henter inn dublettinfo
    # Henter ut èn av variabel-navnene for å gjøre en dublettsjekk uten å måtte pivotere først
    random_row = metadata_dynarev.iloc[0, 1]

    result = [skjema_data]  # Denne vil du jo ha ut uansett?

    # 1. Inkluder dubletter om dublettsjekk
    if dublettsjekk:
        # SQL for dublettsjekk
        query_dublett = f"""
        SELECT enhets_id, COUNT(*) AS antall_skjemaer
        FROM
             DYNAREV.VW_SKJEMA_DATA
        WHERE
            skjema       ='{skjema}'
            AND enhets_type='BEDR'
            AND rad_nr     =0
            AND aktiv      =1
            AND delreg_nr  = {delreg_nr}
            AND FELT_ID = '{random_row}'
        GROUP BY enhets_id
        HAVING COUNT(*) > 1
        """
        dublett = pd.DataFrame(conn.select(sql=query_dublett)

        # Sørger for at de som ikke har dubletter ikke får et tomt datasett tilbake.
        if not len(dublett):
            warnings.warn(
                "Så etter dubletter, men fant ingen, dublettdataframen er derfor tom"
            )
        else:
            print("Du har valgt å ta ut dublett-data")
        result += [dublett]

    # 2. Inkluder sfu data
    if sfu_cols:
        # Henter inn SFU-data
        # SQl for å hente ut SFU-data
        query_sfu = f"""
            SELECT b.*
            FROM dsbbase.dlr_enhet_i_delreg_skjema a, dsbbase.dlr_enhet_i_delreg b
            WHERE a.delreg_nr=b.delreg_nr
            AND a.ident_nr=b.ident_nr
            AND a.enhets_type=b.enhets_type
            AND b.prosedyre is null
            AND a.delreg_nr={delreg_nr}
            AND a.skjema_type='{skjema}'
            """
        sfu = pd.DataFrame(conn.select(sql=query_sfu)

        if sfu_cols == True:
            result += [sfu]
            print("Du har valgt å ta ut sfu-data, tar med alle kolonner.")
        else:
            result += [sfu[sfu_cols]]
            print("Du har valgt å ta ut sfu-data, tar med disse kolonnene:", *sfu_cols)

    conn.close()  # Bør lukkes før du returnerer ut av funksjonen, resultatene skal ligge i minnet alt

    if len(result) == 1:
        return result[0]  # Bare returner en dataframe
    else:
        return tuple(
            result
        )  # Ved å returnere en tuple, så kan man velge å bruke implisitt tuple unpacking


# +
# Dokumentasjon finner du her: https://git-adm.ssb.no/pages/~OBR/oyvinds_blog/master/browse/posts/2021-12-10-dynarevtilpy/
