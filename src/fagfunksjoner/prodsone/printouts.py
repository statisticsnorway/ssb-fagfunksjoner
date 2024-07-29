import pandas as pd

from fagfunksjoner.fagfunksjoner_logger import logger

def prosent_av_kolonne_na(df: pd.DataFrame, col: str) -> None:
    logger.info(len(df[df[col].isna()]), "av", len(df), "har tomme verdier i kolonnen", col)
    logger.info("Det er", round(len(df[df[col].isna()]) / len(df) * 100, 2), "%")


def antall_rader_kobler(df1: pd.DataFrame, col1: str, col2: pd.Series) -> None:
    logger.info(
        "Av",
        len(df1),
        "rader i hoveddataframen, kobler\n  ",
        len(df1[df1[col1].isin(col2)]),
        "mot den andre serien. \nDet er",
        round(len(df1[df1[col1].isin(col2)]) / len(df1) * 100, 2),
        "%.",
    )


def sammenlign_kobling(
    df1: pd.DataFrame, col1: str, col2: str, df2: pd.DataFrame, col3: str, col4: str
) -> None:
    logger.info(
        "Av",
        len(df1),
        f"rader i den første dataframen, kobler \n{col1} på",
        len(df1[df1[col1].isin(df2[col3])]),
        f"rader i {col3}: {round(len(df1[df1[col1].isin(df2[col3])])/len(df1)*100,2)}%, og \n{col2} på",
        len(df1[col2].isin(df2[col4])),
        f"rader i {col4}: {round(len(df1[df1[col2].isin(df2[col4])])/len(df1)*100,2)}%.",
    )
