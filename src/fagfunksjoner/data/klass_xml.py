"""This module contains functions to create a xml file that can be loaded in the KLASS UI.

It passes data trhough a pandas DataFrame from a list of codes and names, to an XML from the pandas dataframe.
"""

import pandas as pd


def make_klass_df_codelist(
    codes: list[str | int],
    names_bokmaal: list[str] | None = None,
    names_nynorsk: list[str] | None = None,
    names_engelsk: list[str] | None = None,
) -> pd.DataFrame:
    """Make a pandas Dataframe from lists of codes and names.

    Args:
        codes: List of codes.
        names_bokmaal: List of names in Bokmål.
        names_nynorsk: List of names in Nynorsk.
        names_engelsk: List of names in English.

    Returns:
        pd.DataFrame: Dataframe with columns for codes and names.

    Raises:
        ValueError: If names_bokmaal and names_nynorsk are both None, or if the length of
            codes and names do not match.
    """
    if names_bokmaal is None and names_nynorsk is None:
        raise ValueError("Must have content in names_bokmaal or names_nynorsk")
    for name in [names_bokmaal, names_nynorsk, names_engelsk]:
        if name and len(codes) != len(name):
            raise ValueError(
                "Length of the entered names must match the length of codes."
            )

    cols = [
        "kode",
        "forelder",
        "navn_bokmål",
        "navn_nynorsk",
        "navn_engelsk",
        "kortnavn_bokmål",
        "kortnavn_nynorsk",
        "kortnavn_engelsk",
        "noter_bokmål",
        "noter_nynorsk",
        "noter_engelsk",
        "gyldig_fra",
        "gyldig_til",
    ]

    data = {col: [None] * len(codes) for col in cols} | {
        "kode": codes,
        "navn_bokmål": names_bokmaal,
        "navn_nynorsk": names_nynorsk,
        "navn_engelsk": names_engelsk,
    }

    return pd.DataFrame({name: data for name, data in data.items()})


def make_klass_xml_codelist(
    path: str,
    codes: list[str | int],
    names_bokmaal: list[str] | None = None,
    names_nynorsk: list[str] | None = None,
    names_engelsk: list[str] | None = None,
) -> pd.DataFrame:
    """Make a klass xml file and pandas Dataframe from a list of codes and names.

    This XML can be loaded into the old KLASS UI under version -> import to the top right.

    Args:
        path (str): Path to save the xml file.
        codes (list[str|int]): List of codes.
        names_bokmaal (list[str] | None): List of names in Bokmål.
        names_nynorsk (list[str] | None): List of names in Nynorsk.
        names_engelsk (list[str] | None): List of names in English.

    Returns:
        pd.DataFrame: Dataframe with columns for codes and names.
    """
    df = make_klass_df_codelist(
        codes=codes,
        names_bokmaal=names_bokmaal,
        names_nynorsk=names_nynorsk,
        names_engelsk=names_engelsk,
    )
    df.to_xml(
        path,
        root_name="versjon",
        row_name="element",
        namespaces={
            "ns1": "http://klass.ssb.no/version",
        },
        prefix="ns1",
    )
    return df
