"""This module contains functions to create a xml file that can be loaded in the KLASS UI.

It passes data through a pandas DataFrame from a list of codes and names, to an XML from the pandas dataframe.
"""

import pandas as pd
from dateutil import parser


PARAM_COLS = {  # Order is important?
    "codes": "kode",
    "parent": "forelder",
    "names_bokmaal": "navn_bokmål",
    "names_nynorsk": "navn_nynorsk",
    "names_engelsk": "navn_engelsk",
    "shortname_bokmaal": "kortnavn_bokmål",
    "shortname_nynorsk": "kortnavn_nynorsk",
    "shortname_engelsk": "kortnavn_engelsk",
    "notes_bokmaal": "noter_bokmål",
    "notes_nynorsk": "noter_nynorsk",
    "notes_engelsk": "noter_engelsk",
    "valid_from": "gyldig_fra",
    "valid_to": "gyldig_til",
}


def format_dates(dates: list[str | None] | None) -> list[str]:
    """Ensure all dates are in dd.MM.yyyy format."""
    if not dates:
        return []
    formatted: list[str] = []
    for date in dates:
        if not date:
            formatted.append("")
        else:
            try:
                parsed_date = parser.parse(date, dayfirst=True)
                formatted.append(parsed_date.strftime("%d.%m.%Y"))
            except Exception as e:
                raise ValueError(f"Invalid date format: {date}") from e
    return formatted


def klass_dataframe_to_xml_codelist(df: pd.DataFrame, path: str) -> pd.DataFrame:
    """Write a klass-xml for a codelist down to a path.

    Args:
        df: The klass-dataframe with the correct columns, with the correct column names.
        path: The path to write the XML to.

    Returns:
        pd.DataFrame: The dataframe sent in, but all the columns inserted in the correct order and with correct naming.

    Raises:
        ValueError: If a column sent in is not among the known column names.
    """
    # Lower column names
    df = df.rename(columns=str.lower)

    # Check the user has not used column names we dont expect
    for col in df.columns:
        if col not in PARAM_COLS.values():
            raise ValueError(
                f"Column name: {col} is not among the expected column names: {PARAM_COLS.values()}"
            )

    # Check that the columns are in the correct order and exist
    filled_dict = {}
    for eng, nor in PARAM_COLS.items():
        if nor in df.columns:
            filled_dict[nor] = df[nor]
        elif eng in df.columns:
            filled_dict[nor] = df[eng]
        else:
            filled_dict[nor] = pd.Series([None] * len(df))

    output_df = pd.DataFrame(filled_dict)

    # Replace all nones with empty strings?
    for col in output_df.select_dtypes(["object", "string"]).columns:
        output_df[col] = output_df[col].fillna("")

    output_df.to_xml(
        path,
        root_name="versjon",
        row_name="element",
        namespaces={
            "ns1": "https://klass.ssb.no/version",
        },
        prefix="ns1",
    )
    return output_df


def make_klass_xml_codelist(
    path: str,
    codes: list[str | int],
    names_bokmaal: list[str | None] | None = None,
    names_nynorsk: list[str | None] | None = None,
    names_engelsk: list[str | None] | None = None,
    parent: list[str | None] | None = None,
    shortname_bokmaal: list[str | None] | None = None,
    shortname_nynorsk: list[str | None] | None = None,
    shortname_engelsk: list[str | None] | None = None,
    notes_bokmaal: list[str | None] | None = None,
    notes_nynorsk: list[str | None] | None = None,
    notes_engelsk: list[str | None] | None = None,
    valid_from: list[str | None] | None = None,
    valid_to: list[str | None] | None = None,
) -> pd.DataFrame:
    """Make a klass xml file and pandas Dataframe from a list of codes and names.

    This XML can be loaded into the old KLASS UI under version -> import to the top right.

    Args:
        path: Path to save the xml file.
        codes: List of codes.
        names_bokmaal: List of names in Bokmål.
        names_nynorsk: List of names in Nynorsk.
        names_engelsk: List of names in English.
        parent: List of parent codes that applies to the codes (for hierarchical codelists).
        shortname_bokmaal: Shortname in Bokmål.
        shortname_nynorsk: Shortname in Nynorsk.
        shortname_engelsk: Shortname in English.
        notes_bokmaal: Notes in Bokmål.
        notes_nynorsk: Notes in Nynorsk.
        notes_engelsk: Notes in English.
        valid_from: Valid from date.
        valid_to: Valid to date.

    Returns:
        pd.DataFrame: Dataframe with columns for codes and names.

    Raises:
        ValueError: If the length of the lists sent in are not the same
    """
    if names_bokmaal is None and names_nynorsk is None:
        raise ValueError("Must have content in names_bokmaal or names_nynorsk")

    # Normalize date formats to dd.MM.yyyy which is what KLASS prefers
    valid_from_str = format_dates(valid_from)
    valid_to_str = format_dates(valid_to)

    cols_names = {
        "codes": codes,
        "names_bokmaal": names_bokmaal,
        "names_nynorsk": names_nynorsk,
        "names_engelsk": names_engelsk,
        "parent": parent,
        "shortname_bokmaal": shortname_bokmaal,
        "shortname_nynorsk": shortname_nynorsk,
        "shortname_engelsk": shortname_engelsk,
        "notes_bokmaal": notes_bokmaal,
        "notes_nynorsk": notes_nynorsk,
        "notes_engelsk": notes_engelsk,
        "valid_from": valid_from_str,
        "valid_to": valid_to_str,
    }
    for name in cols_names.values():
        if name and len(codes) != len(name):
            raise ValueError(
                "Length of the entered names must match the length of codes."
            )
    filled_cols = {PARAM_COLS[k]: v for k, v in cols_names.items() if v}
    data = {col: [None] * len(codes) for col in PARAM_COLS.values()} | filled_cols
    df = pd.DataFrame({name: data for name, data in data.items()})
    return klass_dataframe_to_xml_codelist(df, path)
