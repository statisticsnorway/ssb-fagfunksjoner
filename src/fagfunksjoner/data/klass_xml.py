"""This module contains functions to create a xml file that can be loaded in the KLASS UI.

It passes data through a pandas DataFrame from a list of codes and names, to an XML from the pandas dataframe.
"""

import pandas as pd
from dateutil import parser

CODELIST_PARAM_COLS = {  # Order is important?
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

VARIANT_PARAM_COLS = {
    "codes": "kode",
    "names_bokmaal": "navn_bokmål",
    "names_nynorsk": "navn_nynorsk",
    "names_engelsk": "navn_engelsk",
    "source_codes": "kilde_kode",
    "parent": "forelder",
}

CORRESPONDENCE_PARAM_COLS = {
    "source_codes": "kilde_kode",
    "source_titles": "kilde_tittel",
    "target_codes": "mål_kode",
    "target_titles": "mål_tittel",
}

CODELIST_NAMESPACE = "https://klass.ssb.no/version"
VARIANT_NAMESPACE = "http://klass.ssb.no/variant"
CORRESPONDENCE_NAMESPACE = "http://klass.ssb.no/correspondenceTable"


def _prepare_klass_dataframe(
    df: pd.DataFrame,
    param_cols: dict[str, str],
) -> pd.DataFrame:
    """Return a DataFrame with the expected KLASS columns in the correct order."""
    df = df.rename(columns=str.lower)
    expected_cols = list(param_cols.values())

    for col in df.columns:
        if col not in expected_cols:
            raise ValueError(
                f"Column name: {col} is not among the expected column names: "
                f"{param_cols.values()}"
            )

    output_df = pd.DataFrame(
        {
            col: (
                df[col]
                if col in df.columns
                else pd.Series([None] * len(df), index=df.index)
            )
            for col in expected_cols
        }
    )

    for col in output_df.select_dtypes(["object", "string"]).columns:
        output_df[col] = output_df[col].fillna("")

    return output_df


def _has_value(series: pd.Series) -> pd.Series:
    """Return whether values contain non-whitespace content."""
    return series.astype("string").fillna("").str.strip().ne("")


def _validate_variant_dataframe(df: pd.DataFrame) -> None:
    """Validate the structural requirements KLASS applies to variant elements."""
    has_code = _has_value(df["kode"])
    has_source_code = _has_value(df["kilde_kode"])
    has_content = pd.concat([_has_value(df[col]) for col in df.columns], axis=1).any(
        axis=1
    )

    missing_code = has_content & ~(has_code | has_source_code)
    if missing_code.any():
        rows = df.index[missing_code].tolist()
        raise ValueError(
            "Variant elements must have content in 'kode' or 'kilde_kode'. "
            f"Invalid rows: {rows}"
        )

    name_cols = ["navn_bokmål", "navn_nynorsk", "navn_engelsk"]
    has_name = pd.concat([_has_value(df[col]) for col in name_cols], axis=1).any(axis=1)
    is_reference = has_source_code & ~has_code
    missing_name = has_content & ~is_reference & ~has_name

    if missing_name.any():
        rows = df.index[missing_name].tolist()
        raise ValueError(
            "Variant elements with 'kode' must have a name in at least one language. "
            f"Invalid rows: {rows}"
        )


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
    """  # noqa: DOC502
    output_df = _prepare_klass_dataframe(df, CODELIST_PARAM_COLS)

    output_df.to_xml(
        path,
        root_name="versjon",
        row_name="element",
        namespaces={
            "ns1": CODELIST_NAMESPACE,
        },
        prefix="ns1",
    )
    return output_df


def klass_dataframe_to_xml_variant(df: pd.DataFrame, path: str) -> pd.DataFrame:
    """Write a klass-xml for a classification variant down to a path.

    Args:
        df: The klass-dataframe with variant column names.
        path: The path to write the XML to.

    Returns:
        pd.DataFrame: The dataframe written to XML with all expected columns.

    Raises:
        ValueError: If a column is unknown or the variant structure is invalid.
    """  # noqa: DOC502
    output_df = _prepare_klass_dataframe(df, VARIANT_PARAM_COLS)
    _validate_variant_dataframe(output_df)

    output_df.to_xml(
        path,
        index=False,
        root_name="variant",
        row_name="element",
        namespaces={"": VARIANT_NAMESPACE},
        xml_declaration=True,
        encoding="UTF-8",
    )
    return output_df


def klass_dataframe_to_xml_correspondence(
    df: pd.DataFrame,
    path: str,
) -> pd.DataFrame:
    """Write a klass-xml for a correspondence table down to a path.

    Args:
        df: The klass-dataframe with correspondence column names.
        path: The path to write the XML to.

    Returns:
        pd.DataFrame: The dataframe written to XML with all expected columns.

    Raises:
        ValueError: If a column sent in is not among the known column names.
    """  # noqa: DOC502
    output_df = _prepare_klass_dataframe(df, CORRESPONDENCE_PARAM_COLS)

    output_df.to_xml(
        path,
        index=False,
        root_name="Korrespondansetabell",
        row_name="Korrespondanse",
        namespaces={"": CORRESPONDENCE_NAMESPACE},
        xml_declaration=True,
        encoding="UTF-8",
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
    filled_cols = {CODELIST_PARAM_COLS[k]: v for k, v in cols_names.items() if v}
    data = {
        col: [None] * len(codes) for col in CODELIST_PARAM_COLS.values()
    } | filled_cols
    df = pd.DataFrame(data)
    return klass_dataframe_to_xml_codelist(df, path)
