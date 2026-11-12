"""This module contains functions to create a xml file that can be loaded in the KLASS UI.

It passes data through a pandas DataFrame from a list of codes and names, to an XML from the pandas dataframe.
"""

from collections.abc import Sequence
from datetime import datetime
from typing import Literal

import pandas as pd
from dateutil import parser

KlassType = Literal["codelist", "variant", "correspondence"]
Language = Literal["nb", "nn", "en"]

CODE_COL = "kode"
SOURCE_CODE_COL = "kilde_kode"
BOKMAAL_NAME_COL = "navn_bokmål"
NYNORSK_NAME_COL = "navn_nynorsk"
ENGLISH_NAME_COL = "navn_engelsk"

LANGUAGE_SUFFIX_MAP: dict[Language, str] = {
    "nb": "bokmål",
    "nn": "nynorsk",
    "en": "engelsk",
}

# Order defines XML element order.
CODELIST_PARAM_COLS = {
    "codes": CODE_COL,
    "parent": "forelder",
    "names_bokmaal": BOKMAAL_NAME_COL,
    "names_nynorsk": NYNORSK_NAME_COL,
    "names_engelsk": ENGLISH_NAME_COL,
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
    "codes": CODE_COL,
    "names_bokmaal": BOKMAAL_NAME_COL,
    "names_nynorsk": NYNORSK_NAME_COL,
    "names_engelsk": ENGLISH_NAME_COL,
    "source_codes": SOURCE_CODE_COL,
    "parent": "forelder",
}

CORRESPONDENCE_PARAM_COLS = {
    "source_codes": SOURCE_CODE_COL,
    "source_titles": "kilde_tittel",
    "target_codes": "mål_kode",
    "target_titles": "mål_tittel",
}

PARAM_COLS_BY_TYPE: dict[KlassType, dict[str, str]] = {
    "codelist": CODELIST_PARAM_COLS,
    "variant": VARIANT_PARAM_COLS,
    "correspondence": CORRESPONDENCE_PARAM_COLS,
}

KLASS_PYTHON_COLS = {
    "code",
    "parentcode",
    "level",
    "levelname",
    "name",
    "shortname",
    "presentationname",
    "validfrom",
    "validto",
    "validfrominrequestedrange",
    "validtoinrequestedrange",
    "notes",
    "sourcecode",
    "sourcename",
    "sourceshortname",
    "targetcode",
    "targetname",
    "targetshortname",
}

# KLASS XML namespace identifiers. `http` is intentional and must not be changed.
CODELIST_NAMESPACE = "http://klass.ssb.no/version"  # NOSONAR
VARIANT_NAMESPACE = "http://klass.ssb.no/variant"  # NOSONAR
CORRESPONDENCE_NAMESPACE = "http://klass.ssb.no/correspondenceTable"  # NOSONAR


def get_klass_xml_column_mapping(
    klass_type: KlassType = "codelist",
    language: Language = "nb",
) -> dict[str, str]:
    """Return a column mapping from ssb-klass-python names to KLASS XML names.

    Args:
        klass_type: The type of KLASS XML ('codelist', 'variant', or 'correspondence').
        language: Language code ('nb', 'nn', or 'en') used to map language-specific
            columns like 'name', 'shortName', and 'notes'.

    Returns:
        dict[str, str]: Mapping from standard ssb-klass-python column names to KLASS XML column names.

    Raises:
        ValueError: If klass_type or language is not supported.
    """
    if language not in LANGUAGE_SUFFIX_MAP:
        raise ValueError(
            f"Unsupported language: '{language}'. Expected one of {list(LANGUAGE_SUFFIX_MAP)}."
        )
    if klass_type not in PARAM_COLS_BY_TYPE:
        raise ValueError(
            f"Unsupported klass_type: '{klass_type}'. Expected one of {list(PARAM_COLS_BY_TYPE)}."
        )

    lang_suffix = LANGUAGE_SUFFIX_MAP[language]

    if klass_type == "codelist":
        return {
            "code": CODE_COL,
            "parentCode": "forelder",
            "name": f"navn_{lang_suffix}",
            "shortName": f"kortnavn_{lang_suffix}",
            "notes": f"noter_{lang_suffix}",
            "validFrom": "gyldig_fra",
            "validTo": "gyldig_til",
        }
    if klass_type == "variant":
        return {
            "code": CODE_COL,
            "name": f"navn_{lang_suffix}",
            "sourceCode": SOURCE_CODE_COL,
            "parentCode": "forelder",
        }
    return {
        "sourceCode": SOURCE_CODE_COL,
        "sourceName": "kilde_tittel",
        "targetCode": "mål_kode",
        "targetName": "mål_tittel",
    }


def rename_klass_columns(
    df: pd.DataFrame,
    klass_type: KlassType = "codelist",
    language: Language = "nb",
    drop_extra_cols: bool = True,
    format_date_cols: bool = True,
) -> pd.DataFrame:
    """Rename standard columns from ssb-klass-python to the expected KLASS XML format.

    Args:
        df: DataFrame retrieved from ssb-klass-python (e.g. KlassCodes.data,
            KlassVersion.data, KlassVariant.data, or KlassCorrespondence.data).
        klass_type: The type of KLASS XML ('codelist', 'variant', or 'correspondence').
        language: Language code ('nb', 'nn', or 'en') for name, shortName, and notes columns.
        drop_extra_cols: Whether to drop columns that are not part of the KLASS XML format
            (such as 'level', 'levelName', or 'presentationName').
        format_date_cols: Whether to format 'gyldig_fra' and 'gyldig_til' to 'dd.MM.yyyy'.

    Returns:
        pd.DataFrame: A copy of the DataFrame with KLASS XML column names.
    """
    col_mapping = get_klass_xml_column_mapping(klass_type=klass_type, language=language)
    lower_mapping = {k.lower(): v for k, v in col_mapping.items()}

    rename_dict = {
        col: lower_mapping[col.lower()]
        for col in df.columns
        if col.lower() in lower_mapping
    }
    renamed_df = df.rename(columns=rename_dict).copy()

    if drop_extra_cols:
        expected_cols = set(PARAM_COLS_BY_TYPE[klass_type].values())
        keep_cols = [col for col in renamed_df.columns if col in expected_cols]
        renamed_df = renamed_df[keep_cols]

    if format_date_cols and klass_type == "codelist":
        for date_col in ("gyldig_fra", "gyldig_til"):
            if date_col in renamed_df.columns:
                renamed_df[date_col] = format_dates(renamed_df[date_col].tolist())

    return renamed_df


def _prepare_klass_dataframe(
    df: pd.DataFrame,
    param_cols: dict[str, str],
    klass_type: KlassType = "codelist",
) -> pd.DataFrame:
    """Return a DataFrame with the expected KLASS columns in the correct order."""
    df = df.rename(columns=str.lower)
    expected_cols = list(param_cols.values())

    for col in df.columns:
        if col not in expected_cols:
            if col in KLASS_PYTHON_COLS:
                raise ValueError(
                    f"Column name: {col} is not among the expected column names: "
                    f"{expected_cols}. "
                    "It looks like the DataFrame uses column names from 'ssb-klass-python'. "
                    f"Use 'rename_klass_columns(df, klass_type=\"{klass_type}\")' "
                    "to prepare the DataFrame before converting to XML."
                )
            raise ValueError(
                f"Column name: {col} is not among the expected column names: "
                f"{expected_cols}"
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
    has_code = _has_value(df[CODE_COL])
    has_source_code = _has_value(df[SOURCE_CODE_COL])
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


def _validate_codelist_dates(df: pd.DataFrame) -> None:
    """Validate that codelist date columns use the dd.MM.yyyy format expected by KLASS."""
    for date_col in ("gyldig_fra", "gyldig_til"):
        for val in df[date_col]:
            if pd.isna(val) or not str(val).strip():
                continue
            date_str = str(val).strip()
            try:
                datetime.strptime(date_str, "%d.%m.%Y")
            except ValueError as e:
                raise ValueError(
                    f"Invalid date format in column '{date_col}': '{date_str}'. "
                    "KLASS XML requires dates in 'dd.MM.yyyy' format (e.g. '01.01.2025'). "
                    "Use 'format_dates()' or 'rename_klass_columns()' to format dates."
                ) from e


def format_dates(dates: Sequence[str | None] | None) -> list[str]:
    """Ensure all dates are in dd.MM.yyyy format."""
    if dates is None or len(dates) == 0:
        return []
    formatted: list[str] = []
    for date in dates:
        if pd.isna(date) or not str(date).strip():
            formatted.append("")
        else:
            date_str = str(date).strip()
            try:
                try:
                    parsed_date = datetime.fromisoformat(date_str)
                except ValueError:
                    parsed_date = parser.parse(date_str, dayfirst=True)
                formatted.append(parsed_date.strftime("%d.%m.%Y"))
            except Exception as e:
                raise ValueError(f"Invalid date format: {date}") from e
    return formatted


def klass_dataframe_to_xml_codelist(df: pd.DataFrame, path: str) -> pd.DataFrame:
    """Write a klass-xml for a codelist down to a path.

    Expected columns are: 'kode', 'forelder', 'navn_bokmål', 'navn_nynorsk',
    'navn_engelsk', 'kortnavn_bokmål', 'kortnavn_nynorsk', 'kortnavn_engelsk',
    'noter_bokmål', 'noter_nynorsk', 'noter_engelsk', 'gyldig_fra', 'gyldig_til'.
    If your DataFrame comes from ssb-klass-python, use `rename_klass_columns(df, klass_type="codelist")` first.

    Args:
        df: The klass-dataframe with the correct columns, with the correct column names.
        path: The path to write the XML to.

    Returns:
        pd.DataFrame: The dataframe sent in, but all the columns inserted in the correct order and with correct naming.

    Raises:
        ValueError: If a column sent in is not among the known column names or dates are not in 'dd.MM.yyyy' format.
    """  # noqa: DOC502
    output_df = _prepare_klass_dataframe(df, CODELIST_PARAM_COLS, klass_type="codelist")
    _validate_codelist_dates(output_df)

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

    Expected columns are: 'kode', 'navn_bokmål', 'navn_nynorsk', 'navn_engelsk',
    'kilde_kode', 'forelder'.
    If your DataFrame comes from ssb-klass-python, use `rename_klass_columns(df, klass_type="variant")` first.

    Args:
        df: The klass-dataframe with variant column names.
        path: The path to write the XML to.

    Returns:
        pd.DataFrame: The dataframe written to XML with all expected columns.

    Raises:
        ValueError: If a column is unknown or the variant structure is invalid.
    """  # noqa: DOC502
    output_df = _prepare_klass_dataframe(df, VARIANT_PARAM_COLS, klass_type="variant")
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

    Expected columns are: 'kilde_kode', 'kilde_tittel', 'mål_kode', 'mål_tittel'.
    If your DataFrame comes from ssb-klass-python, use `rename_klass_columns(df, klass_type="correspondence")` first.

    Args:
        df: The klass-dataframe with correspondence column names.
        path: The path to write the XML to.

    Returns:
        pd.DataFrame: The dataframe written to XML with all expected columns.

    Raises:
        ValueError: If a column sent in is not among the known column names.
    """  # noqa: DOC502
    output_df = _prepare_klass_dataframe(
        df,
        CORRESPONDENCE_PARAM_COLS,
        klass_type="correspondence",
    )

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
