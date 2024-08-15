# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Import file description from Datadok and use that to import archive file
# We use the path in Datadok and the name of the archive file as arguments to the function

import gc


# %%
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests

from fagfunksjoner.data.dicts import get_key_by_value
from fagfunksjoner.fagfunksjoner_logger import logger
from fagfunksjoner.prodsone.check_env import linux_shortcuts


# %% [markdown]
# ## Hente fra api til Datadok
# Vi har et api til datadok og det returnerer filbeskrivelse som en html-fil. Det kan f.eks. kalles slik
#
# `curl -i 'ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionByPath?path=$ENERGI/er_eb/arkiv/grunnlag/g1990'
# `
#
# Den interne metadataportalen http://www.byranettet.ssb.no/metadata/ har også alle filbeskrivelsene og filvariablene.


# %%
def is_valid_url(url: str) -> bool:
    """Check if the provided URL is valid.

    Args:
        url: The URL to validate.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


# %%
@dataclass
class ContextVariable:
    """Class representing a context variable."""

    context_id: str
    title: str
    description: str
    datatype: str
    length: int
    start_position: int
    precision: int | None
    division: str


@dataclass
class CodeList:
    """Class representing a code list."""

    context_id: str
    codelist_title: str
    codelist_description: str
    code_value: str
    code_text: str


@dataclass
class Metadata:
    """Class representing metadata which includes context variables and code lists."""

    context_variables: list[ContextVariable]
    codelists: list[CodeList]


@dataclass
class ArchiveData:
    """Class representing the archive data along with its metadata and code lists."""

    df: pd.DataFrame
    metadata_df: pd.DataFrame
    codelist_df: pd.DataFrame
    codelist_dict: dict[str, dict[str, str]]
    names: list[str]
    widths: list[int]
    datatypes: dict[str, str]


# %%
def extract_context_variables(root: ET.Element) -> list[ContextVariable]:
    """Extracts context variables from the XML root element and returns a list of ContextVariable objects.

    Args:
        root: The root element of the XML tree to parse.

    Returns:
        list: A list of ContextVariable objects.

    Raises:
        ValueError: Missing information in the XML.
    """
    data = []
    contact_info = root.find("{http://www.ssb.no/ns/meta}ContactInformation")
    if contact_info is None:
        raise ValueError("ContactInformation not found in the XML")
    division = contact_info.find("{http://www.ssb.no/ns/meta/common}Division")
    if division is None or division.text is None:
        raise ValueError("Division not found in the XML or has no text")

    division_text = division.text

    for context_var in root.findall("{http://www.ssb.no/ns/meta}ContextVariable"):
        context_id = context_var.get("id")
        title_elem = context_var.find("{http://www.ssb.no/ns/meta}Title")
        description_elem = context_var.find("{http://www.ssb.no/ns/meta}Description")
        properties = context_var.find("{http://www.ssb.no/ns/meta}Properties")

        if title_elem is None or title_elem.text is None:
            raise ValueError("Title element missing or has no text")
        if description_elem is None or description_elem.text is None:
            raise ValueError("Description element missing or has no text")
        if properties is None:
            raise ValueError("Properties element missing")

        datatype_elem = properties.find("{http://www.ssb.no/ns/meta}Datatype")
        length_elem = properties.find("{http://www.ssb.no/ns/meta}Length")
        start_position_elem = properties.find(
            "{http://www.ssb.no/ns/meta}StartPosition"
        )
        precision_elem = properties.find("{http://www.ssb.no/ns/meta}Precision")

        if datatype_elem is None or datatype_elem.text is None:
            raise ValueError("Datatype element missing or has no text")
        if length_elem is None or length_elem.text is None:
            raise ValueError("Length element missing or has no text")
        if start_position_elem is None or start_position_elem.text is None:
            raise ValueError("StartPosition element missing or has no text")

        precision = (
            int(precision_elem.text)
            if precision_elem is not None and precision_elem.text is not None
            else None
        )

        data.append(
            ContextVariable(
                context_id if context_id is not None else "",
                title_elem.text,
                description_elem.text,
                datatype_elem.text,
                int(length_elem.text),
                int(start_position_elem.text),
                precision,
                division_text,
            )
        )
    return data


def extract_codelist(root: ET.Element) -> list[CodeList]:
    """Extracts code lists from the XML root element and returns a list of CodeList objects.

    Args:
        root: The root element of the XML tree to parse.

    Returns:
        list[Codelist]: A list of CodeList objects.
    """
    codelist_data = []
    for context_var in root.findall("{http://www.ssb.no/ns/meta}ContextVariable"):
        codelist = context_var.find("{http://www.ssb.no/ns/meta/codelist}Codelist")
        if codelist is not None:
            codelist_meta = codelist.find(
                "{http://www.ssb.no/ns/meta/codelist}CodelistMeta"
            )
            if codelist_meta is None:
                continue

            codelist_title_elem = codelist_meta.find(
                "{http://www.ssb.no/ns/meta/codelist}Title"
            )
            codelist_description_elem = codelist_meta.find(
                "{http://www.ssb.no/ns/meta/codelist}Description"
            )

            if codelist_title_elem is None or codelist_title_elem.text is None:
                continue
            if (
                codelist_description_elem is None
                or codelist_description_elem.text is None
            ):
                continue

            codelist_title = codelist_title_elem.text
            codelist_description = codelist_description_elem.text

            codes = codelist.find("{http://www.ssb.no/ns/meta/codelist}Codes")
            if codes is not None:
                for code in codes.findall("{http://www.ssb.no/ns/meta/codelist}Code"):
                    code_value_elem = code.find(
                        "{http://www.ssb.no/ns/meta/codelist}CodeValue"
                    )
                    code_text_elem = code.find(
                        "{http://www.ssb.no/ns/meta/codelist}CodeText"
                    )

                    if code_value_elem is None or code_value_elem.text is None:
                        continue
                    if code_text_elem is None or code_text_elem.text is None:
                        continue

                    # Ensuring type for mypy
                    context_id: str = context_var.get("id", "")

                    codelist_data.append(
                        CodeList(
                            context_id,
                            codelist_title,
                            codelist_description,
                            code_value_elem.text,
                            code_text_elem.text,
                        )
                    )
    return codelist_data


def codelist_to_df(codelist: list[CodeList]) -> pd.DataFrame:
    """Converts a list of CodeList objects to a DataFrame.

    Args:
        codelist (list[CodeList]): A list of CodeList objects.

    Returns:
        pd.DataFrame: A DataFrame containing the code list information.
    """
    return pd.DataFrame([vars(cl) for cl in codelist])


def metadata_to_df(context_variables: list[ContextVariable]) -> pd.DataFrame:
    """Converts a list of ContextVariable objects to a DataFrame.

    Args:
        context_variables: A list of ContextVariable objects.

    Returns:
        pd.DataFrame: A DataFrame containing the context variable information.
    """
    df = pd.DataFrame([vars(cv) for cv in context_variables])
    df["type"] = (
        df["datatype"]
        .str.replace("Tekst", "string[pyarrow]", regex=False)
        .str.replace("Heltall", "Int64", regex=False)
        .str.replace("Desimaltall", "string[pyarrow]", regex=False)
        .str.replace("Desim. (K)", "string[pyarrow]", regex=False)
        .str.replace("Desim. (P)", "string[pyarrow]", regex=False)
        .str.replace("Dato1", "string[pyarrow]", regex=False)
        .str.replace("Dato2", "string[pyarrow]", regex=False)
    )
    return df


def codelist_to_dict(codelist_df: pd.DataFrame) -> dict[str, dict[str, str]]:
    """Converts a DataFrame containing code lists to a dictionary.

    Args:
        codelist_df: DataFrame containing the code list information.

    Returns:
        dict[str, CodeList]: A dictionary mapping code list titles to dictionaries of code values and texts.
    """
    if codelist_df.empty:
        logger.info("NOTE: Filbeskrivelsen har ingen kodelister")
        return {}

    col_dict: dict[str, dict[str, str]] = {
        str(col): dict(zip(sub_df["code_value"], sub_df["code_text"], strict=False))
        for col, sub_df in codelist_df.groupby("codelist_title")
    }

    return col_dict


def date_parser(
    date_str: str, date_format: str
) -> datetime | pd._libs.tslibs.nattype.NaTType:
    """Parses a date string into a datetime object based on the provided format.

    Args:
        date_str: The date string to be parsed.
        date_format: The format in which the date string is.

    Returns:
        datetime: The parsed datetime object, or pd.NaT if parsing fails.
    """
    try:
        return datetime.strptime(date_str, date_format)
    except ValueError:
        return pd.NaT


def date_formats(metadata_df: pd.DataFrame) -> dict[str, str]:
    """Creates a dictionary of date conversion functions based on the metadata DataFrame.

    Args:
        metadata_df: DataFrame containing metadata.

    Returns:
        dict[str, str]: A dictionary mapping column titles to date conversion formats.

    Raises:
        ValueError: On unrecognized dateformats.
    """
    date_formats = {
        ("Dato1", 8): "%Y%m%d",
        ("Dato1", 6): "%y%m%d",
        ("Dato2", 8): "%d%m%Y",
        ("Dato2", 6): "%d%m%y",
    }

    date_metas_mask = (metadata_df["length"].astype("Int64").isin([6, 8])) & (
        metadata_df["datatype"].isin(["Dato1", "Dato2"])
    )

    # If there are dateformats we dont know about, we want an error on that
    not_catched = metadata_df[~date_metas_mask]
    for _, row in not_catched.iterrows():
        if "dato" in row["datatype"].lower() or "date" in row["datatype"].lower():
            raise ValueError(f"Dataformatting for metadatarow not catched: {row}")

    date_metas = metadata_df[date_metas_mask]

    # If there are no date columns to convert, exit function
    if not len(date_metas):
        logger.warning("NOTE: Ingen datofelt funnet")
        return {}

    # Pick the formattings that are known
    formattings = {}
    for _, row in date_metas.iterrows():
        formatting = date_formats.get((row["datatype"], row["length"]), None)
        if formatting:
            formattings[row["title"]] = formatting
    return formattings


def extract_parameters(
    df: pd.DataFrame,
) -> tuple[list[str], list[int], dict[str, str], str]:
    """Extracts parameters from the metadata DataFrame for importing archive data.

    Args:
        df: A DataFrame containing metadata.

    Returns:
        tuple[list[str], list[int], dict[str, str], str]: Extracted parameters to input into archive import.
    """
    col_names = df["title"].tolist()
    col_lengths = df["length"].astype(int).tolist()
    datatype = dict(zip(df["title"], df["type"], strict=False))
    decimal = "," if "Desim. (K)" in df["datatype"].values else "."
    return col_names, col_lengths, datatype, decimal


def downcast_ints(
    df: pd.DataFrame, metadata_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Store ints as the lowest possible datatype that can contain the values.

    Args:
        df: The DataFrame containing archive data.
        metadata_df: The DataFrame containing metadata.

    Returns:
        pd.DataFrame: The modified archive DataFrame with downcast ints.
    """
    int_cols = metadata_df.loc[metadata_df["type"] == "Int64", "title"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], downcast="integer")
        # Correct metadata
        metadata_df.loc[metadata_df["title"] == col, "type"] = df[col].dtype.name
    return df, metadata_df


def convert_dates(
    df: pd.DataFrame, metadata_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Faster to convert columns vectorized after importing as string, instead of running every row through a lambda.

    Args:
        df: The DataFrame containing archive data.
        metadata_df: The DataFrame containing metadata.

    Returns:
        pd.DataFrame: The modified archive DataFrame with converted datetimecolumns.
    """
    formats = date_formats(metadata_df)
    for col, formatting in formats.items():
        df[col] = pd.to_datetime(df[col], format=formatting)
        # Correct datatypes in metadata
        metadata_df.loc[metadata_df["title"] == col, "type"] = "datetime64"

    return df, metadata_df


def handle_decimals(
    df: pd.DataFrame, metadata_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Adjusts the decimal values in the archive DataFrame based on the metadata or contained decimal sign.

    Args:
        df: The DataFrame containing archive data.
        metadata_df: The DataFrame containing metadata.

    Returns:
        pd.DataFrame: The modified archive DataFrame with adjusted decimal values.
    """
    desi_cols = (
        metadata_df["title"]
        .loc[metadata_df["datatype"].str.lower().str.contains("desim")]
        .tolist()
    )

    for col in desi_cols:
        # Look for comma as delimiter
        if df[col].str.contains(",", regex=False).any():
            df[col] = df[col].str.replace(",", ".").astype("Float64")
        # Look for punktum as delimiter
        elif df[col].str.contains(".", regex=False).any():  # "." is a special character in regex, making this fail if regex is used.
            df[col] = df[col].str.replace(",", ".").astype("Float64")
        # If no delimiter is found, use number of decimals from metadata
        else:
            num_desi = int(
                metadata_df.loc[metadata_df["title"] == col, "precision"].iloc[0]
            )
            divisor = 10**num_desi
            df[col] = df[col].astype("Float64").div(divisor)
        # Correct metadata
        metadata_df.loc[metadata_df["title"] == col, "type"] = "Float64"

    return df, metadata_df


def import_archive_data(
    archive_desc_xml: str, archive_file: str, **read_fwf_params: Any
) -> ArchiveData:
    """Imports archive data based on the given XML description and archive file.

    Args:
        archive_desc_xml: Path or URL to the XML file describing the archive.
        archive_file: Path to the archive file.
        read_fwf_params: Remaining parameters to pass to pd.read_fwf, dtype, widths, names and na_values is overwritten,
            so dont use those.

    Returns:
        ArchiveData: An ArchiveData object containing the imported data, metadata, and code lists.

    Raises:
        ParseError: If we cant parse the content on the datadok-api endpoint as XML.
        ValueError: If params are passed through read_fwf_params that we will overwrite with the import function.

    Example usage::

        archive_data = import_archive_data('path_to_xml.xml', 'path_to_archive_file.txt')
        print(archive_data.df)
    """
    if is_valid_url(archive_desc_xml):
        xml_file = requests.get(archive_desc_xml).text
    else:
        with open(archive_desc_xml) as file:
            xml_file = file.read()
    try:
        root = ET.fromstring(xml_file)
    except ET.ParseError as ParseError:
        logger.error(f"{archive_desc_xml} , {xml_file}")
        raise ParseError

    context_variables = extract_context_variables(root)
    codelists = extract_codelist(root)
    metadata = Metadata(context_variables, codelists)

    metadata_df = metadata_to_df(metadata.context_variables)
    codelist_df = codelist_to_df(metadata.codelists)
    codelist_dict = codelist_to_dict(codelist_df)
    names, widths, datatypes, _decimal = extract_parameters(metadata_df)

    # Default historical file encoding used at SSB
    if "encoding" not in read_fwf_params:
        read_fwf_params["encoding"] = "latin1"
    # Throw error if user passes in params we will overwrite
    overwrites = ["filepath_or_buffer", "dtype", "widths", "name", "na_values"]
    for param in overwrites:
        if param in read_fwf_params:
            err = f"You cannot pass {param} to pandas.fwf(), because we are overwriting it."
            raise ValueError(err)

    df = pd.read_fwf(
        archive_file,
        dtype=datatypes,
        widths=widths,
        names=names,
        na_values=".",
        **read_fwf_params,
    )
    df, metadata_df = convert_dates(df, metadata_df)
    df, metadata_df = handle_decimals(df, metadata_df)
    df, metadata_df = downcast_ints(df, metadata_df)
    # Corrected datatype
    datatypes = dict(zip(metadata_df["title"], metadata_df["type"], strict=False))
    gc.collect()
    return ArchiveData(
        df, metadata_df, codelist_df, codelist_dict, names, widths, datatypes
    )


def open_path_metapath_datadok(
    path: str, metapath: str, **read_fwf_params: Any
) -> ArchiveData:
    """If open_path_datadok doesnt work, specify the path on linux AND the path in Datadok.

    Args:
        path: Path to the archive file on linux.
        metapath: Path described in datadok.
        read_fwf_params: Remaining parameters to pass to pd.read_fwf, dtype, widths, names and na_values is overwritten,
            so dont use those.

    Returns:
        ArchiveData: An ArchiveData object containing the imported data, metadata, and code lists.
    """
    return import_archive_data(
        archive_desc_xml=f"http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionByPath?path={metapath}",
        archive_file=path,
        **read_fwf_params,
    )


def open_path_datadok(path: str, **read_fwf_params: Any) -> ArchiveData:
    """Get archive data only based on the path of the .dat or .txt file.

    This function attempts to correct and test options, to try track down the file and metadata mentioned.

    Args:
        path: The path to the archive file in prodsonen to attempt to get metadata for and open.
        read_fwf_params: Remaining parameters to pass to pd.read_fwf, dtype, widths, names and na_values is overwritten,
            so dont use those.

    Returns:
        ArchiveData: An ArchiveData object containing the imported data, metadata, and code lists.

    Raises:
        ValueError: If no datadok-api endpoint is found for the path given.
    """
    combinations = get_path_combinations(path)
    url_path = test_url_combos(combinations)

    if url_path is None:
        url_path = go_back_in_time(path)
        if url_path is None:
            raise ValueError(
                f"Couldnt find datadok-api response, looked 20 years back in time, and looked for all of these combinations: {combinations}"
            )

    url_address = url_from_path(url_path)
    logger.info(f"Found datadok-response for path {url_path}")

    # Correcting path in
    for path, ext in combinations:
        filepath = f"{path}{ext}"
        if os.path.isfile(filepath):
            break
    else:
        raise ValueError(
            f"Couldnt find a file on disk for {filepath} using these combinations: {combinations}"
        )
    logger.info(f"Found datafile at path {filepath}")

    return import_archive_data(url_address, filepath, **read_fwf_params)


# Correcting path for API
def url_from_path(path: str) -> str:
    """Append sent path to the endpoint URL that datadok uses.

    Args:
        path: The path to append to the endpoint.

    Returns:
        str: The URL for the given path
    """
    return f"http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionByPath?path={path}"


def test_url(url: str) -> bool:
    """Test if there is content at the given endpoint in the Datadok-API.

    Args:
        url: The URL we should test.

    Returns:
        bool: True if there is content at the URL. False otherwise.
    """
    result = requests.get(url)
    if 200 != result.status_code or "Value cannot be null." in result.text:
        return False
    return True


def test_url_combos(combinations: list[tuple[str, str]]) -> None | str:
    """Tests a set of path combinations for valid responses from the Datadok-API.

    Args:
        combinations: A list of tuples, each tuple containing two elements.
            First element is most of the file path, second part is the file extensions, including "."

    Returns:
        None | str: Returns the tested path, if one test passes, if nothing is found, return None.
    """
    for path_head, ext in combinations:
        url_path = path_head + ext
        url_address = url_from_path(url_path)
        if test_url(url_address):
            return url_path
    return None


def get_path_combinations(
    path: str, file_exts: list[str] | None = None
) -> list[tuple[str, str]]:
    """Generate a list of combinations of possible paths and file extensions for a given path.

    Args:
        path: The given path, will be modified to include both $UTD, $UTD_PII, utd and utd_pii
        file_exts: Possible file extensions for the files. Defaults to ["", ".dat", ".txt"].

    Returns:
        list[tuple[str, str]]: The generated combinations for possible locations of the files.

    Raises:
        TypeError: If what we get for the dollar-paths is not a single string.
    """
    if file_exts is None:
        exts: list[str] = ["", ".dat", ".txt"]
    elif not isinstance(file_exts, list):
        exts = [file_exts]  # type: ignore[unreachable]
    else:
        exts = file_exts

    if path.endswith(".dat") or path.endswith(".txt"):
        path = path[:-4]

    paths = [path]

    stammer = linux_shortcuts()
    if path.startswith("$"):
        dollar: str = path.split("/")[0].replace("$", "").replace("_PII", "").upper()
        non_dollar = stammer.get(dollar, None)
        if non_dollar is not None:
            paths += [os.path.join(non_dollar, "/".join(path.split("/")[1:]))]
    else:
        if not path.startswith("/"):
            path = "/" + path
        path_parts = [x for x in path.split("/")[:4] if x]
        if len(path_parts) > 3:
            path_parts = path_parts[:3]
        non_dollar = "/" + "/".join(path_parts)
        dollar_want = get_key_by_value(stammer, non_dollar)
        if isinstance(dollar_want, str):
            dollar = dollar_want
        else:
            raise TypeError(
                "What we got out of the dollar-linux file was not a single string: {dollar_want}"
            )
        paths += [path.replace(non_dollar, "$" + dollar)]

    # add pii/ non-pii if necessary
    paths_pii = []
    for path in paths:
        if path.startswith("$"):
            path_parts = [x for x in path.split("/") if x]
            if not path_parts[0].endswith("_PII"):
                path_parts[0] += "_PII"
            else:
                path_parts[0] = path_parts[0].replace("_PII", "")
            paths_pii += ["/".join(path_parts)]
        else:
            path_parts = [x for x in path.split("/") if x]
            if not path_parts[2].endswith("_pii"):
                path_parts[2] += "_pii"
            else:
                path_parts[2] = path_parts[2].replace("_pii", "")
            paths_pii += ["/" + "/".join(path_parts)]

    paths += paths_pii

    return [(path, ext) for path in paths for ext in exts]


def go_back_in_time(path: str) -> str | None:
    """Look for datadok-api URLs back in time. Sometimes new ones are not added, if the previous still works.

    Only modifies yearly publishings for now...

    Args:
        path: The path to modify and test for previous years.

    Returns:
        str | None: The path that was found, with a corresponding URL with content in the Datadok-API.
            If nothing is found returns None.
    """
    # Identify character ranges we want to manipulate
    yr_char_ranges: list[tuple[int, int]] = []
    path_parts = path.rsplit("/", 1)
    while True:
        if not yr_char_ranges:
            last_offset = 0
        else:
            last_offset = yr_char_ranges[-1][-1]
        if (
            len(path_parts[1]) > last_offset
            and path_parts[1][last_offset].lower() == "g"
            and path_parts[1][last_offset + 1 : last_offset + 5].lower().isdigit()
        ):
            yr_char_ranges += [(last_offset + 1, last_offset + 5)]
        else:
            break

    if yr_char_ranges:
        # Looking 20 years back in time
        for looking_back in range(-1, -20, -1):
            for year_range in yr_char_ranges:
                yr = path_parts[1][year_range[0] : year_range[1]]
                path_parts[1] = (
                    path_parts[1][: year_range[0]]
                    + str(int(yr) - 1)
                    + path_parts[1][year_range[1] :]
                )
            yr_combinations = get_path_combinations("/".join(path_parts))
            for path, ext in yr_combinations:
                url_path = path + ext
                url_address = url_from_path(url_path)
                if test_url(url_address):
                    f"Looking back {looking_back} years, found a path at {path + ext}"
                    return path + ext

    logger.info(f"Looking back {looking_back} years, DIDNT find a path at {path + ext}")
    return None
