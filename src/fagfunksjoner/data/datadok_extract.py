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

from fagfunksjoner.fagfunksjoner_logger import logger


STRING_DTYPE = "string[pyarrow]"

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
        url (str): The URL to validate.

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
def get_element_text(
    element: ET.Element, tag: str, namespace: str, error_msg: str
) -> str:
    """Retrieves the text content of a specific element and raises an error if not found or empty.

    Args:
        element (ET.Element): The parent element to search within.
        tag (str): The tag of the element to find.
        namespace (str): The XML namespace.
        error_msg (str): The error message to raise if the element is not found or has no text.

    Returns:
        str: The text content of the found element.

    Raises:
        ValueError: If the element is not found or has no text.
    """
    found_elem = element.find(f"{namespace}{tag}")
    if found_elem is None or found_elem.text is None:
        raise ValueError(error_msg)
    return found_elem.text


def extract_contact_info(root: ET.Element, namespace: str) -> str:
    """Extracts division text from the contact information in the XML root element.

    Args:
        root (ET.Element): The root element of the XML tree.
        namespace (str): The XML namespace for the contact information.

    Returns:
        str: The division text.

    Raises:
        ValueError: If contact information or division is missing.
    """
    contact_info = root.find(f"{namespace}ContactInformation")
    if contact_info is None:
        raise ValueError("ContactInformation not found in the XML")
    return get_element_text(
        contact_info,
        "Division",
        f"{namespace}common",
        "Division not found in the XML or has no text",
    )


def extract_properties(
    properties: ET.Element, namespace: str
) -> tuple[str, int, int, None | int]:
    """Extracts properties of a context variable from the properties element.

    Args:
        properties (ET.Element): The properties element to extract data from.
        namespace (str): The XML namespace for the properties.

    Returns:
        tuple: A tuple containing datatype, length, start position, and precision.
    """
    datatype = get_element_text(
        properties, "Datatype", namespace, "Datatype element missing or has no text"
    )
    length = int(
        get_element_text(
            properties, "Length", namespace, "Length element missing or has no text"
        )
    )
    start_position = int(
        get_element_text(
            properties,
            "StartPosition",
            namespace,
            "StartPosition element missing or has no text",
        )
    )

    precision_elem = properties.find(f"{namespace}Precision")
    precision = (
        int(precision_elem.text)
        if precision_elem is not None and precision_elem.text is not None
        else None
    )

    return datatype, length, start_position, precision


def extract_context_variable(
    context_var: ET.Element, namespace: str, division_text: str
) -> ContextVariable:
    """Extracts a ContextVariable object from a context variable element.

    Args:
        context_var (ET.Element): The context variable element to extract data from.
        namespace (str): The XML namespace for the context variables.
        division_text (str): The division text extracted from contact information.

    Returns:
        ContextVariable: The extracted ContextVariable object.

    Raises:
        ValueError: If required elements are missing or have no text.
    """
    context_id = context_var.get("id", "")
    title = get_element_text(
        context_var, "Title", namespace, "Title element missing or has no text"
    )
    description = get_element_text(
        context_var,
        "Description",
        namespace,
        "Description element missing or has no text",
    )
    properties = context_var.find(f"{namespace}Properties")
    if properties is None:
        raise ValueError("Properties element missing")

    datatype, length, start_position, precision = extract_properties(
        properties, namespace
    )

    return ContextVariable(
        context_id,
        title,
        description,
        datatype,
        length,
        start_position,
        precision,
        division_text,
    )


def extract_context_variables(root: ET.Element) -> list[ContextVariable]:
    """Extracts context variables from the XML root element and returns a list of ContextVariable objects.

    Args:
        root (ET.Element): The root element of the XML tree to parse.

    Returns:
        List[ContextVariable]: A list of ContextVariable objects.
    """
    namespace = "{http://www.ssb.no/ns/meta}"
    division_text = extract_contact_info(root, namespace)
    context_vars = []

    for context_var in root.findall(f"{namespace}ContextVariable"):
        context_vars.append(
            extract_context_variable(context_var, namespace, division_text)
        )

    return context_vars


def extract_codes(
    codes_element: ET.Element,
    context_id: str,
    title: str,
    description: str,
    namespace: str,
) -> list[CodeList]:
    """Extracts codes from the codes element.

    Args:
        codes_element (ET.Element): The codes element to extract data from.
        context_id (str): The context ID.
        title (str): The title of the codelist.
        description (str): The description of the codelist.
        namespace (str): The XML namespace for the codelist.

    Returns:
        List[CodeList]: A list of CodeList objects.
    """
    codelist_data = []
    for code in codes_element.findall(f"{namespace}Code"):
        code_value = get_element_text(
            code, "CodeValue", namespace, "Cant find CodeValue."
        )
        code_text = get_element_text(code, "CodeText", namespace, "Cant find CodeText.")
        if code_value and code_text:
            codelist_data.append(
                CodeList(context_id, title, description, code_value, code_text)
            )
    return codelist_data


def extract_codelist_meta(
    codelist_meta: ET.Element, context_id: str, namespace: str
) -> list[CodeList]:
    """Extracts codelist meta information and associated codes.

    Args:
        codelist_meta (ET.Element): The codelist meta element to extract data from.
        context_id (str): The context ID.
        namespace (str): The XML namespace for the codelist.

    Returns:
        List[CodeList]: A list of CodeList objects.
    """
    codelist_data = []
    title = get_element_text(codelist_meta, "Title", namespace, "Cant find Title.")
    description = get_element_text(
        codelist_meta, "Description", namespace, "Cant find Description."
    )
    if title and description:
        codes = codelist_meta.find(f"{namespace}Codes")
        if codes:
            codelist_data.extend(
                extract_codes(codes, context_id, title, description, namespace)
            )
    return codelist_data


def extract_context_variable_codelist(
    context_var: ET.Element, namespace: str
) -> list[CodeList]:
    """Extracts codelist data from a context variable element.

    Args:
        context_var (ET.Element): The context variable element to extract data from.
        namespace (str): The XML namespace for the codelist.

    Returns:
        List[CodeList]: A list of CodeList objects.
    """
    codelist_data = []
    codelist = context_var.find(f"{namespace}Codelist")
    if codelist:
        codelist_meta = codelist.find(f"{namespace}CodelistMeta")
        if codelist_meta:
            context_id = context_var.get("id", "")
            codelist_data.extend(
                extract_codelist_meta(codelist_meta, context_id, namespace)
            )
    return codelist_data


def extract_codelist(root: ET.Element) -> list[CodeList]:
    """Extracts code lists from the XML root element and returns a list of CodeList objects.

    Args:
        root (ET.Element): The root element of the XML tree to parse.

    Returns:
        List[CodeList]: A list of CodeList objects.
    """
    namespace = "{http://www.ssb.no/ns/meta/codelist}"
    codelist_data = []
    for context_var in root.findall("{http://www.ssb.no/ns/meta}ContextVariable"):
        codelist_data.extend(extract_context_variable_codelist(context_var, namespace))
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
        context_variables (list[ContextVariable]): A list of ContextVariable objects.

    Returns:
        pd.DataFrame: A DataFrame containing the context variable information.
    """
    df = pd.DataFrame([vars(cv) for cv in context_variables])
    df["type"] = (
        df["datatype"]
        .str.replace("Tekst", STRING_DTYPE, regex=False)
        .str.replace("Heltall", "Int64", regex=False)
        .str.replace("Desimaltall", STRING_DTYPE, regex=False)
        .str.replace("Desim. (K)", STRING_DTYPE, regex=False)
        .str.replace("Desim. (P)", STRING_DTYPE, regex=False)
        .str.replace("Dato1", STRING_DTYPE, regex=False)
        .str.replace("Dato2", STRING_DTYPE, regex=False)
    )
    return df


def codelist_to_dict(codelist_df: pd.DataFrame) -> dict[str, dict[str, str]]:
    """Converts a DataFrame containing code lists to a dictionary.

    Args:
        codelist_df (pd.DataFrame): DataFrame containing the code list information.

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
        date_str (str): The date string to be parsed.
        date_format (str): The format in which the date string is.

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
        metadata_df (pd.DataFrame): DataFrame containing metadata.

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
        df (pd.DataFrame): A DataFrame containing metadata.

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
        df (pd.DataFrame): The DataFrame containing archive data.
        metadata_df (pd.DataFrame): The DataFrame containing metadata.

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
        df (pd.DataFrame): The DataFrame containing archive data.
        metadata_df (pd.DataFrame): The DataFrame containing metadata.

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
        df (pd.DataFrame): The DataFrame containing archive data.
        metadata_df (pd.DataFrame): The DataFrame containing metadata.

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
        if df[col].str.contains(",").any():
            df[col] = df[col].str.replace(",", ".").astype("Float64")
        # Look for punktum as delimiter
        elif df[col].str.contains(".").any():
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
        archive_desc_xml (str): Path or URL to the XML file describing the archive.
        archive_file (str): Path to the archive file.
        read_fwf_params (Any): Remaining parameters to pass to pd.read_fwf, dtype, widths, names and na_values is overwritten,
            so dont use those.

    Returns:
        ArchiveData: An ArchiveData object containing the imported data, metadata, and code lists.

    Raises:
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
    root = ET.fromstring(xml_file)
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
        path (str): Path to the archive file on linux.
        metapath (str): Path described in datadok.
        read_fwf_params (Any): Remaining parameters to pass to pd.read_fwf, dtype, widths, names and na_values is overwritten,
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
        path (str): The path to the archive file in prodsonen to attempt to get metadata for and open.
        read_fwf_params (Any): Remaining parameters to pass to pd.read_fwf, dtype, widths, names and na_values is overwritten,
            so dont use those.

    Returns:
        ArchiveData: An ArchiveData object containing the imported data, metadata, and code lists.
    """
    # Correcting path for API
    dokpath = path
    if dokpath.endswith(".dat") or dokpath.endswith(".txt"):
        dokpath = ".".join(dokpath.split(".")[:-1])
    url_address = f"http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionByPath?path={dokpath}"

    # Correcting address if it doesnt go anywhere
    if 200 != requests.get(url_address).status_code and not path.startswith("$"):
        for name, stamm in os.environ.items():
            if not name.startswith("JUPYTERHUB") and path.startswith(stamm):
                dokpath = f"${name}{path.replace(stamm,'')}"
    url_address = f"http://ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionByPath?path={dokpath}"

    # Correcting path in
    filepath = path
    # Flip Stamm
    for name, stamm in os.environ.items():
        if not name.startswith("JUPYTERHUB") and filepath.startswith(f"${name}"):
            end = filepath.replace(f"${name}", "")
            if end.startswith(os.sep):
                end = end[len(os.sep) :]
            filepath = os.path.join(stamm, end)

    if filepath.endswith(".txt") or filepath.endswith(".dat"):
        ...
    else:
        if os.path.isfile(f"{filepath}.txt"):
            filepath += ".txt"
        elif os.path.isfile(f"{filepath}.dat"):
            logger.info(filepath)
            filepath += ".dat"

    return import_archive_data(url_address, filepath, **read_fwf_params)
