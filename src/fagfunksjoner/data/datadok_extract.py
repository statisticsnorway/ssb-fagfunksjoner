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
import xml.dom.minidom
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
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
    # register xml namespaces
    ns = {
        "ns": "http://www.ssb.no/ns/meta",
        "common": "http://www.ssb.no/ns/meta/common",
    }

    contact_info = root.find("ns:ContactInformation", ns)
    if contact_info is None:
        xml_str = ET.tostring(root, encoding="utf8").decode("utf8")
        pretty_xml = xml.dom.minidom.parseString(xml_str).toprettyxml(indent="  ")
        raise ValueError(f"ContactInformation not found in the XML: {pretty_xml}")

    division = contact_info.find("common:Division", ns)
    if division is None or division.text is None:
        raise ValueError("Division not found in the XML or has no text")

    division_text = division.text

    for context_var in root.findall("ns:ContextVariable", ns):
        context_id = context_var.get("id")
        title_elem = context_var.find("ns:Title", ns)
        description_elem = context_var.find("ns:Description", ns)
        properties = context_var.find("ns:Properties", ns)

        if title_elem is None or title_elem.text is None:
            raise ValueError("Title element missing or has no text")
        if description_elem is None or description_elem.text is None:
            raise ValueError("Description element missing or has no text")
        if properties is None:
            raise ValueError("Properties element missing")

        datatype_elem = properties.find("ns:Datatype", ns)
        length_elem = properties.find("ns:Length", ns)
        start_position_elem = properties.find("ns:StartPosition", ns)
        precision_elem = properties.find("ns:Precision", ns)

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
        elif (
            df[col].str.contains(".", regex=False).any()
        ):  # "." is a special character in regex, making this fail if regex is used.
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
    archive_desc_xml: str, archive_file: str | Path, **read_fwf_params: Any
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
    if test_url(archive_desc_xml):
        xml_file = requests.get(archive_desc_xml).text
        logger.debug(
            f"Opening datadok metadata from URL: {archive_desc_xml}, with content: {xml_file}"
        )
    else:
        with open(archive_desc_xml) as file:
            xml_file = file.read()
        logger.debug(
            f"Opening datadok metadata from FILE: {archive_desc_xml}, with content: {xml_file}"
        )
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


def open_path_datadok(path: str | Path, **read_fwf_params: Any) -> ArchiveData:
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
        FileNotFoundError: If more than one file matches, with different file extensions, we do not know which to pick.
    """
    path_lib = convert_to_pathlib(path)
    combinations = get_path_combinations(path_lib, file_exts=[""])
    url_path = test_url_combos(combinations)

    if url_path is None:
        url_path = go_back_in_time(path_lib, file_exts=[""])
        if url_path is None:
            raise ValueError(
                f"Couldnt find datadok-api response, looked 20 years back in time, and looked for all of these combinations: {combinations}"
            )

    url_address = url_from_path(url_path)
    logger.info(f"Found datadok-response for path {url_path}")

    file_combinations = get_path_combinations(
        path_lib.with_suffix(""), file_exts=None, add_dollar=False
    )  # file_exts=None gets replaced by dat, txt, ""
    logger.debug(f"Will try combinations: {file_combinations}")
    # Correcting path in
    if str(path_lib).startswith("$"):
        path_lib = replace_dollar_stamme(path_lib)
    if path_lib.is_file():
        filepath = path_lib
    else:
        for path, ext in file_combinations:
            filepath = path.with_suffix(ext)
            if filepath.is_file():
                break
        else:
            # Last resort, see if any file matches stripping the extensions
            filelist = list(path_lib.parent.glob(path_lib.stem + "*"))
            # If more than one, we cannot now which one you want...
            if len(filelist) > 2:
                msg = f"Found more than one matching file {filelist}. Specify file ending please."
                raise FileNotFoundError(msg)
            elif len(filelist) == 0:
                msg = (
                    f"Found no file matching the name {filepath} in the folder: {path_lib.parent}"
                )
                raise FileNotFoundError(msg)
            # Replace filepath with file we found matching name
            filepath = filelist[0]

        logger.info(f"Found datafile at path {filepath}")

    return import_archive_data(url_address, filepath, **read_fwf_params)


# Correcting path for API
def url_from_path(path: str | Path) -> str:
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
    try:
        urlparse(url)
    except AttributeError:
        logger.debug("Url tested, not parsed correctly.")
        return False
    logger.debug("Url tested, parsed correctly.")
    try:
        result = requests.get(url)
        if 200 != result.status_code or "Value cannot be null." in result.text:
            logger.debug(
                f"Url tested, found no content at endpoint, either status_code is not 200: {result.status_code}, or there is null value text: {result.text}"
            )
            return False
        logger.debug("Url tested, found content at endpoint.")
        return True
    except (requests.exceptions.ConnectionError, requests.exceptions.MissingSchema):
        logger.debug("Url tested, cought error.")
        return False


def test_url_combos(combinations: list[tuple[Path, str]]) -> None | str | Path:
    """Tests a set of path combinations for valid responses from the Datadok-API.

    Args:
        combinations: A list of tuples, each tuple containing two elements.
            First element is most of the file path, second part is the file extensions, including "."

    Returns:
        None | str: Returns the tested path, if one test passes, if nothing is found, return None.
    """
    for path_head, ext in combinations:
        url_path = path_head.with_suffix(ext)
        url_address = url_from_path(url_path)
        if test_url(url_address):
            return url_path
    return None


def convert_to_pathlib(path: str | Path) -> Path:
    """Make sure the path is converted to pathlib.Path.

    Args:
        path (str | Path): The path to possibly convert.

    Returns:
        Path: The converted path.
    """
    if not isinstance(path, Path):
        return Path(path)
    return path


def get_path_combinations(
    path: str | Path, file_exts: list[str] | str | None = None, add_dollar: bool = True
) -> list[tuple[Path, str]]:
    """Generate a list of combinations of possible paths and file extensions for a given path.

    Args:
        path: The given path, will be modified to include both $UTD, $UTD_PII, utd and utd_pii
        file_exts: Possible file extensions for the files. Defaults to ["", ".dat", ".txt"].
        add_dollar: If we should add dollar paths (not needed for file opening.)

    Returns:
        list[tuple[str, str]]: The generated combinations for possible locations of the files.
    """
    path_lib = convert_to_pathlib(path)

    if file_exts is None:
        exts: list[str] = ["", ".dat", ".txt"]
    elif isinstance(file_exts, str):
        exts = [file_exts]
    else:
        exts = file_exts

    paths: list[Path] = add_dollar_or_nondollar_path(path_lib, add_dollar=add_dollar)
    paths = add_pii_paths(paths)

    return [
        (
            p,
            ext,
        )
        for p in paths
        for ext in exts
    ]


def add_pii_paths(paths: list[Path]) -> list[Path]:
    """Add PII-paths to a list of paths, to look in more places.

    Args:
        paths (list[str]): List containing paths that we will add PII or non-PII paths for.

    Returns:
        list[str]: List containing more paths now, we should have added to it.
    """
    paths_pii = []
    for path in paths:
        path_lib = convert_to_pathlib(path)
        if str(path_lib).startswith("$"):
            if not path_lib.parts[0].endswith("_PII"):
                new_path = Path(str(path_lib.parts[0]) + "_PII", *path_lib.parts[1:])
                logger.debug(f"{new_path=}")
            else:
                new_path = Path(
                    str(path_lib.parts[0]).replace("_PII", ""), *path_lib.parts[1:]
                )
        else:
            if not path_lib.parts[3].endswith("_pii"):
                new_path = Path(
                    *path_lib.parts[:3],
                    str(path_lib.parts[3]) + "_pii",
                    *path_lib.parts[4:],
                )
            else:
                new_path = Path(
                    *path_lib.parts[:3],
                    str(path_lib.parts[3]).replace("_pii", ""),
                    *path_lib.parts[4:],
                )
        paths_pii += [new_path]
    return paths + paths_pii

def replace_dollar_stamme(path_lib: Path) -> Path | None:
    """Replace the dollar in a path with the full path using the linux-stammer.

    Args:
        path_lib (Path): Th inpath, suspected to have a dollar.

    Returns:
        Path | None: Corrected path returned.
    """
    dollar: str = (
        str(path_lib.parts[0]).replace("$", "").replace("_PII", "").upper()
    )
    non_dollar = linux_shortcuts().get(dollar, None)
    if non_dollar is not None:
        new_path = Path(non_dollar, *path_lib.parts[1:])
        logger.debug(f"Constructed new_path {new_path} from dollar {dollar} and non_dollar {non_dollar} from path {path_lib}")
        return new_path
    return None


def add_dollar_or_nondollar_path(
    path: str | Path, add_dollar: bool = True
) -> list[Path]:
    """Add a $-path or non-$-path to an existing path. Output should be a list of length 2.

    Args:
        path: The path to expand on.
        add_dollar: If we should add dollar-paths (not needed for opening file).

    Raises:
        TypeError: If what we get for the dollar-paths is not a single string.

    Returns:
        list[str]: List containing one more path now.
    """
    path_lib = convert_to_pathlib(path)
    paths = [path_lib]
    if str(path_lib).startswith("$"):
        new_path = replace_dollar_stamme(path_lib)
        paths += [new_path]
    elif add_dollar:
        if len(path_lib.parts) >= 4:
            non_dollar_path = Path(*path_lib.parts[:4])
        else:
            non_dollar_path = path_lib
        logger.debug(
            f"Looking up in stammer with {non_dollar_path.as_posix()!s} path_lib number of parts: {len(path_lib.parts)} {path_lib.parts}"
        )
        dollar_want = get_key_by_value(linux_shortcuts(), str(non_dollar_path.as_posix()))
        if isinstance(dollar_want, str):
            dollar = dollar_want
        else:
            raise TypeError(
                "What we got out of the dollar-linux file was not a single string: {dollar_want}"
            )
        new_path = Path(
            str(path_lib.as_posix()).replace(
                str(non_dollar_path.as_posix()), "$" + dollar
            )
        )
        paths += [new_path]
        logger.info(
            f"Path after adding dollar '{new_path}', replacing non-dollar: '{non_dollar_path.as_posix()!s}' with '${dollar}', paths: {paths}"
        )
    return paths


def go_back_in_time(
    path: str | Path, file_exts: list[str] | None = None
) -> Path | None:
    """Look for datadok-api URLs back in time. Sometimes new ones are not added, if the previous still works.

    Only modifies yearly publishings for now...

    Args:
        path: The path to modify and test for previous years.
        file_exts: The different file extensions to try.

    Returns:
        str | None: The path that was found, with a corresponding URL with content in the Datadok-API.
            If nothing is found returns None.
    """
    path_lib = convert_to_pathlib(path)
    if file_exts is None:
        exts: list[str] = ["", ".dat", ".txt"]
    # Identify character ranges we want to manipulate in the filename
    yr_char_ranges = get_yr_char_ranges(path_lib)
    # Loop over the years we want to look at, changing all the year ranges in the path
    if yr_char_ranges:
        # Looking 20 years back in time
        for looking_back in range(-1, -20, -1):
            for year_range in yr_char_ranges:
                yr = path_lib.name[year_range[0] : year_range[1]]
                name_update = (
                    path_lib.name[: year_range[0]]
                    + str(int(yr) - 1)
                    + path_lib.name[year_range[1] :]
                )
                new_path = Path(path_lib.parent, name_update)
            yr_combinations = get_path_combinations(new_path, file_exts=exts)
            for yrpath, ext in yr_combinations:
                url_address = url_from_path(yrpath.with_suffix(ext))
                if test_url(url_address):
                    f"Looking back {looking_back} years, found a path at {yrpath.with_suffix(ext)}"
                    return yrpath.with_suffix(ext)

        logger.info(
            f"Looking back {looking_back} years, DIDNT find a path at {yrpath.with_suffix(ext)}"
        )
    else:
        logger.info(
            "Couldnt determine any year ranges in the pattern gXXXX (possibly repeating, like gXXXXgXXXX.)."
        )
    return None


def get_yr_char_ranges(path: str | Path) -> list[tuple[int, int]]:
    """Find the character ranges containing years in the path. Usually 1-4 ranges.

    Args:
        path: The filename to look at for character ranges.

    Returns:
        list[tuple[int, int]]: A list of tuples, tuples have length 2,
            one int for the starting position of a year range, and one int for the last position.
    """
    path_lib = convert_to_pathlib(path)
    yr_char_ranges: list[tuple[int, int]] = []
    while True:
        if not yr_char_ranges:
            last_offset = 0
        else:
            last_offset = yr_char_ranges[-1][-1]
        if (
            len(path_lib.stem) > last_offset
            and path_lib.stem[last_offset].lower() == "g"
            and path_lib.stem[last_offset + 1 : last_offset + 5].lower().isdigit()
        ):
            yr_char_ranges += [(last_offset + 1, last_offset + 5)]
        else:
            break
    return yr_char_ranges
