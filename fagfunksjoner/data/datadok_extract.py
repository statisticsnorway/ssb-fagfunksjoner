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

# %%
from dataclasses import dataclass, field
from typing import List, Dict

import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
import requests


# %% [markdown]
# ## Hente fra api til Datadok
# Vi har et api til datadok og det returnerer filbeskrivelse som en html-fil. Det kan f.eks. kalles slik
#
# `curl -i 'ws.ssb.no/DatadokService/DatadokService.asmx/GetFileDescriptionByPath?path=$ENERGI/er_eb/arkiv/grunnlag/g1990'
# `
#
# Den interne metadataportalen http://www.byranettet.ssb.no/metadata/ har også alle filbeskrivelsene og filvariablene. 

# %%
def is_valid_url(url):
    """
    Check if the provided URL is valid.

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
    """
    Class representing a context variable.
    """
    context_id: str
    title: str
    description: str
    datatype: str
    length: int
    start_position: int
    precision: int
    division: str

@dataclass
class CodeList:
    """
    Class representing a code list.
    """
    context_id: str
    codelist_title: str
    codelist_description: str
    code_value: str
    code_text: str

@dataclass
class Metadata:
    """
    Class representing metadata which includes context variables and code lists.
    """
    context_variables: List[ContextVariable]
    codelists: List[CodeList]

@dataclass
class ArchiveData:
    """
    Class representing the archive data along with its metadata and code lists.
    """
    df: pd.DataFrame
    metadata_df: pd.DataFrame
    codelist_df: pd.DataFrame
    codelist_dict: Dict[str, CodeList]
    names: List[str]
    widths: List[int]
    datatypes: Dict[str, str]


# %%
def extract_context_variables(root) -> list:
    """
    Extracts context variables from the XML root element and returns a list of ContextVariable objects.

    Args:
        root (ET.Element): The root element of the XML tree to parse.

    Returns:
        list: A list of ContextVariable objects.
    """
    data = []
    contact_info = root.find('{http://www.ssb.no/ns/meta}ContactInformation')
    division = contact_info.find('{http://www.ssb.no/ns/meta/common}Division').text
    for context_var in root.findall('{http://www.ssb.no/ns/meta}ContextVariable'):
        context_id = context_var.get('id')
        title = context_var.find('{http://www.ssb.no/ns/meta}Title').text
        description = context_var.find('{http://www.ssb.no/ns/meta}Description').text
        properties = context_var.find('{http://www.ssb.no/ns/meta}Properties')
        datatype = properties.find('{http://www.ssb.no/ns/meta}Datatype').text
        length = properties.find('{http://www.ssb.no/ns/meta}Length').text
        start_position = properties.find('{http://www.ssb.no/ns/meta}StartPosition').text
        precision_tag = properties.find('{http://www.ssb.no/ns/meta}Precision')
        precision = precision_tag.text if precision_tag is not None else None
        data.append(ContextVariable(context_id, title, description, datatype, length, start_position, precision, division))
    return data

def extract_codelist(root) -> list:
    """
    Extracts code lists from the XML root element and returns a list of CodeList objects.

    Args:
        root (ET.Element): The root element of the XML tree to parse.

    Returns:
        list: A list of CodeList objects.
    """
    codelist_data = []
    for context_var in root.findall('{http://www.ssb.no/ns/meta}ContextVariable'):
        codelist = context_var.find('{http://www.ssb.no/ns/meta/codelist}Codelist')
        if codelist is not None:
            codelist_meta = codelist.find('{http://www.ssb.no/ns/meta/codelist}CodelistMeta')
            codelist_title = codelist_meta.find('{http://www.ssb.no/ns/meta/codelist}Title').text
            codelist_description = codelist_meta.find('{http://www.ssb.no/ns/meta/codelist}Description').text

            codes = codelist.find('{http://www.ssb.no/ns/meta/codelist}Codes')
            for code in codes.findall('{http://www.ssb.no/ns/meta/codelist}Code'):
                code_value = code.find('{http://www.ssb.no/ns/meta/codelist}CodeValue').text
                code_text = code.find('{http://www.ssb.no/ns/meta/codelist}CodeText').text
                codelist_data.append(CodeList(context_var.get('id'), codelist_title, codelist_description, code_value, code_text))
    return codelist_data

def codelist_to_df(codelist) -> pd.DataFrame:
    """
    Converts a list of CodeList objects to a DataFrame.

    Args:
        codelist (list): A list of CodeList objects.

    Returns:
        pd.DataFrame: A DataFrame containing the code list information.
    """
    return pd.DataFrame([vars(cl) for cl in codelist])

def metadata_to_df(context_variables) -> pd.DataFrame:
    """
    Converts a list of ContextVariable objects to a DataFrame.

    Args:
        context_variables (list): A list of ContextVariable objects.

    Returns:
        pd.DataFrame: A DataFrame containing the context variable information.
    """
    df = pd.DataFrame([vars(cv) for cv in context_variables])
    df['type'] = (
        df['datatype']
        .str.replace('Tekst', 'string[pyarrow]', regex=False)
        .str.replace('Heltall', 'Int64', regex=False)
        .str.replace('Desimaltall', 'Float64', regex=False)
        .str.replace('Desim. (K)', 'Float64', regex=False)
        .str.replace('Desim. (P)', 'Float64', regex=False)
        .str.replace('Dato1', 'string[pyarrow]', regex=False)
        .str.replace('Dato2', 'string[pyarrow]', regex=False)
    )
    return df

def codelist_to_dict(codelist_df) -> dict:
    """
    Converts a DataFrame containing code lists to a dictionary.

    Args:
        codelist_df (pd.DataFrame): DataFrame containing the code list information.

    Returns:
        dict: A dictionary mapping code list titles to dictionaries of code values and texts.
    """
    if codelist_df.empty:
        print('NOTE: Filbeskrivelsen har ingen kodelister')
        return {}

    col_dict = {
        col: dict(zip(sub_df['code_value'], sub_df['code_text']))
        for col, sub_df in codelist_df.groupby('codelist_title')
    }

    return col_dict

def date_parser(date_str, format):
    """
    Parses a date string into a datetime object based on the provided format.

    Args:
        date_str (str): The date string to be parsed.
        format (str): The format in which the date string is.

    Returns:
        datetime: The parsed datetime object, or pd.NaT if parsing fails.
    """
    try:
        return datetime.strptime(date_str, format)
    except ValueError:
        return pd.NaT

def date_formats(metadata_df: pd.DataFrame) -> dict:
    """
    Creates a dictionary of date conversion functions based on the metadata DataFrame.

    Args:
        metadata_df (pd.DataFrame): DataFrame containing metadata.

    Returns:
        dict: A dictionary mapping column titles to date conversion formats.
    """
    date_formats = {
        ('Dato1', 8): "%Y%m%d",
        ('Dato1', 6): "%y%m%d",
        ('Dato2', 8): "%d%m%Y",
        ('Dato2', 6): "%d%m%y"
    }
    
    date_metas_mask = ((metadata_df["length"].astype("Int64").isin([6, 8])) &
                      (metadata_df["datatype"].isin(['Dato1', 'Dato2'])))
    
    # If there are dateformats we dont know about, we want an error on that
    not_catched = metadata_df[~date_metas_mask]
    missing_date_formats = []
    for _, row in not_catched.iterrows():
        if "dato" in row["datatype"].lower() or "date" in row["datatype"].lower():
            raise ValueError(f"Dataformatting for metadatarow not catched: {row}")
    
    date_metas = metadata_df[date_metas_mask]
    
    # If there are no date columns to convert, exit function
    if not len(date_metas):
        print('NOTE: Ingen datofelt funnet')
        return {}
    
    # Pick the formattings that are known
    formattings = {}
    for _, row in date_metas.iterrows():
        formatting = date_formats.get((row['datatype'], row['length']), None)
        if formatting:
            formattings[row['title']] = formatting
    return formattings


def convert_dates(df: pd.DataFrame, metadata_df: pd.DataFrame) -> pd.DataFrame:
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
    return df


def import_parameters(df: pd.DataFrame) -> list:
    """
    Extracts parameters from the metadata DataFrame for importing archive data.

    Args:
        df (pd.DataFrame): A DataFrame containing metadata.

    Returns:
        list: A list containing column names, column lengths, datatypes, and decimal separator.
    """
    col_names = df['title'].tolist()
    col_lengths = df['length'].astype(int).tolist()
    datatype = dict(zip(df['title'], df['type']))
    decimal = ',' if 'Desim. (K)' in df['datatype'].values else '.'
    return col_names, col_lengths, datatype, decimal

def downcast_ints(df: pd.DataFrame, metadata_df: pd.DataFrame) -> pd.DataFrame:
    """Store ints as the lowest possible datatype that can contain the values.
    
    Args:
        df (pd.DataFrame): The DataFrame containing archive data.
        metadata_df (pd.DataFrame): The DataFrame containing metadata.

    Returns:
        pd.DataFrame: The modified archive DataFrame with downcast ints.
    """
    int_cols = metadata_df.loc[metadata_df["type"] == "Int64", "title"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    return df

def move_decimal(archive_df: pd.DataFrame, metadata_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adjusts the decimal values in the archive DataFrame based on the metadata.

    Args:
        archive_df (pd.DataFrame): The DataFrame containing archive data.
        metadata_df (pd.DataFrame): The DataFrame containing metadata.

    Returns:
        pd.DataFrame: The modified archive DataFrame with adjusted decimal values.
    """
    desi_col = metadata_df['title'].loc[metadata_df['datatype'] == 'Desimaltall'].tolist()
    num_desi_col = metadata_df['precision'].loc[metadata_df['datatype'] == 'Desimaltall'].astype(int).tolist()
    if len(desi_col) > 0:
        col_no = 0
        for col in desi_col: 
            divisor = 10**(num_desi_col[col_no])
            archive_df[col] = archive_df[col].div(divisor)
            col_no += 1
    return archive_df

def import_archive_data(archive_desc_xml: str, archive_file: str) -> ArchiveData:
    """
    Imports archive data based on the given XML description and archive file.

    Args:
        archive_desc_xml (str): Path or URL to the XML file describing the archive.
        archive_file (str): Path to the archive file.

    Returns:
        ArchiveData: An ArchiveData object containing the imported data, metadata, and code lists.
        
    Example usage:
        archive_data = import_archive_data('path_to_xml.xml', 'path_to_archive_file.txt')
        print(archive_data.df)
    """
    if is_valid_url(archive_desc_xml):
        xml_file = requests.get(archive_desc_xml).text
    else:
        with open(archive_desc_xml, 'r') as file:
            xml_file = file.read()
    root = ET.fromstring(xml_file)
    context_variables = extract_context_variables(root)
    codelists = extract_codelist(root)
    metadata = Metadata(context_variables, codelists)

    metadata_df = metadata_to_df(metadata.context_variables)
    codelist_df = codelist_to_df(metadata.codelists)
    codelist_dict = codelist_to_dict(codelist_df)
    names, widths, datatypes, decimal = import_parameters(metadata_df)
    df = pd.read_fwf(archive_file,
                     dtype=datatypes,
                     widths=widths,
                     names=names,
                     decimal=','
                    )
    df = convert_dates(df, metadata_df)
    #df = move_decimal(df, metadata_df)  # During testing this was not necessary to, gives wrong result
    df = downcast_ints(df, metadata_df)
    return ArchiveData(df, metadata_df, codelist_df, codelist_dict, names, widths, datatypes)
