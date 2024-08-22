import math
import os
import tempfile
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime
from unittest.mock import mock_open, patch, ANY

import pandas as pd
import pytest

from fagfunksjoner.data import datadok_extract


def test_extract_context_variables():
    xml_data = """
    <root xmlns:meta="http://www.ssb.no/ns/meta" xmlns:common="http://www.ssb.no/ns/meta/common">
        <meta:ContactInformation>
            <common:Division>Test Division</common:Division>
        </meta:ContactInformation>
        <meta:ContextVariable id="1">
            <meta:Title>Variable Title</meta:Title>
            <meta:Description>Variable Description</meta:Description>
            <meta:Properties>
                <meta:Datatype>Tekst</meta:Datatype>
                <meta:Length>10</meta:Length>
                <meta:StartPosition>1</meta:StartPosition>
            </meta:Properties>
        </meta:ContextVariable>
    </root>
    """
    root = ET.fromstring(xml_data)
    context_variables = datadok_extract.extract_context_variables(root)

    assert len(context_variables) == 1
    assert context_variables[0].context_id == "1"
    assert context_variables[0].title == "Variable Title"
    assert context_variables[0].description == "Variable Description"
    assert context_variables[0].datatype == "Tekst"
    assert context_variables[0].length == 10
    assert context_variables[0].start_position == 1
    assert context_variables[0].division == "Test Division"


def test_codelist_to_df():
    codelist = [
        datadok_extract.CodeList(
            context_id="1",
            codelist_title="Title1",
            codelist_description="Desc1",
            code_value="001",
            code_text="Code Text 1",
        ),
        datadok_extract.CodeList(
            context_id="2",
            codelist_title="Title2",
            codelist_description="Desc2",
            code_value="002",
            code_text="Code Text 2",
        ),
    ]

    df = datadok_extract.codelist_to_df(codelist)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.iloc[0]["context_id"] == "1"
    assert df.iloc[1]["code_value"] == "002"
    assert df.iloc[0]["codelist_title"] == "Title1"


def test_date_parser():
    assert datadok_extract.date_parser("20240101", "%Y%m%d") == datetime(2024, 1, 1)
    assert datadok_extract.date_parser("240101", "%y%m%d") == datetime(2024, 1, 1)
    assert pd.isna(datadok_extract.date_parser("invalid-date", "%Y%m%d"))


def test_codelist_to_dict():
    # Test with non-empty DataFrame
    data = {
        "codelist_title": ["Title1", "Title1", "Title2"],
        "code_value": ["001", "002", "003"],
        "code_text": ["Text1", "Text2", "Text3"],
    }
    df = pd.DataFrame(data)
    result = datadok_extract.codelist_to_dict(df)

    assert isinstance(result, dict)
    assert "Title1" in result
    assert result["Title1"]["001"] == "Text1"
    assert result["Title2"]["003"] == "Text3"

    # Test with empty DataFrame
    df_empty = pd.DataFrame(columns=["codelist_title", "code_value", "code_text"])
    result_empty = datadok_extract.codelist_to_dict(df_empty)

    assert result_empty == {}


def test_convert_dates():
    data = {
        "title": ["date_col1", "date_col2"],
        "length": [8, 6],
        "datatype": ["Dato1", "Dato2"],
    }
    metadata_df = pd.DataFrame(data)

    df_data = {"date_col1": ["20240101", "20231231"], "date_col2": ["010124", "311223"]}
    df = pd.DataFrame(df_data)

    df, metadata_df = datadok_extract.convert_dates(df, metadata_df)

    assert pd.api.types.is_datetime64_any_dtype(df["date_col1"])
    assert pd.api.types.is_datetime64_any_dtype(df["date_col2"])


def test_extract_codelist():
    xml_data = """
    <root xmlns:meta="http://www.ssb.no/ns/meta" xmlns:codelist="http://www.ssb.no/ns/meta/codelist">
        <meta:ContextVariable id="1">
            <codelist:Codelist>
                <codelist:CodelistMeta>
                    <codelist:Title>Codelist Title 1</codelist:Title>
                    <codelist:Description>Codelist Description 1</codelist:Description>
                </codelist:CodelistMeta>
                <codelist:Codes>
                    <codelist:Code>
                        <codelist:CodeValue>001</codelist:CodeValue>
                        <codelist:CodeText>Code Text 1</codelist:CodeText>
                    </codelist:Code>
                    <codelist:Code>
                        <codelist:CodeValue>002</codelist:CodeValue>
                        <codelist:CodeText>Code Text 2</codelist:CodeText>
                    </codelist:Code>
                </codelist:Codes>
            </codelist:Codelist>
        </meta:ContextVariable>
        <meta:ContextVariable id="2">
            <codelist:Codelist>
                <codelist:CodelistMeta>
                    <codelist:Title>Codelist Title 2</codelist:Title>
                    <codelist:Description>Codelist Description 2</codelist:Description>
                </codelist:CodelistMeta>
                <codelist:Codes>
                    <codelist:Code>
                        <codelist:CodeValue>003</codelist:CodeValue>
                        <codelist:CodeText>Code Text 3</codelist:CodeText>
                    </codelist:Code>
                </codelist:Codes>
            </codelist:Codelist>
        </meta:ContextVariable>
    </root>
    """
    root = ET.fromstring(xml_data)
    codelist_data = datadok_extract.extract_codelist(root)

    assert len(codelist_data) == 3
    assert codelist_data[0].context_id == "1"
    assert codelist_data[0].codelist_title == "Codelist Title 1"
    assert codelist_data[0].codelist_description == "Codelist Description 1"
    assert codelist_data[0].code_value == "001"
    assert codelist_data[0].code_text == "Code Text 1"

    assert codelist_data[2].context_id == "2"
    assert codelist_data[2].codelist_title == "Codelist Title 2"
    assert codelist_data[2].codelist_description == "Codelist Description 2"
    assert codelist_data[2].code_value == "003"
    assert codelist_data[2].code_text == "Code Text 3"


def test_extract_codelist_missing_elements():
    xml_data_missing = """
    <root xmlns:meta="http://www.ssb.no/ns/meta" xmlns:codelist="http://www.ssb.no/ns/meta/codelist">
        <meta:ContextVariable id="1">
            <codelist:Codelist>
                <codelist:CodelistMeta>
                    <!-- Missing title and description -->
                </codelist:CodelistMeta>
                <codelist:Codes>
                    <codelist:Code>
                        <!-- Missing code value and code text -->
                    </codelist:Code>
                </codelist:Codes>
            </codelist:Codelist>
        </meta:ContextVariable>
    </root>
    """
    root = ET.fromstring(xml_data_missing)
    codelist_data = datadok_extract.extract_codelist(root)

    # Since the codelist is missing critical elements, the result should be empty
    assert len(codelist_data) == 0


def test_handle_decimals_with_comma():
    # Test case with comma as the decimal separator
    data = {"col1": ["1,23", "4,56", "7,89"], "col2": ["10,00", "20,50", "30,75"]}
    df = pd.DataFrame(data)

    metadata_data = {
        "title": ["col1", "col2"],
        "datatype": ["Desimaltall", "Desimaltall"],
        "precision": [None, None],  # Precision not used in this case
    }
    metadata_df = pd.DataFrame(metadata_data)

    df, metadata_df = datadok_extract.handle_decimals(df, metadata_df)

    assert pd.api.types.is_float_dtype(df["col1"])
    assert pd.api.types.is_float_dtype(df["col2"])
    assert math.isclose(df["col1"].iloc[0], 1.23)
    assert math.isclose(df["col2"].iloc[1], 20.50)
    assert metadata_df.loc[metadata_df["title"] == "col1", "type"].iloc[0] == "Float64"


def test_handle_decimals_with_period():
    # Test case with period as the decimal separator
    data = {"col1": ["1.23", "4.56", "7.89"], "col2": ["10.00", "20.50", "30.75"]}
    df = pd.DataFrame(data)

    metadata_data = {
        "title": ["col1", "col2"],
        "datatype": ["Desimaltall", "Desimaltall"],
        "precision": [None, None],  # Precision not used in this case
    }
    metadata_df = pd.DataFrame(metadata_data)

    df, metadata_df = datadok_extract.handle_decimals(df, metadata_df)

    assert pd.api.types.is_float_dtype(df["col1"])
    assert df["col2"].dtype == "Float64"
    assert math.isclose(df["col1"].iloc[0], 1.23)
    assert math.isclose(df["col2"].iloc[1], 20.50)
    assert metadata_df.loc[metadata_df["title"] == "col1", "type"].iloc[0] == "Float64"


def test_handle_decimals_with_no_separator():
    # Test case with no separator and precision in metadata
    data = {"col1": ["123", "456", "789"], "col2": ["1000", "2050", "3075"]}
    df = pd.DataFrame(data)

    metadata_data = {
        "title": ["col1", "col2"],
        "datatype": ["Desimaltall", "Desimaltall"],
        "precision": [2, 2],  # Precision indicates 2 decimal places
    }
    metadata_df = pd.DataFrame(metadata_data)

    df, metadata_df = datadok_extract.handle_decimals(df, metadata_df)

    assert pd.api.types.is_float_dtype(df["col1"])
    assert pd.api.types.is_float_dtype(df["col2"])
    assert math.isclose(df["col1"].iloc[0], 1.23)  # 123 -> 1.23 based on precision
    assert math.isclose(df["col2"].iloc[1], 20.50)  # 2050 -> 20.50 based on precision
    assert metadata_df.loc[metadata_df["title"] == "col1", "type"].iloc[0] == "Float64"


def test_handle_decimals_with_empty_metadata():
    # Test case with empty metadata DataFrame
    data = {"col1": ["1,23", "4,56", "7,89"]}
    df = pd.DataFrame(data)

    metadata_df = pd.DataFrame(columns=["title", "datatype", "precision"])

    df, metadata_df = datadok_extract.handle_decimals(df, metadata_df)

    # The dataframe should remain unchanged since no metadata information is provided
    assert df["col1"].iloc[0] == "1,23"  # No change
    assert df["col1"].dtype == "object"  # No type change


# Mocking the is_valid_url function to return True or False as needed
@patch("requests.get")
@patch("builtins.open", new_callable=mock_open, read_data="<root></root>")
@patch("pandas.read_fwf")
def test_import_archive_data_with_url(mock_read_fwf, mock_open, mock_requests_get):
    # Mocking the XML response from the URL
    mock_requests_get.return_value.text = """
    <root xmlns:meta="http://www.ssb.no/ns/meta" xmlns:common="http://www.ssb.no/ns/meta/common">
        <meta:ContactInformation>
            <common:Division>Test Division</common:Division>
        </meta:ContactInformation>
        <meta:ContextVariable id="1">
            <meta:Title>Variable Title</meta:Title>
            <meta:Description>Variable Description</meta:Description>
            <meta:Properties>
                <meta:Datatype>Tekst</meta:Datatype>
                <meta:Length>10</meta:Length>
                <meta:StartPosition>1</meta:StartPosition>
            </meta:Properties>
        </meta:ContextVariable>
    </root>
    """

    # Mocking the DataFrame returned by pd.read_fwf
    mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    mock_read_fwf.return_value = mock_df

    # Import the function from the module where it is defined
    archive_data = datadok_extract.import_archive_data(
        archive_desc_xml="http://example.com/mock.xml", archive_file="mock_archive.txt"
    )

    # Check if requests.get was called
    mock_requests_get.assert_called_once_with("http://example.com/mock.xml")

    # Check if pd.read_fwf was called with the correct parameters
    mock_read_fwf.assert_called_once()
    assert isinstance(archive_data.df, pd.DataFrame)
    assert archive_data.df.equals(mock_df)

    # Ensure metadata DataFrame and codelist DataFrame are not empty
    assert not archive_data.metadata_df.empty
    assert archive_data.metadata_df["title"].iloc[0] == "Variable Title"


@patch("requests.get")
@patch("builtins.open", new_callable=mock_open, read_data="<root></root>")
@patch("pandas.read_fwf")
def test_import_archive_data_with_file(mock_read_fwf, mock_open, mock_requests_get):
    # Simulate the file-based XML content with ContactInformation element
    xml_content = """
    <root xmlns:meta="http://www.ssb.no/ns/meta" xmlns:common="http://www.ssb.no/ns/meta/common">
        <meta:ContactInformation>
            <common:Division>Test Division</common:Division>
        </meta:ContactInformation>
        <meta:ContextVariable id="1">
            <meta:Title>Variable Title</meta:Title>
            <meta:Description>Variable Description</meta:Description>
            <meta:Properties>
                <meta:Datatype>Tekst</meta:Datatype>
                <meta:Length>10</meta:Length>
                <meta:StartPosition>1</meta:StartPosition>
            </meta:Properties>
        </meta:ContextVariable>
    </root>
    """
    mock_open.return_value.read.return_value = xml_content

    # Mocking the DataFrame returned by pd.read_fwf
    mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    mock_read_fwf.return_value = mock_df

    # Import the function from the module where it is defined
    archive_data = datadok_extract.import_archive_data(
        archive_desc_xml="mock.xml", archive_file="mock_archive.txt"
    )

    # Check if the file was opened
    mock_open.assert_called_once_with("mock.xml")

    # Check if pd.read_fwf was called with the correct parameters
    mock_read_fwf.assert_called_once()
    assert isinstance(archive_data.df, pd.DataFrame)
    assert archive_data.df.equals(mock_df)

    # Ensure metadata DataFrame and codelist DataFrame are not empty
    assert not archive_data.metadata_df.empty
    assert archive_data.metadata_df["title"].iloc[0] == "Variable Title"


@patch("requests.get")
@patch("builtins.open", new_callable=mock_open, read_data="<root></root>")
@patch("pandas.read_fwf")
def test_import_archive_data_with_invalid_params(
    mock_read_fwf, mock_open, mock_requests_get
):
    # Simulate the file-based XML content with ContactInformation element
    xml_content = """
    <root xmlns:meta="http://www.ssb.no/ns/meta" xmlns:common="http://www.ssb.no/ns/meta/common">
        <meta:ContactInformation>
            <common:Division>Test Division</common:Division>
        </meta:ContactInformation>
        <meta:ContextVariable id="1">
            <meta:Title>Variable Title</meta:Title>
            <meta:Description>Variable Description</meta:Description>
            <meta:Properties>
                <meta:Datatype>Tekst</meta:Datatype>
                <meta:Length>10</meta:Length>
                <meta:StartPosition>1</meta:StartPosition>
            </meta:Properties>
        </meta:ContextVariable>
    </root>
    """
    mock_open.return_value.read.return_value = xml_content

    # Now, we expect a ValueError related to the dtype parameter
    with pytest.raises(ValueError, match="You cannot pass dtype to pandas.fwf"):
        datadok_extract.import_archive_data(
            archive_desc_xml="mock.xml",
            archive_file="mock_archive.txt",
            dtype={
                "col1": "Int64"
            },  # Passing an invalid parameter that should be overridden
        )


def test_import_archive_data_parse_error():
    # Test case when the XML cannot be parsed
    invalid_xml_content = "<root><invalid></root>"

    with patch("builtins.open", mock_open(read_data=invalid_xml_content)):
        with pytest.raises(ET.ParseError):
            datadok_extract.import_archive_data(
                archive_desc_xml="https://mock.xml", archive_file="mock_archive.txt"
            )


@patch("fagfunksjoner.data.datadok_extract.linux_shortcuts")
@patch("fagfunksjoner.data.datadok_extract.get_key_by_value")
def test_get_path_combinations_basic(mock_get_key_by_value, mock_linux_shortcuts):
    # Mock the linux_shortcuts to return an empty dictionary
    utd_path, path, expected = utd_path_expected()
    mock_linux_shortcuts.return_value = {"UTD": utd_path}
    mock_get_key_by_value.return_value = "UTD"
    utd_path, path, expected = utd_path_expected()
    result = datadok_extract.get_path_combinations(path)
    assert sorted(result) == sorted(expected)


@patch("fagfunksjoner.data.datadok_extract.linux_shortcuts")
@patch("fagfunksjoner.data.datadok_extract.get_key_by_value")
def test_get_path_combinations_with_dollar(mock_get_key_by_value, mock_linux_shortcuts):
    # Mock the linux_shortcuts to return a dictionary mapping
    utd_path, path, expected = utd_path_expected()
    mock_linux_shortcuts.return_value = {"UTD": utd_path}
    mock_get_key_by_value.return_value = "UTD"
    result = datadok_extract.get_path_combinations(path)
    assert sorted(result) == sorted(expected)


def utd_path_expected() -> tuple[str, str, list[tuple[str, str]]]:
    utd_path = Path("/ssb/stamme01/utd")
    path = Path("/ssb/stamme01/utd/path/to/file")
    expected = [
        ("/ssb/stamme01/utd/path/to/file", ""),
        ("/ssb/stamme01/utd/path/to/file", ".dat"),
        ("/ssb/stamme01/utd/path/to/file", ".txt"),
        ("/ssb/stamme01/utd_pii/path/to/file", ""),
        ("/ssb/stamme01/utd_pii/path/to/file", ".dat"),
        ("/ssb/stamme01/utd_pii/path/to/file", ".txt"),
        ("$UTD/path/to/file", ""),
        ("$UTD/path/to/file", ".dat"),
        ("$UTD/path/to/file", ".txt"),
        ("$UTD_PII/path/to/file", ""),
        ("$UTD_PII/path/to/file", ".dat"),
        ("$UTD_PII/path/to/file", ".txt"),
    ]
    expected = [(Path(p[0]), p[1],) for p in expected]
    return utd_path, path, expected

@patch("fagfunksjoner.data.datadok_extract.linux_shortcuts")
@patch("fagfunksjoner.data.dicts.get_key_by_value")
def test_get_path_combinations_with_non_dollar(
    mock_get_key_by_value, mock_linux_shortcuts
):
    # Mock the linux_shortcuts to return a dictionary mapping
    utd_path, path, expected = utd_path_expected()
    mock_linux_shortcuts.return_value = {"UTD": utd_path}
    mock_get_key_by_value.return_value = "UTD"
    # Convert the paths to be OS-independent using os.path.join
    result = datadok_extract.get_path_combinations(path)
    assert sorted(result) == sorted(expected)



@patch("fagfunksjoner.data.datadok_extract.linux_shortcuts")
@patch("fagfunksjoner.data.datadok_extract.get_key_by_value")
def test_get_path_combinations_with_invalid_dollar_key(
    mock_get_key_by_value, mock_linux_shortcuts
):
    # Mock the linux_shortcuts to return an empty dictionary
    mock_get_key_by_value.return_value = 10
    mock_linux_shortcuts.return_value = {10: "invalid/path/to"}

    with pytest.raises(
        TypeError,
        match="What we got out of the dollar-linux file was not a single string",
    ):
        datadok_extract.get_path_combinations("invalid/path/to/file")



@patch("fagfunksjoner.data.datadok_extract.linux_shortcuts")
@patch("fagfunksjoner.data.datadok_extract.get_key_by_value")
@patch("fagfunksjoner.data.datadok_extract.import_archive_data")
@patch("fagfunksjoner.data.datadok_extract.test_url")
def test_file_with_ark_extension(mock_test_url, mock_import_archive_data, mock_get_key_by_value, mock_linux_shortcuts):
    utd_path, path, expected = utd_path_expected()
    mock_linux_shortcuts.return_value = {"UTD": utd_path}
    mock_get_key_by_value.return_value = "UTD"
    mock_import_archive_data.return_value = pd.DataFrame()
    mock_test_url.return_value = True

    with tempfile.TemporaryDirectory() as tmpdirname:
        ark_file = Path(tmpdirname, "file.ark")
        ark_file.open('a').close()
        result = datadok_extract.open_path_datadok(ark_file)
        mock_import_archive_data.assert_called_once_with(ANY, ark_file)
        assert isinstance(result, pd.DataFrame)


@patch("fagfunksjoner.data.datadok_extract.linux_shortcuts")
@patch("fagfunksjoner.data.datadok_extract.get_key_by_value")
@patch("fagfunksjoner.data.datadok_extract.import_archive_data")
@patch("fagfunksjoner.data.datadok_extract.test_url")
def test_file_with_ark_extension_finds_dat(mock_test_url, mock_import_archive_data, mock_get_key_by_value, mock_linux_shortcuts):
    utd_path, path, expected = utd_path_expected()
    mock_linux_shortcuts.return_value = {"UTD": utd_path}
    mock_get_key_by_value.return_value = "UTD"
    mock_import_archive_data.return_value = pd.DataFrame()
    mock_test_url.return_value = True

    with tempfile.TemporaryDirectory() as tmpdirname:
        ark_file = Path(tmpdirname, "file.dat")
        ark_file.open('a').close()
        result = datadok_extract.open_path_datadok(ark_file.with_suffix(".ark"))
        mock_import_archive_data.assert_called_once_with(ANY, ark_file)
        assert isinstance(result, pd.DataFrame)