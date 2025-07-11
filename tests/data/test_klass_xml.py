import pandas as pd
import pytest

from src.fagfunksjoner.data.klass_xml import (
    make_klass_df_codelist,
    make_klass_xml_codelist,
)


def test_make_klass_df_codelist_success():
    """Test successful creation of a DataFrame."""
    codes = [1, 2, 3]
    names_bokmaal = ["Name1", "Name2", "Name3"]
    names_nynorsk = ["Namn1", "Namn2", "Namn3"]
    names_engelsk = ["Name1_EN", "Name2_EN", "Name3_EN"]

    df = make_klass_df_codelist(
        codes=codes,
        names_bokmaal=names_bokmaal,
        names_nynorsk=names_nynorsk,
        names_engelsk=names_engelsk,
    )

    assert isinstance(df, pd.DataFrame)
    assert list(df["kode"]) == codes
    assert list(df["navn_bokmål"]) == names_bokmaal
    assert list(df["navn_nynorsk"]) == names_nynorsk
    assert list(df["navn_engelsk"]) == names_engelsk

    # Make sure columns are in correct spot
    assert ["kode", "forelder", "navn_bokmål", "navn_nynorsk", "navn_engelsk"] == list(
        df.columns[:5]
    )


def test_make_klass_df_codelist_missing_names():
    """Test creation of a DataFrame with missing optional name lists."""
    codes = [1, 2, 3]
    names_bokmaal = ["Name1", "Name2", "Name3"]

    df = make_klass_df_codelist(
        codes=codes,
        names_bokmaal=names_bokmaal,
        names_nynorsk=None,
        names_engelsk=None,
    )

    assert isinstance(df, pd.DataFrame)
    assert list(df["kode"]) == codes
    assert list(df["navn_bokmål"]) == names_bokmaal
    assert df["navn_nynorsk"].isnull().all()
    assert df["navn_engelsk"].isnull().all()


def test_make_klass_df_codelist_missing_required_names():
    """Test that a ValueError is raised when both names_bokmaal and names_nynorsk are None."""
    codes = [1, 2, 3]

    with pytest.raises(
        ValueError, match="Must have content in names_bokmaal or names_nynorsk"
    ):
        make_klass_df_codelist(
            codes=codes,
            names_bokmaal=None,
            names_nynorsk=None,
            names_engelsk=None,
        )


def test_make_klass_df_codelist_mismatched_lengths():
    """Test that a ValueError is raised when codes and names have mismatched lengths."""
    codes = [1, 2, 3]
    names_bokmaal = ["Name1", "Name2"]

    with pytest.raises(
        ValueError, match="Length of the entered names must match the length of codes."
    ):
        make_klass_df_codelist(
            codes=codes,
            names_bokmaal=names_bokmaal,
            names_nynorsk=None,
            names_engelsk=None,
        )


def test_make_klass_xml_codelist_success(tmp_path):
    """Test successful creation of an XML file and DataFrame."""
    codes = [1, 2, 3]
    names_bokmaal = ["Name1", "Name2", "Name3"]
    names_nynorsk = ["Namn1", "Namn2", "Namn3"]
    names_engelsk = ["Name1_EN", "Name2_EN", "Name3_EN"]

    xml_path = tmp_path / "klass.xml"

    df = make_klass_xml_codelist(
        path=str(xml_path),
        codes=codes,
        names_bokmaal=names_bokmaal,
        names_nynorsk=names_nynorsk,
        names_engelsk=names_engelsk,
    )

    # Check that the DataFrame is correct
    assert isinstance(df, pd.DataFrame)
    assert list(df["kode"]) == codes
    assert list(df["navn_bokmål"]) == names_bokmaal
    assert list(df["navn_nynorsk"]) == names_nynorsk
    assert list(df["navn_engelsk"]) == names_engelsk

    # Check that the XML file was created
    assert xml_path.exists()
    with open(xml_path, encoding="utf-8") as f:
        xml_content = f.read()
        assert "xml version=" in xml_content
        assert "<ns1:element>" in xml_content
        assert "Name1" in xml_content
        assert "Namn1" in xml_content
        assert "Name1_EN" in xml_content


def test_make_klass_xml_codelist_missing_required_names(tmp_path):
    """Test that a ValueError is raised when both names_bokmaal and names_nynorsk are None."""
    codes = [1, 2, 3]
    xml_path = tmp_path / "klass.xml"

    with pytest.raises(
        ValueError, match="Must have content in names_bokmaal or names_nynorsk"
    ):
        make_klass_xml_codelist(
            path=str(xml_path),
            codes=codes,
            names_bokmaal=None,
            names_nynorsk=None,
            names_engelsk=None,
        )


def test_make_klass_xml_codelist_all_fields(tmp_path):
    xml_output_path = tmp_path / "full_klass_codelist.xml"

    df = make_klass_xml_codelist(
        path=str(xml_output_path),
        codes=["100", "110", "120"],
        parent=[None, "100", "100"],
        names_bokmaal=["Hovedområde", "Underområde A", "Underområde B"],
        names_nynorsk=["Hovudområde", "Underområde A", "Underområde B"],
        names_engelsk=["Main Area", "Subarea A", "Subarea B"],
        shortname_bokmaal=["HO", "UA", "UB"],
        shortname_nynorsk=["HO", "UA", "UB"],
        shortname_engelsk=["MA", "SA", "SB"],
        notes_bokmaal=[
            "Overordnet kategori",
            "Del av hovedområdet",
            "Del av hovedområdet",
        ],
        notes_nynorsk=[
            "Overordna kategori",
            "Del av hovudområdet",
            "Del av hovudområdet",
        ],
        notes_engelsk=["Top-level category", "Part of main area", "Part of main area"],
        valid_from=[
            "2025-01-01",
            "2025-01-01",
            "2025-01-01",
        ],  # ISO format to test parsing
        valid_to=["2030-12-31", "2030-12-31", "2030-12-31"],
    )

    assert xml_output_path.exists()
    assert isinstance(df, pd.DataFrame)
    assert list(df["gyldig_fra"].unique()) == ["01.01.2025"]
    assert list(df["gyldig_til"].unique()) == ["31.12.2030"]
    assert df["forelder"].iloc[1] == "100"
