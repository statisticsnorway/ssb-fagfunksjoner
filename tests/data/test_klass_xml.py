from xml.etree import ElementTree

import pandas as pd
import pytest

from fagfunksjoner.data.klass_xml import (
    CORRESPONDENCE_NAMESPACE,
    CORRESPONDENCE_PARAM_COLS,
    VARIANT_NAMESPACE,
    VARIANT_PARAM_COLS,
    format_dates,
    get_klass_xml_column_mapping,
    klass_dataframe_to_xml_codelist,
    klass_dataframe_to_xml_correspondence,
    klass_dataframe_to_xml_variant,
    make_klass_xml_codelist,
    rename_klass_columns,
)


def test_format_dates_valid():
    assert format_dates(["2024-01-01", None, ""]) == ["01.01.2024", "", ""]


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


def test_make_klass_xml_with_empty_optional_lists(tmp_path):
    """Test behavior with empty lists for optional fields."""
    codes = ["A"]
    names_bokmaal = ["A"]
    xml_path = tmp_path / "empty_lists.xml"
    df = make_klass_xml_codelist(
        path=str(xml_path),
        codes=codes,
        names_bokmaal=names_bokmaal,
        names_nynorsk=[],
        names_engelsk=[],
        parent=[],
        shortname_bokmaal=[],
        shortname_nynorsk=[],
        shortname_engelsk=[],
        notes_bokmaal=[],
        notes_nynorsk=[],
        notes_engelsk=[],
        valid_from=[],
        valid_to=[],
    )

    assert xml_path.exists()
    assert df["navn_bokmål"][0] == "A"


def test_make_klass_xml_with_nones(tmp_path):
    """Test behavior when some optional fields are set to None."""
    codes = ["A"]
    names_bokmaal = ["A"]
    xml_path = tmp_path / "nones.xml"

    df = make_klass_xml_codelist(
        path=str(xml_path),
        codes=codes,
        names_bokmaal=names_bokmaal,
        valid_from=None,
        valid_to=None,
        parent=None,
    )

    assert xml_path.exists()
    assert df["gyldig_fra"].iloc[0] == ""
    assert df["forelder"].iloc[0] == ""


def test_make_klass_xml_invalid_date_format(tmp_path):
    """Test that invalid date format raises a ValueError."""
    codes = ["1"]
    names_bokmaal = ["One"]
    xml_path = tmp_path / "bad_date.xml"

    with pytest.raises(ValueError, match=r"Invalid date format: not-a-date"):
        make_klass_xml_codelist(
            path=str(xml_path),
            codes=codes,
            names_bokmaal=names_bokmaal,
            valid_from=["not-a-date"],
        )


def test_klass_dataframe_to_xml_unexpected_column(tmp_path):
    """Test klass_dataframe_to_xml_codelist with an unknown column name."""
    df = pd.DataFrame({"kode": ["1"], "unexpected_column": ["oops"]})
    xml_path = tmp_path / "fail.xml"

    with pytest.raises(ValueError, match=r"unexpected_column"):
        klass_dataframe_to_xml_codelist(df, str(xml_path))


def test_make_klass_xml_field_length_mismatch(tmp_path):
    """Test that mismatched field lengths raise ValueError."""
    codes = ["1", "2"]
    names_bokmaal = ["One"]
    xml_path = tmp_path / "mismatch.xml"

    with pytest.raises(ValueError, match=r"Length of the entered names must match"):
        make_klass_xml_codelist(
            path=str(xml_path),
            codes=codes,
            names_bokmaal=names_bokmaal,
        )


def test_klass_dataframe_to_xml_correspondence(tmp_path):
    df = pd.DataFrame(
        {
            "kilde_kode": ["1739", None],
            "kilde_tittel": ["Raarvihke Røyrvik", None],
            "mål_kode": ["1739", "1939"],
            "mål_tittel": ["Røyrvik", "Storfjord"],
        }
    )
    xml_path = tmp_path / "correspondence.xml"

    output_df = klass_dataframe_to_xml_correspondence(df, str(xml_path))

    assert list(output_df.columns) == list(CORRESPONDENCE_PARAM_COLS.values())
    root = ElementTree.parse(xml_path).getroot()
    assert root.tag == f"{{{CORRESPONDENCE_NAMESPACE}}}Korrespondansetabell"
    rows = list(root)
    assert len(rows) == 2
    assert rows[0].tag == f"{{{CORRESPONDENCE_NAMESPACE}}}Korrespondanse"
    assert [child.tag for child in rows[0]] == [
        f"{{{CORRESPONDENCE_NAMESPACE}}}kilde_kode",
        f"{{{CORRESPONDENCE_NAMESPACE}}}kilde_tittel",
        f"{{{CORRESPONDENCE_NAMESPACE}}}mål_kode",
        f"{{{CORRESPONDENCE_NAMESPACE}}}mål_tittel",
    ]

    xml_content = xml_path.read_text(encoding="utf-8")
    assert "<kilde_kode/>" in xml_content
    assert "<kilde_tittel/>" in xml_content
    assert "<index>" not in xml_content


def test_klass_dataframe_to_xml_correspondence_adds_missing_columns(tmp_path):
    df = pd.DataFrame({"kilde_kode": ["01"], "mål_kode": ["02"]})
    xml_path = tmp_path / "correspondence.xml"

    output_df = klass_dataframe_to_xml_correspondence(df, str(xml_path))

    assert list(output_df.columns) == list(CORRESPONDENCE_PARAM_COLS.values())
    xml_content = xml_path.read_text(encoding="utf-8")
    assert "<kilde_tittel/>" in xml_content
    assert "<mål_tittel/>" in xml_content


def test_klass_dataframe_to_xml_correspondence_missing_values_are_empty_tags(tmp_path):
    df = pd.DataFrame(
        {
            "kilde_kode": [None, pd.NA, ""],
            "mål_kode": ["01", "02", "03"],
        }
    )
    xml_path = tmp_path / "correspondence.xml"

    klass_dataframe_to_xml_correspondence(df, str(xml_path))

    xml_content = xml_path.read_text(encoding="utf-8")
    assert xml_content.count("<kilde_kode/>") == 3


def test_klass_dataframe_to_xml_variant(tmp_path):
    df = pd.DataFrame(
        {
            "kode": ["A", None],
            "navn_bokmål": ["Gruppe A", None],
            "kilde_kode": [None, "1.1"],
            "forelder": [None, "A"],
        }
    )
    xml_path = tmp_path / "variant.xml"

    output_df = klass_dataframe_to_xml_variant(df, str(xml_path))

    assert list(output_df.columns) == list(VARIANT_PARAM_COLS.values())
    root = ElementTree.parse(xml_path).getroot()
    assert root.tag == f"{{{VARIANT_NAMESPACE}}}variant"
    rows = list(root)
    assert len(rows) == 2
    assert rows[0].tag == f"{{{VARIANT_NAMESPACE}}}element"
    assert [child.tag for child in rows[0]] == [
        f"{{{VARIANT_NAMESPACE}}}kode",
        f"{{{VARIANT_NAMESPACE}}}navn_bokmål",
        f"{{{VARIANT_NAMESPACE}}}navn_nynorsk",
        f"{{{VARIANT_NAMESPACE}}}navn_engelsk",
        f"{{{VARIANT_NAMESPACE}}}kilde_kode",
        f"{{{VARIANT_NAMESPACE}}}forelder",
    ]

    xml_content = xml_path.read_text(encoding="utf-8")
    assert "<kilde_kode/>" in xml_content
    assert "<kode/>" in xml_content
    assert "<index>" not in xml_content


def test_klass_dataframe_to_xml_variant_requires_code_or_source_code(tmp_path):
    df = pd.DataFrame({"navn_bokmål": ["Gruppe A"]})

    with pytest.raises(ValueError, match=r"kode.*kilde_kode"):
        klass_dataframe_to_xml_variant(df, str(tmp_path / "variant.xml"))


def test_klass_dataframe_to_xml_variant_requires_name_for_own_code(tmp_path):
    df = pd.DataFrame({"kode": ["A"]})

    with pytest.raises(ValueError, match="name in at least one language"):
        klass_dataframe_to_xml_variant(df, str(tmp_path / "variant.xml"))


def test_klass_dataframe_to_xml_variant_reference_does_not_require_name(tmp_path):
    df = pd.DataFrame({"kilde_kode": ["1.1"]})
    xml_path = tmp_path / "variant.xml"

    output_df = klass_dataframe_to_xml_variant(df, str(xml_path))

    assert output_df.loc[0, "kilde_kode"] == "1.1"
    assert xml_path.exists()


def test_klass_dataframe_to_xml_rejects_unknown_column(tmp_path):
    df = pd.DataFrame(
        {
            "kilde_kode": ["1739"],
            "mål_kode": ["1739"],
            "kommentar": ["helper"],
        }
    )

    with pytest.raises(ValueError, match="kommentar"):
        klass_dataframe_to_xml_correspondence(df, str(tmp_path / "correspondence.xml"))


def test_get_klass_xml_column_mapping():
    codelist_nn = get_klass_xml_column_mapping(klass_type="codelist", language="nn")
    assert codelist_nn["code"] == "kode"
    assert codelist_nn["name"] == "navn_nynorsk"
    assert codelist_nn["shortName"] == "kortnavn_nynorsk"
    assert codelist_nn["notes"] == "noter_nynorsk"

    variant_en = get_klass_xml_column_mapping(klass_type="variant", language="en")
    assert variant_en["name"] == "navn_engelsk"
    assert variant_en["sourceCode"] == "kilde_kode"

    corr_map = get_klass_xml_column_mapping(klass_type="correspondence")
    assert corr_map == {
        "sourceCode": "kilde_kode",
        "sourceName": "kilde_tittel",
        "targetCode": "mål_kode",
        "targetName": "mål_tittel",
    }


def test_get_klass_xml_column_mapping_invalid_args():
    with pytest.raises(ValueError, match="Unsupported language"):
        get_klass_xml_column_mapping(language="de")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Unsupported klass_type"):
        get_klass_xml_column_mapping(klass_type="other")  # type: ignore[arg-type]


def test_rename_klass_columns_codelist_and_export(tmp_path):
    raw_df = pd.DataFrame(
        {
            "code": ["0301", "1103"],
            "parentCode": [None, None],
            "level": ["1", "1"],
            "levelName": ["Kommune", "Kommune"],
            "name": ["Oslo", "Stavanger"],
            "shortName": ["OSL", "SVG"],
            "presentationName": ["0301 Oslo", "1103 Stavanger"],
            "validFrom": ["2025-01-12", None],
            "validTo": ["2030-12-31", ""],
            "notes": ["Hovedstad", ""],
        }
    )

    prepared_df = rename_klass_columns(raw_df, klass_type="codelist", language="nb")
    assert "level" not in prepared_df.columns
    assert "presentationName" not in prepared_df.columns
    assert list(prepared_df["kode"]) == ["0301", "1103"]
    assert list(prepared_df["navn_bokmål"]) == ["Oslo", "Stavanger"]
    assert list(prepared_df["gyldig_fra"]) == ["12.01.2025", ""]
    assert list(prepared_df["gyldig_til"]) == ["31.12.2030", ""]

    xml_path = tmp_path / "codelist_from_klass.xml"
    output_df = klass_dataframe_to_xml_codelist(prepared_df, str(xml_path))
    assert xml_path.exists()
    assert output_df.loc[0, "gyldig_fra"] == "12.01.2025"


def test_rename_klass_columns_keep_extra_and_unformatted_dates():
    raw_df = pd.DataFrame(
        {
            "code": ["01"],
            "name": ["Oslo"],
            "level": ["1"],
            "validFrom": ["2025-01-12"],
        }
    )

    df_kept = rename_klass_columns(
        raw_df,
        klass_type="codelist",
        drop_extra_cols=False,
        format_date_cols=False,
    )
    assert "level" in df_kept.columns
    assert df_kept.loc[0, "gyldig_fra"] == "2025-01-12"

    df_no_dates = rename_klass_columns(
        pd.DataFrame({"code": ["01"], "name": ["Oslo"]}),
        klass_type="codelist",
    )
    assert list(df_no_dates.columns) == ["kode", "navn_bokmål"]


def test_rename_klass_columns_variant_and_correspondence(tmp_path):
    raw_variant = pd.DataFrame(
        {
            "code": ["A", None],
            "name": ["Gruppe A", None],
            "sourceCode": [None, "1.1"],
            "parentCode": [None, "A"],
            "level": ["1", "2"],
        }
    )
    variant_df = rename_klass_columns(raw_variant, klass_type="variant", language="nb")
    variant_xml = tmp_path / "variant_from_klass.xml"
    klass_dataframe_to_xml_variant(variant_df, str(variant_xml))
    assert variant_xml.exists()

    raw_corr = pd.DataFrame(
        {
            "sourceCode": ["0301"],
            "sourceName": ["Oslo"],
            "sourceShortName": [""],
            "targetCode": ["03"],
            "targetName": ["Oslo fylke"],
            "targetShortName": [""],
            "validFrom": ["2024-01-01"],
        }
    )
    corr_df = rename_klass_columns(raw_corr, klass_type="correspondence")
    corr_xml = tmp_path / "corr_from_klass.xml"
    klass_dataframe_to_xml_correspondence(corr_df, str(corr_xml))
    assert corr_xml.exists()


def test_klass_dataframe_to_xml_gives_headsup_on_klass_python_columns(tmp_path):
    raw_codelist = pd.DataFrame({"code": ["01"], "name": ["Oslo"]})
    with pytest.raises(
        ValueError,
        match=r"ssb-klass-python.*rename_klass_columns\(df, klass_type=\"codelist\"\)",
    ):
        klass_dataframe_to_xml_codelist(raw_codelist, str(tmp_path / "codelist.xml"))

    raw_corr = pd.DataFrame({"sourceCode": ["01"], "targetCode": ["02"]})
    with pytest.raises(
        ValueError,
        match=r"ssb-klass-python.*rename_klass_columns\(df, klass_type=\"correspondence\"\)",
    ):
        klass_dataframe_to_xml_correspondence(raw_corr, str(tmp_path / "corr.xml"))


def test_klass_dataframe_to_xml_codelist_rejects_unformatted_date(tmp_path):
    df = pd.DataFrame(
        {
            "kode": ["01"],
            "navn_bokmål": ["Oslo"],
            "gyldig_fra": ["2025-01-12"],
        }
    )
    with pytest.raises(
        ValueError,
        match=r"Invalid date format in column 'gyldig_fra': '2025-01-12'.*dd\.MM\.yyyy.*rename_klass_columns",
    ):
        klass_dataframe_to_xml_codelist(df, str(tmp_path / "codelist.xml"))
