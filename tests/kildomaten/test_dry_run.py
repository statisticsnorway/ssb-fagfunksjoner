import logging
from pathlib import Path
from shutil import rmtree
from typing import ClassVar

import pandas as pd
import pytest
from pydantic import ValidationError

import fagfunksjoner.kildomaten.pseudo as pseudo_module
import fagfunksjoner.kildomaten.whodat as whodat_module
from fagfunksjoner.kildomaten import (
    FileConfig,
    WhodatSearchStrategy,
    run_kildomaten_pipeline,
)
from fagfunksjoner.kildomaten.fileconfig import WHODAT_VARIABLES
from fagfunksjoner.kildomaten.validate import assert_prepped_input
from fagfunksjoner.kildomaten.whodat import should_run_whodat, whodat_lookup_fnr

LOCAL_TMP = Path("tests/kildomaten/.tmp")


class ServiceCallError(AssertionError):
    """Raised when a dry-run unexpectedly calls an external service."""


class ExplodingService:
    @classmethod
    def from_pandas(cls, *args, **kwargs):
        raise ServiceCallError("External service should not be called in dry-run")


class FakeInvalidFnrValidator:
    def __init__(self, df):
        """Store the DataFrame that the fake validator should return."""
        self.df = df

    @classmethod
    def from_pandas(cls, df):
        return cls(df)

    def on_field(self, field):
        return self

    def validate_map_to_stable_id(self):
        return self

    def to_pandas(self):
        return self.df.copy()


class FakeWhodatResult:
    details: ClassVar[list[object]] = []

    def to_dict_from_original_indices(self):
        return {
            0: "11111111111",
            1: None,
            2: "",
            3: pd.NA,
        }


class FakeWhodatProcess:
    @classmethod
    def from_pandas(cls, df):
        return cls()

    def search_fnr(self):
        return self

    def with_search_strategy(self, **kwargs):
        return self

    def run(self):
        return FakeWhodatResult()


def test_file_config_validates_user_supplied_whodat_columns():
    documented_variables = {
        "navn",
        "kjoenn",
        "foedselsdato",
        "foedselsaarFraOgMed",
        "foedselsaarTilOgMed",
        "adressenavn",
        "husnummer",
        "postnummer",
        "kommunenummer",
        "fylkesnummer",
    }

    assert WHODAT_VARIABLES == documented_variables
    config = FileConfig(
        fnr_col="fnr",
        pseudo_cols=["fnr"],
        use_fnrsearch=True,
        fnrsearch_cols=list(documented_variables),
    )
    assert config.whodat_columns == documented_variables

    with pytest.raises(ValidationError, match="Unsupported WhoDat variables"):
        FileConfig(
            fnr_col="fnr",
            pseudo_cols=["fnr"],
            use_fnrsearch=True,
            fnrsearch_cols=["navn", "not_a_whodat_column"],
        )


def test_file_config_rejects_string_where_list_is_expected():
    with pytest.raises(ValidationError, match="lists, not strings"):
        FileConfig(fnr_col="fnr", pseudo_cols="fnr")


def test_file_config_treats_blank_fnr_col_as_unconfigured():
    config = FileConfig(fnr_col="", pseudo_cols=[])

    assert config.fnr_col is None
    assert config.person_columns == set()


def test_file_config_rejects_unknown_field_names():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        FileConfig(
            fnr_col="fnr",
            pseudo_cols=["fnr"],
            unknown_setting=True,
        )


def test_whodat_search_strategy_validates_options_and_variables():
    strategy = WhodatSearchStrategy(
        variables=["navn", "navn", "foedselsaarFraOgMed"],
        inkluder_oppholdsadresse=True,
        soek_fonetisk=True,
        inkluder_doede=True,
        opplysningsgrunnlag="historisk",
    )

    assert strategy.variables == ["navn", "foedselsaarFraOgMed"]
    assert strategy.inkluder_oppholdsadresse is True
    assert strategy.soek_fonetisk is True
    assert strategy.inkluder_doede is True
    assert strategy.opplysningsgrunnlag == "historisk"

    with pytest.raises(ValidationError):
        WhodatSearchStrategy(
            variables=["navn"],
            opplysningsgrunnlag="not-supported",
        )


def test_dataframe_dry_run_does_not_require_output_path_or_call_services(monkeypatch):
    monkeypatch.setattr(whodat_module, "Validator", ExplodingService)
    monkeypatch.setattr(whodat_module, "Whodat", ExplodingService)
    monkeypatch.setattr(pseudo_module, "Pseudonymize", ExplodingService)

    result = run_kildomaten_pipeline(
        pd.DataFrame(
            {
                "fnr": ["not-valid", None],
                "navn": ["Test Person", None],
            }
        ),
        FileConfig(
            fnr_col="fnr",
            pseudo_cols=["fnr"],
            use_fnrsearch=True,
            fnrsearch_cols=["navn"],
        ),
        dry_run=True,
    )

    assert isinstance(result, pd.DataFrame)
    assert result.loc[0, "fnr"] == "not-valid"
    assert pd.isna(result.loc[1, "fnr"])
    assert not Path("dry_run_output.parquet").exists()


def test_dataframe_non_dry_run_requires_output_path():
    df = pd.DataFrame({"kode": ["a"]})
    file_config = FileConfig()

    with pytest.raises(ValueError, match="output_path is required"):
        run_kildomaten_pipeline(df, file_config)


def test_path_dry_run_reads_parquet_and_returns_derived_output_path():
    rmtree(LOCAL_TMP, ignore_errors=True)
    LOCAL_TMP.mkdir(parents=True)
    source_path = LOCAL_TMP / "resultat_kilde_p2024.parquet"

    try:
        pd.DataFrame({"kode": ["a"], "verdi": [1]}).to_parquet(source_path)

        result = run_kildomaten_pipeline(source_path, FileConfig(), dry_run=True)

        assert isinstance(result, pd.DataFrame)
        assert result["kode"].tolist() == ["a"]
        assert not (LOCAL_TMP / "resultat_inndata_p2024.parquet").exists()
    finally:
        rmtree(LOCAL_TMP, ignore_errors=True)


def test_path_input_must_be_parquet_even_in_dry_run():
    rmtree(LOCAL_TMP, ignore_errors=True)
    LOCAL_TMP.mkdir(parents=True)
    csv_path = LOCAL_TMP / "input.csv"
    file_config = FileConfig()

    try:
        csv_path.write_text("a,b\n1,2\n", encoding="utf-8")

        with pytest.raises(ValueError, match="Expected a parquet file"):
            run_kildomaten_pipeline(csv_path, file_config, dry_run=True)
    finally:
        rmtree(LOCAL_TMP, ignore_errors=True)


def test_dry_run_applies_preprocess_rename_and_drop_logic_without_writing():
    def preprocess(df):
        out = df.copy()
        out["navn"] = out["first"] + " " + out["last"]
        return out

    rmtree(LOCAL_TMP, ignore_errors=True)
    LOCAL_TMP.mkdir(parents=True)
    output_path = LOCAL_TMP / "should_not_be_written.parquet"

    try:
        result = run_kildomaten_pipeline(
            pd.DataFrame(
                {
                    "foedselsnummer": ["bad"],
                    "first": ["Ada"],
                    "last": ["Lovelace"],
                    "sensitive_name": ["Ada Lovelace"],
                }
            ),
            FileConfig(
                fnr_col="fnr",
                pseudo_cols=["fnr"],
                use_fnrsearch=True,
                fnrsearch_cols=["navn"],
                rename_map={"foedselsnummer": "fnr"},
                drop_cols=["sensitive_name"],
                preprocess_func=preprocess,
                output_path=output_path,
            ),
            dry_run=True,
        )

        assert isinstance(result, pd.DataFrame)
        assert "sensitive_name" not in result.columns
        assert result["fnr"].tolist() == ["bad"]
        assert not output_path.exists()
    finally:
        rmtree(LOCAL_TMP, ignore_errors=True)


def test_pipeline_logs_missing_configured_action_columns_in_dry_run(caplog):
    caplog.set_level(logging.WARNING)

    result = run_kildomaten_pipeline(
        pd.DataFrame(
            {
                "fnr": ["12345678901"],
                "navn": ["Test Person"],
            }
        ),
        FileConfig(
            fnr_col="fnr",
            pseudo_cols=["fnr"],
            rename_map={"missing_rename_source": "renamed"},
            copy_cols_new_old={"copied": "missing_copy_source"},
            drop_cols=["missing_drop_col", "missing_extra_drop_col"],
        ),
        dry_run=True,
    )

    assert isinstance(result, pd.DataFrame)
    assert "copied" not in result.columns
    messages = [record.getMessage() for record in caplog.records]
    assert any("rename_map source columns are missing" in msg for msg in messages)
    assert any("missing_rename_source" in msg for msg in messages)
    assert any("copy_cols_new_old source column is missing" in msg for msg in messages)
    assert any("missing_copy_source -> copied" in msg for msg in messages)
    assert any("Configured drop_cols are missing" in msg for msg in messages)
    assert any("missing_drop_col" in msg for msg in messages)
    assert any("missing_extra_drop_col" in msg for msg in messages)


def test_pipeline_warns_and_skips_copy_when_target_column_exists(caplog):
    caplog.set_level(logging.WARNING)

    result = run_kildomaten_pipeline(
        pd.DataFrame(
            {
                "fnr": ["existing-fnr"],
                "source_fnr": ["source-fnr"],
            }
        ),
        FileConfig(
            fnr_col="fnr",
            pseudo_cols=["fnr"],
            copy_cols_new_old={"fnr": "source_fnr"},
        ),
        dry_run=True,
    )

    assert isinstance(result, pd.DataFrame)
    assert result["fnr"].tolist() == ["existing-fnr"]
    assert any(
        "target column already exists" in record.getMessage()
        and "source_fnr -> fnr" in record.getMessage()
        for record in caplog.records
    )


def test_input_validation_fails_when_configured_fnr_col_is_missing(caplog):
    caplog.set_level(logging.ERROR)
    df = pd.DataFrame({"not_fnr": ["12345678901"]})
    file_config = FileConfig(fnr_col="fnr", pseudo_cols=[])

    with pytest.raises(AssertionError, match="Missing configured person columns"):
        assert_prepped_input(df, file_config)

    assert any(
        "Configured person columns are missing from input" in record.getMessage()
        and "fnr" in record.getMessage()
        for record in caplog.records
    )


def test_input_validation_logs_missing_required_pseudo_columns(caplog):
    caplog.set_level(logging.ERROR)
    df = pd.DataFrame({"fnr": ["12345678901"]})
    file_config = FileConfig(fnr_col="fnr", pseudo_cols=["missing_pseudo_col"])

    with pytest.raises(AssertionError, match="Missing configured person columns"):
        assert_prepped_input(df, file_config)

    assert any(
        "Configured person columns are missing from input" in record.getMessage()
        and "missing_pseudo_col" in record.getMessage()
        for record in caplog.records
    )


def test_whodat_dry_run_counts_rows_that_would_be_sent_and_skips_blank_pii(
    monkeypatch,
):
    monkeypatch.setattr(whodat_module, "Validator", ExplodingService)
    monkeypatch.setattr(whodat_module, "Whodat", ExplodingService)

    df = pd.DataFrame(
        {
            "fnr": ["bad", "also-bad", "12345678901", None],
            "navn": ["Has Name", None, "Valid Shape", "Has Name"],
        },
        index=[10, 11, 12, 13],
    )

    out, stats = whodat_lookup_fnr(
        df,
        FileConfig(
            fnr_col="fnr",
            pseudo_cols=["fnr"],
            use_fnrsearch=True,
            fnrsearch_cols=["navn"],
        ),
        dry_run=True,
    )

    assert out["fnr"].tolist() == df["fnr"].tolist()
    assert pd.isna(out.loc[11, "navn"])
    assert stats["dry_run"] == 1
    assert stats["needs_lookup"] == 3
    assert stats["missing_fnr"] == 1
    assert stats["invalid_fnr"] == 2
    assert stats["skipped_no_searchable"] == 1
    assert stats["to_whodat"] == 2
    assert stats["whodat_hits"] == 0


def test_whodat_resets_index_and_keeps_original_fnr_without_usable_hit(monkeypatch):
    monkeypatch.setattr(whodat_module, "Validator", FakeInvalidFnrValidator)
    monkeypatch.setattr(whodat_module, "Whodat", FakeWhodatProcess)

    df = pd.DataFrame(
        {
            "fnr": ["bad-one", "bad-two", "bad-three", "bad-four"],
            "navn": ["One", "Two", "Three", "Four"],
        },
        index=[10, 11, 12, 13],
    )

    out, stats = whodat_lookup_fnr(
        df,
        FileConfig(
            fnr_col="fnr",
            pseudo_cols=["fnr"],
            use_fnrsearch=True,
            fnrsearch_cols=["navn"],
            max_whodat_share=1.0,
        ),
    )

    assert out["fnr"].tolist() == [
        "11111111111",
        "bad-two",
        "bad-three",
        "bad-four",
    ]
    assert out.index.tolist() == [0, 1, 2, 3]
    assert out["fnr_orig"].tolist() == df["fnr"].tolist()
    assert stats["whodat_hits"] == 1


def test_whodat_dry_run_normalizes_configured_helper_columns():
    df = pd.DataFrame(
        {
            "fnr": ["bad"],
            "navn": ["  Test Person  "],
            "kjoenn": ["1"],
            "foedselsdato": ["1990-01-31"],
            "foedselsaarFraOgMed": [1980],
            "foedselsaarTilOgMed": [" 1999 "],
            "adressenavn": ["  Testveien  "],
            "husnummer": [" 10B "],
            "postnummer": [" 0123 "],
            "kommunenummer": ["0301"],
            "fylkesnummer": ["03"],
        }
    )

    out, stats = whodat_lookup_fnr(
        df,
        FileConfig(
            fnr_col="fnr",
            pseudo_cols=["fnr"],
            use_fnrsearch=True,
            fnrsearch_cols=[
                "navn",
                "kjoenn",
                "foedselsdato",
                "foedselsaarFraOgMed",
                "foedselsaarTilOgMed",
                "adressenavn",
                "husnummer",
                "postnummer",
                "kommunenummer",
                "fylkesnummer",
            ],
        ),
        dry_run=True,
    )

    assert out.loc[0, "navn"] == "Test Person"
    assert out.loc[0, "kjoenn"] == "mann"
    assert out.loc[0, "foedselsdato"] == "19900131"
    assert out.loc[0, "foedselsaarFraOgMed"] == "1980"
    assert out.loc[0, "foedselsaarTilOgMed"] == "1999"
    assert out.loc[0, "adressenavn"] == "Testveien"
    assert out.loc[0, "husnummer"] == "10B"
    assert out.loc[0, "postnummer"] == "0123"
    assert out.loc[0, "kommunenummer"] == "0301"
    assert out.loc[0, "fylkesnummer"] == "03"
    assert stats["to_whodat"] == 1


def test_whodat_strategy_only_config_uses_documented_helper_columns_in_dry_run(
    monkeypatch,
):
    monkeypatch.setattr(whodat_module, "Validator", ExplodingService)
    monkeypatch.setattr(whodat_module, "Whodat", ExplodingService)

    df = pd.DataFrame(
        {
            "fnr": ["bad"],
            "navn": ["Test Person"],
            "adressenavn": ["Testveien"],
            "postnummer": ["0123"],
        }
    )
    config = FileConfig(
        fnr_col="fnr",
        pseudo_cols=["fnr"],
        use_fnrsearch=True,
        fnrsearch_strategies=[
            WhodatSearchStrategy(
                variables=["navn", "adressenavn", "postnummer"],
                inkluder_doede=True,
                inkluder_oppholdsadresse=True,
            )
        ],
        add_relaxed_fnrsearch_strategy=False,
    )

    out, stats = whodat_lookup_fnr(df, config, dry_run=True)

    assert should_run_whodat(out, config) is True
    assert stats["to_whodat"] == 1
