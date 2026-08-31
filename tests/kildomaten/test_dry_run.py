from pathlib import Path
from shutil import rmtree

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
from fagfunksjoner.kildomaten.whodat import should_run_whodat, whodat_lookup_fnr

LOCAL_TMP = Path("tests/kildomaten/.tmp")


class ServiceCallError(AssertionError):
    """Raised when a dry-run unexpectedly calls an external service."""


class ExplodingService:
    @classmethod
    def from_pandas(cls, *args, **kwargs):
        raise ServiceCallError("External service should not be called in dry-run")


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
        bruk_fnrleting=True,
        fnrleting_cols=list(documented_variables),
    )
    assert config.whodat_columns == documented_variables

    with pytest.raises(ValidationError, match="Unsupported WhoDat variables"):
        FileConfig(
            fnr_col="fnr",
            pseudo_cols=["fnr"],
            bruk_fnrleting=True,
            fnrleting_cols=["navn", "not_a_whodat_column"],
        )


def test_file_config_rejects_string_where_list_is_expected():
    with pytest.raises(ValidationError, match="lists, not strings"):
        FileConfig(fnr_col="fnr", pseudo_cols="fnr")


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
            bruk_fnrleting=True,
            fnrleting_cols=["navn"],
        ),
        dry_run=True,
    )

    assert result == Path("dry_run_output.parquet")
    assert not result.exists()


def test_dataframe_non_dry_run_requires_output_path():
    with pytest.raises(ValueError, match="output_path is required"):
        run_kildomaten_pipeline(
            pd.DataFrame({"kode": ["a"]}),
            FileConfig(),
        )


def test_path_dry_run_reads_parquet_and_returns_derived_output_path():
    rmtree(LOCAL_TMP, ignore_errors=True)
    LOCAL_TMP.mkdir(parents=True)
    source_path = LOCAL_TMP / "resultat_kilde_p2024.parquet"

    try:
        pd.DataFrame({"kode": ["a"], "verdi": [1]}).to_parquet(source_path)

        result = run_kildomaten_pipeline(source_path, FileConfig(), dry_run=True)

        assert result == LOCAL_TMP / "resultat_inndata_p2024.parquet"
        assert not result.exists()
    finally:
        rmtree(LOCAL_TMP, ignore_errors=True)


def test_path_input_must_be_parquet_even_in_dry_run():
    rmtree(LOCAL_TMP, ignore_errors=True)
    LOCAL_TMP.mkdir(parents=True)
    csv_path = LOCAL_TMP / "input.csv"

    try:
        csv_path.write_text("a,b\n1,2\n", encoding="utf-8")

        with pytest.raises(ValueError, match="Expected a parquet file"):
            run_kildomaten_pipeline(csv_path, FileConfig(), dry_run=True)
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
                bruk_fnrleting=True,
                fnrleting_cols=["navn"],
                rename_map={"foedselsnummer": "fnr"},
                drop_cols=["sensitive_name"],
                preprocess_func=preprocess,
                output_path=output_path,
            ),
            dry_run=True,
        )

        assert result == output_path
        assert not output_path.exists()
    finally:
        rmtree(LOCAL_TMP, ignore_errors=True)


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
            bruk_fnrleting=True,
            fnrleting_cols=["navn"],
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
            bruk_fnrleting=True,
            fnrleting_cols=[
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
        bruk_fnrleting=True,
        fnrleting_search_strategies=[
            WhodatSearchStrategy(
                variables=["navn", "adressenavn", "postnummer"],
                inkluder_doede=True,
                inkluder_oppholdsadresse=True,
            )
        ],
        add_relaxed_fnrleting_strategy=False,
    )

    out, stats = whodat_lookup_fnr(df, config, dry_run=True)

    assert should_run_whodat(out, config) is True
    assert stats["to_whodat"] == 1
