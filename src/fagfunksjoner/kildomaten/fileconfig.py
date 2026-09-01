from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

WHODAT_VARIABLES = {
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

DEFAULT_GENDER_VALUES: dict[str, Literal["mann", "kvinne"]] = {
    "1": "mann",
    "m": "mann",
    "mann": "mann",
    "male": "mann",
    "2": "kvinne",
    "k": "kvinne",
    "f": "kvinne",
    "female": "kvinne",
    "kvinne": "kvinne",
}


def _clean_column(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Column names must be strings")
    return value.strip()


def _clean_columns(values: Iterable[object]) -> list[str]:
    return [_clean_column(value) for value in values]


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))


class WhodatSearchStrategy(BaseModel):
    """A WhoDat search strategy for one lookup attempt."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    variables: list[str] = Field(min_length=1)
    inkluder_oppholdsadresse: bool = False
    soek_fonetisk: bool = False
    inkluder_doede: bool = False
    opplysningsgrunnlag: Literal["gjeldende", "historisk"] = "gjeldende"

    @field_validator("variables")
    @classmethod
    def _validate_variables(cls, variables: list[str]) -> list[str]:
        cleaned = _unique(_clean_columns(variables))
        invalid = sorted(set(cleaned) - WHODAT_VARIABLES)
        if invalid:
            raise ValueError(f"Unsupported WhoDat variables: {invalid}")
        return cleaned


class FileConfig(BaseModel):
    """Configuration for processing one source file type.

    `FileConfig` is the user-facing control object for
    `run_kildomaten_pipeline`. It describes how incoming columns should be
    prepared, which columns contain person identifiers, whether missing or
    invalid FNR values should be searched for in WhoDat, how pseudonymization
    should run, and where the final parquet file should be written.

    Column names in this model are stripped when the config is created. Unknown
    config fields are rejected so misspelled settings fail early instead of
    being silently ignored. WhoDat variable names and WhoDat option names are
    intentionally kept in the spelling expected by WhoDat, such as `navn`,
    `kjoenn`, `inkluder_doede`, and `soek_fonetisk`.

    Attributes:
        fnr_col (str | None): Name of the main FNR column. When
            `use_fnrsearch=True`, this column is validated and rows with missing
            or invalid values may be sent to WhoDat. During pseudonymization,
            valid values from this column are used to create stable SNR values.
            If WhoDat finds a replacement FNR, that value replaces the original
            in this column; otherwise the original value is kept, including
            invalid values.
        pseudo_cols (list[str]): Columns that should be encrypted with
            Papis-compatible pseudonymization. The main `fnr_col` can be
            included here when the original FNR column should also be
            pseudonymized. Missing configured columns are logged during runtime
            because they usually indicate that the input data or config has
            changed.
        use_fnrsearch (bool): Enables WhoDat FNR search. Requires `fnr_col` and
            at least one of `fnrsearch_cols` or `fnrsearch_strategies`. When
            false, the pipeline skips WhoDat and proceeds directly to
            pseudonymization when person data is present.
        fnrsearch_cols (list[str]): Ordered list of WhoDat helper variables
            available in the input data. If no explicit `fnrsearch_strategies`
            are configured, the pipeline builds incremental strategies from
            this list, adding one more available helper column at each step.
            Values are normalized to string formats expected by WhoDat before
            lookup.
        fnrsearch_strategies (list[WhodatSearchStrategy]): Explicit WhoDat
            search attempts to run, in order. Use this when the default
            incremental strategy is too broad or too narrow. Strategy variables
            that are not present in the input are omitted at runtime, and
            configured missing helper columns are logged.
        add_relaxed_fnrsearch_strategy (bool): Adds a final broader fallback
            strategy using all available helper columns with
            `inkluder_doede=True` and `soek_fonetisk=True`. This can increase
            hits, but may also widen the search more than desired for some
            datasets.
        snr_col (str): Output column for SNR values. Valid FNR values are mapped
            to stable SNR through the pseudonymization service. Rows without a
            usable stable SNR are filled with UUID-based SNR values.
        snr_mark_col (str): Output boolean marker column. True means the row
            received a UUID-filled SNR rather than a stable SNR from
            pseudonymization.
        rename_map (dict[str, str]): Mapping from existing input column names to
            pipeline column names. This runs after `preprocess_func` and before
            copying columns. Missing source columns are logged at runtime.
        copy_cols_new_old (dict[str, str]): Mapping from new column name to
            existing column name. This is useful when a source column must be
            preserved while also copied into a pipeline-specific name. The
            mapping direction is `{new_col: old_col}`. Missing source columns
            are logged and skipped.
        drop_cols (list[str]): Columns to remove from the final output after
            WhoDat and pseudonymization have run. Missing configured columns are
            logged at runtime because this can indicate config drift.
        preprocess_func (Callable[[pd.DataFrame], pd.DataFrame] | None):
            Optional callable that receives the input DataFrame and returns a
            modified DataFrame before `rename_map` and `copy_cols_new_old` are
            applied. Use this for dataset-specific cleanup that cannot be
            expressed as simple config.
        output_path (Path | None): Explicit parquet output path. Required for
            DataFrame input in non-dry-runs. In dry-runs, the pipeline returns
            the processed DataFrame and does not write this path.
        output_dir (Path | None): Optional output directory used when the input
            is a path and `output_path` is not set. If omitted, the source file
            directory is used.
        output_name_insert (str): Text inserted into the derived output file
            name when the input is a path and `output_path` is not set.
        output_overwrite (bool): Whether derived output paths may reuse an
            existing filename when writing parquet. When false and `output_path`
            is not set, output versioning is based on existing files in
            `output_dir`; avoid concurrent writes to the same output directory
            because version selection is stateful. Explicit `output_path`
            values are used as provided.
        chunk_size (int): Maximum number of rows per WhoDat request chunk. Lower
            this if requests become too large for the service.
        max_whodat_share (float): Maximum share of input rows allowed to be sent
            to WhoDat. If more rows qualify, lookup is skipped as a safety
            measure.
        max_whodat_rows (int): Maximum absolute number of input rows allowed to
            be sent to WhoDat. If more rows qualify, lookup is skipped as a
            safety measure.
        gender_values (dict[str, Literal["mann", "kvinne"]]): Mapping from
            dataset-specific gender values to the WhoDat-required values `mann`
            and `kvinne`. Keys are normalized by stripping spaces and
            lowercasing before use.
        model_config (ConfigDict): Pydantic model settings. Arbitrary callable
            types are allowed for fields such as `preprocess_func`, and unknown
            config fields are rejected.
    """

    fnr_col: str | None = None
    pseudo_cols: list[str] = Field(default_factory=list)
    use_fnrsearch: bool = False
    fnrsearch_cols: list[str] = Field(default_factory=list)
    fnrsearch_strategies: list[WhodatSearchStrategy] = Field(default_factory=list)
    add_relaxed_fnrsearch_strategy: bool = True
    snr_col: str = "snr"
    snr_mark_col: str = "snr_mrk"
    rename_map: dict[str, str] = Field(default_factory=dict)
    copy_cols_new_old: dict[str, str] = Field(default_factory=dict)
    drop_cols: list[str] = Field(default_factory=list)
    preprocess_func: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    output_path: Path | None = None
    output_dir: Path | None = None
    output_name_insert: str = "_inndata_"
    output_overwrite: bool = True
    chunk_size: int = Field(default=250, gt=0)
    max_whodat_share: float = Field(default=0.10, ge=0, le=1)
    max_whodat_rows: int = Field(default=50_000, ge=0)
    gender_values: dict[str, Literal["mann", "kvinne"]] = Field(
        default_factory=lambda: DEFAULT_GENDER_VALUES.copy()
    )
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    @field_validator("fnr_col", mode="before")
    @classmethod
    def _validate_optional_fnr_column(cls, value: object) -> object:
        if value is None:
            return None
        if not isinstance(value, str):
            return value
        cleaned = _clean_column(value)
        return cleaned or None

    @field_validator("snr_col", "snr_mark_col", mode="before")
    @classmethod
    def _validate_column(cls, value: object) -> object:
        if value is None:
            return value
        if not isinstance(value, str):
            return value
        cleaned = _clean_column(value)
        if not cleaned:
            raise ValueError("Column names cannot be blank")
        return cleaned

    @field_validator(
        "pseudo_cols",
        "drop_cols",
        mode="before",
    )
    @classmethod
    def _validate_column_list(cls, values: object) -> object:
        if values is None:
            return []
        if isinstance(values, str):
            raise ValueError("Column collections must be lists, not strings")
        if not isinstance(values, Iterable):
            return values
        return _unique(_clean_columns(values))

    @field_validator("fnrsearch_cols", mode="before")
    @classmethod
    def _validate_fnrsearch_cols(cls, values: object) -> object:
        if values is None:
            return []
        if isinstance(values, str):
            raise ValueError("fnrsearch_cols must be a list, not a string")
        if not isinstance(values, Iterable):
            return values
        cleaned = _unique(_clean_columns(values))
        invalid = sorted(set(cleaned) - WHODAT_VARIABLES)
        if invalid:
            raise ValueError(f"Unsupported WhoDat variables: {invalid}")
        return cleaned

    @field_validator("rename_map", mode="before")
    @classmethod
    def _validate_rename_map(cls, value: object) -> object:
        if value is None:
            return {}
        if not isinstance(value, dict):
            return value
        return {
            _clean_column(str(source)): _clean_column(str(target))
            for source, target in value.items()
        }

    @field_validator("copy_cols_new_old", mode="before")
    @classmethod
    def _validate_copy_cols_new_old(cls, value: object) -> object:
        if value is None:
            return {}
        if not isinstance(value, dict):
            return value
        return {
            _clean_column(str(source)): _clean_column(str(target))
            for source, target in value.items()
        }

    @field_validator("gender_values", mode="before")
    @classmethod
    def _validate_gender_values(cls, value: object) -> object:
        if value is None:
            return DEFAULT_GENDER_VALUES.copy()
        if not isinstance(value, dict):
            return value
        return {
            str(source).strip().lower().replace(" ", ""): target
            for source, target in value.items()
        }

    @model_validator(mode="after")
    def _validate_config(self) -> "FileConfig":
        if self.use_fnrsearch and not self.fnr_col:
            raise ValueError("fnr_col is required when use_fnrsearch=True")
        if self.use_fnrsearch and not (
            self.fnrsearch_cols or self.fnrsearch_strategies
        ):
            raise ValueError(
                "fnrsearch_cols or fnrsearch_strategies is required "
                "when use_fnrsearch=True"
            )
        if self.fnr_col and self.fnr_col in {self.snr_col, self.snr_mark_col}:
            raise ValueError("fnr_col cannot be the same as an output SNR column")
        if self.snr_col == self.snr_mark_col:
            raise ValueError("snr_col and snr_mark_col must be different")
        return self

    @property
    def whodat_columns(self) -> set[str]:
        """Return all configured WhoDat columns."""
        strategy_cols = {
            column
            for strategy in self.fnrsearch_strategies
            for column in strategy.variables
        }
        return set(self.fnrsearch_cols) | strategy_cols

    @property
    def person_columns(self) -> set[str]:
        """Return configured columns that indicate person data."""
        return {column for column in [self.fnr_col, *self.pseudo_cols] if column}
