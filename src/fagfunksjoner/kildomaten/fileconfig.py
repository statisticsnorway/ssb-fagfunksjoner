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

    model_config = ConfigDict(frozen=True)

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
    """Configuration for processing one source file type."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    fnr_col: str | None = None
    pseudo_cols: list[str] = Field(default_factory=list)
    bruk_fnrleting: bool = False
    fnrleting_cols: list[str] = Field(default_factory=list)
    fnrleting_search_strategies: list[WhodatSearchStrategy] = Field(
        default_factory=list
    )
    add_relaxed_fnrleting_strategy: bool = True
    rename_map: dict[str, str] = Field(default_factory=dict)
    drop_cols: list[str] = Field(default_factory=list)
    sensitive_cols: list[str] = Field(default_factory=lambda: ["pers_personnummer"])
    preprocess_func: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    snr_col: str = "snr"
    snr_mark_col: str = "snr_mrk"
    output_path: Path | None = None
    output_dir: Path | None = None
    output_name_insert: str = "_inndata_"
    chunk_size: int = Field(default=250, gt=0)
    max_whodat_share: float = Field(default=0.10, ge=0, le=1)
    max_whodat_rows: int = Field(default=50_000, ge=0)
    gender_values: dict[str, Literal["mann", "kvinne"]] = Field(
        default_factory=lambda: DEFAULT_GENDER_VALUES.copy()
    )

    @field_validator("fnr_col", "snr_col", "snr_mark_col", mode="before")
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
        "sensitive_cols",
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

    @field_validator("fnrleting_cols", mode="before")
    @classmethod
    def _validate_fnrleting_cols(cls, values: object) -> object:
        if values is None:
            return []
        if isinstance(values, str):
            raise ValueError("fnrleting_cols must be a list, not a string")
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
        if self.bruk_fnrleting and not self.fnr_col:
            raise ValueError("fnr_col is required when bruk_fnrleting=True")
        if self.bruk_fnrleting and not (
            self.fnrleting_cols or self.fnrleting_search_strategies
        ):
            raise ValueError(
                "fnrleting_cols or fnrleting_search_strategies is required "
                "when bruk_fnrleting=True"
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
            for strategy in self.fnrleting_search_strategies
            for column in strategy.variables
        }
        return set(self.fnrleting_cols) | strategy_cols

    @property
    def person_columns(self) -> set[str]:
        """Return configured columns that indicate person data."""
        return {column for column in [self.fnr_col, *self.pseudo_cols] if column}
