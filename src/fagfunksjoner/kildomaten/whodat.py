from collections.abc import Mapping
from typing import Any, cast

import pandas as pd
from dapla_pseudo import Validator
from dapla_whodat import Whodat

from .dtypes import STRING_PYARROW_DTYPE
from .fileconfig import FileConfig, WhodatSearchStrategy
from .kilde_logging import logger

_DIGIT_ONLY_WHODAT_COLUMNS = {
    "foedselsaarFraOgMed",
    "foedselsaarTilOgMed",
    "postnummer",
    "kommunenummer",
    "fylkesnummer",
}


def _normalize_gender_for_whodat(
    gender: pd.Series,
    file_config: FileConfig,
) -> pd.Series:
    """Convert configured gender values to WhoDat format ('mann'/'kvinne')."""
    x = (
        gender.astype(STRING_PYARROW_DTYPE)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "", regex=True)
    )
    return x.map(file_config.gender_values).astype(STRING_PYARROW_DTYPE)


def _as_stripped_string(value: pd.Series) -> pd.Series:
    return value.astype(STRING_PYARROW_DTYPE).str.strip()


def _as_digit_string(value: pd.Series, *, datetime_format: str) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(value):
        return cast(pd.Series, value.dt.strftime(datetime_format))
    return _as_stripped_string(value).str.replace(r"\D", "", regex=True)


def _prepare_whodat_column(
    column: str,
    value: pd.Series,
    file_config: FileConfig,
) -> pd.Series:
    if column == "kjoenn":
        return _normalize_gender_for_whodat(value, file_config)
    if column == "foedselsdato":
        return _as_digit_string(value, datetime_format="%Y%m%d")
    if column in _DIGIT_ONLY_WHODAT_COLUMNS:
        return _as_digit_string(value, datetime_format="%Y")
    return _as_stripped_string(value)


def _prepare_whodat_work_columns(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> pd.DataFrame:
    """Normalize configured WhoDat columns to the formats expected by WhoDat."""
    df = df.copy()
    for column in file_config.whodat_columns:
        if column not in df.columns:
            continue
        df[column] = _prepare_whodat_column(column, df[column], file_config)
    return df


def _available_whodat_columns(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> list[str]:
    configured_columns = [*file_config.fnrsearch_cols]
    for strategy in file_config.fnrsearch_strategies:
        configured_columns.extend(strategy.variables)

    return [
        column for column in dict.fromkeys(configured_columns) if column in df.columns
    ]


def _missing_whodat_columns(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> list[str]:
    return sorted(column for column in file_config.whodat_columns if column not in df)


def _needs_whodat_lookup_mask(
    df: pd.DataFrame,
    fnr_col: str,
    *,
    dry_run: bool = False,
) -> pd.Series:
    if dry_run:
        fnr = df[fnr_col].astype(STRING_PYARROW_DTYPE).str.strip()
        has_bad_shape = (
            fnr.notna() & (fnr != "") & ~fnr.str.match(r"^\d{11}$").fillna(False)
        )
        return fnr.isna() | (fnr == "") | has_bad_shape

    validation_df = (
        Validator.from_pandas(df[[fnr_col]].copy())
        .on_field(fnr_col)
        .validate_map_to_stable_id()
        .to_pandas()
    )
    return df[fnr_col].isin(validation_df[fnr_col])


def _has_searchable_pii(
    df: pd.DataFrame,
    available_cols: list[str],
) -> pd.Series:
    has_searchable = pd.Series(False, index=df.index)
    for column in available_cols:
        values = df[column].astype(STRING_PYARROW_DTYPE).str.strip()
        has_searchable = has_searchable | (values.notna() & (values != ""))
    return has_searchable


def _dedupe_strategies(
    strategies: list[WhodatSearchStrategy],
) -> list[WhodatSearchStrategy]:
    seen: set[tuple[tuple[str, ...], bool, bool, bool, str]] = set()
    deduped = []
    for strategy in strategies:
        key = (
            tuple(strategy.variables),
            strategy.inkluder_oppholdsadresse,
            strategy.soek_fonetisk,
            strategy.inkluder_doede,
            strategy.opplysningsgrunnlag,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(strategy)
    return deduped


def _build_search_strategies(
    available_cols: list[str],
    file_config: FileConfig,
) -> list[WhodatSearchStrategy]:
    strategies = []
    if file_config.fnrsearch_strategies:
        for strategy in file_config.fnrsearch_strategies:
            variables = [
                column for column in strategy.variables if column in available_cols
            ]
            if variables:
                strategies.append(strategy.model_copy(update={"variables": variables}))
    else:
        strategies.extend(
            WhodatSearchStrategy(variables=available_cols[:i])
            for i in range(1, len(available_cols) + 1)
        )

    if file_config.add_relaxed_fnrsearch_strategy and available_cols:
        strategies.append(
            WhodatSearchStrategy(
                variables=available_cols,
                inkluder_doede=True,
                soek_fonetisk=True,
            )
        )

    return _dedupe_strategies(strategies)


def _strategy_label(strategy: WhodatSearchStrategy) -> str:
    options = []
    if strategy.inkluder_doede:
        options.append("inkluder_doede=True")
    if strategy.soek_fonetisk:
        options.append("soek_fonetisk=True")
    if strategy.inkluder_oppholdsadresse:
        options.append("inkluder_oppholdsadresse=True")
    if strategy.opplysningsgrunnlag != "gjeldende":
        options.append(f"opplysningsgrunnlag={strategy.opplysningsgrunnlag}")
    suffix = f", {', '.join(options)}" if options else ""
    return f"FNR search: {strategy.variables}{suffix}"


def _log_whodat_step_hits(
    result: object,
    strategies: list[WhodatSearchStrategy],
) -> None:
    details = getattr(result, "details", None)
    if details is None:
        logger.info("WhoDat: no details available for search step distribution.")
        return

    stepnames = [_strategy_label(strategy) for strategy in strategies]
    try:
        hits = {}
        for item in details:
            original_index = item["index_original_df"]
            step_number = item["unique_response_step_number"]
            if step_number is None:
                hits[original_index] = "No FNR hit after all configured attempts."
            else:
                hits[original_index] = stepnames[int(step_number) - 1]
        logger.info(
            "WhoDat search step hits: %s",
            pd.Series(hits).value_counts(dropna=True).to_dict(),
        )
    except Exception as e:
        logger.info("Could not log WhoDat search step distribution: %s", e)


def _clean_whodat_mapping(mapping: Mapping[Any, Any]) -> dict[object, str]:
    cleaned = {}
    for index, fnr in mapping.items():
        if pd.isna(fnr):
            continue
        value = str(fnr).strip()
        if value:
            cleaned[index] = value
    return cleaned


def _exceeds_whodat_limits(
    stats: dict[str, int | float],
    file_config: FileConfig,
) -> bool:
    return (
        stats["to_whodat"] > file_config.max_whodat_rows
        or stats["to_whodat_share"] > file_config.max_whodat_share
    )


def _run_whodat_search_chunks(
    df: pd.DataFrame,
    lookup_indices: list[object],
    available_cols: list[str],
    strategies: list[WhodatSearchStrategy],
    file_config: FileConfig,
) -> dict[object, str]:
    all_mappings: dict[object, str] = {}
    n_chunks = -(-len(lookup_indices) // file_config.chunk_size)

    logger.info(
        "WhoDat: starting lookup. Rows=%d, chunks=%d, strategies=%s.",
        len(lookup_indices),
        n_chunks,
        [strategy.variables for strategy in strategies],
    )

    for i in range(0, len(lookup_indices), file_config.chunk_size):
        chunk_no = i // file_config.chunk_size + 1
        chunk_idx = lookup_indices[i : i + file_config.chunk_size]
        work = df.loc[chunk_idx, available_cols].copy()

        process = Whodat.from_pandas(work).search_fnr()
        for strategy in strategies:
            process = process.with_search_strategy(
                variables=strategy.variables,
                inkluder_oppholdsadresse=strategy.inkluder_oppholdsadresse,
                soek_fonetisk=strategy.soek_fonetisk,
                inkluder_doede=strategy.inkluder_doede,
                opplysningsgrunnlag=strategy.opplysningsgrunnlag,
            )

        logger.info("WhoDat chunk %d/%d: rows=%d.", chunk_no, n_chunks, len(work))

        try:
            result = process.run()
        except Exception as e:
            logger.exception("WhoDat chunk %d/%d failed: %s", chunk_no, n_chunks, e)
            continue

        mapping = result.to_dict_from_original_indices()
        all_mappings.update(_clean_whodat_mapping(mapping))
        _log_whodat_step_hits(result, strategies)

        logger.info("WhoDat chunk %d/%d: hits=%d.", chunk_no, n_chunks, len(mapping))

    return all_mappings


def drop_work_columns(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> pd.DataFrame:
    """Drop WhoDat work columns from a DataFrame.

    Args:
        df: DataFrame that may contain WhoDat work columns.
        file_config: File-specific processing configuration.

    Returns:
        pd.DataFrame: The DataFrame without WhoDat work columns.
    """
    drop_cols = [
        column for column in file_config.whodat_columns if column in df.columns
    ]
    return df.drop(columns=drop_cols) if drop_cols else df


def should_run_whodat(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> bool:
    """Return whether WhoDat lookup should run for a DataFrame.

    Args:
        df: DataFrame to inspect for configured FNR lookup input.
        file_config: File-specific processing configuration.

    Returns:
        bool: True when WhoDat lookup is configured and has usable inputs.
    """
    return (
        file_config.use_fnrsearch
        and file_config.fnr_col in df.columns
        and bool(_available_whodat_columns(df, file_config))
    )


def whodat_lookup_fnr(
    df: pd.DataFrame,
    file_config: FileConfig,
    *,
    dry_run: bool = False,
) -> tuple[pd.DataFrame, dict[str, int | float]]:
    """Look up FNR values in WhoDat when they are missing or invalid.

    Lookup is only attempted for rows that have a missing or invalid FNR,
    contain at least one searchable WhoDat value, and stay within the maximum
    row count and share limits. Requests are processed in chunks to reduce the
    risk of 413 Payload Too Large responses.

    Args:
        df: Input data with configured WhoDat columns.
        file_config: File-specific processing configuration.
        dry_run: Whether to skip external Validator and WhoDat calls.

    Returns:
        tuple[pd.DataFrame, dict]: The updated DataFrame and lookup statistics.
    """
    df = _prepare_whodat_work_columns(df, file_config)
    base_stats = {
        "needs_lookup": 0,
        "missing_fnr": 0,
        "invalid_fnr": 0,
        "skipped_no_searchable": 0,
        "to_whodat": 0,
        "to_whodat_share": 0.0,
        "whodat_hits": 0,
        "dry_run": int(dry_run),
    }

    if not file_config.fnr_col or file_config.fnr_col not in df.columns:
        logger.info(
            "WhoDat: skipping lookup because the configured FNR column is missing."
        )
        return df, base_stats

    fnr = df[file_config.fnr_col].astype(STRING_PYARROW_DTYPE).str.strip()
    missing = fnr.isna() | (fnr == "")
    needs = _needs_whodat_lookup_mask(
        df,
        file_config.fnr_col,
        dry_run=dry_run,
    )

    stats = {
        **base_stats,
        "needs_lookup": int(needs.sum()),
        "missing_fnr": int(missing.sum()),
        "invalid_fnr": int((needs & ~missing).sum()),
    }

    if not stats["needs_lookup"]:
        if dry_run:
            logger.info("WhoDat dry-run: no rows would be selected for lookup.")
        else:
            logger.info(
                "WhoDat: no rows need lookup after FNR validation against stable ID."
            )
        return df, stats

    available_cols = _available_whodat_columns(df, file_config)
    missing_cols = _missing_whodat_columns(df, file_config)
    if missing_cols:
        logger.warning(
            "WhoDat: configured helper columns not found in input: %s",
            missing_cols,
        )
    if not available_cols:
        logger.info("WhoDat: no configured helper columns are available.")
        return df, stats

    has_searchable = _has_searchable_pii(df, available_cols)
    mask_lookup = needs & has_searchable

    stats["skipped_no_searchable"] = int((needs & ~has_searchable).sum())
    stats["to_whodat"] = int(mask_lookup.sum())
    stats["to_whodat_share"] = stats["to_whodat"] / len(df) if len(df) else 0.0

    if not stats["to_whodat"]:
        logger.info(
            "WhoDat: no rows sent to lookup. Skipped without searchable values=%d.",
            stats["skipped_no_searchable"],
        )
        return df, stats

    if dry_run:
        logger.info(
            "WhoDat dry-run: skipping Validator/WhoDat service calls. "
            "Rows that would be sent=%d.",
            stats["to_whodat"],
        )
        return df, stats

    if _exceeds_whodat_limits(stats, file_config):
        logger.warning(
            "WhoDat: skipping lookup because too many rows qualify. "
            "to_whodat=%d, share=%.4f, max_rows=%d, max_share=%.4f.",
            stats["to_whodat"],
            stats["to_whodat_share"],
            file_config.max_whodat_rows,
            file_config.max_whodat_share,
        )
        return df, stats

    original_fnr_col = f"{file_config.fnr_col}_orig"
    if original_fnr_col not in df.columns:
        df[original_fnr_col] = df[file_config.fnr_col]

    lookup_indices = df.index[mask_lookup].tolist()
    strategies = _build_search_strategies(available_cols, file_config)

    all_mappings = _run_whodat_search_chunks(
        df=df,
        lookup_indices=lookup_indices,
        available_cols=available_cols,
        strategies=strategies,
        file_config=file_config,
    )

    stats["whodat_hits"] = len(all_mappings)

    df.loc[mask_lookup, file_config.fnr_col] = (
        df.loc[mask_lookup]
        .index.to_series()
        .map(all_mappings)
        .combine_first(df.loc[mask_lookup, original_fnr_col])
    )

    logger.info(
        "WhoDat: hits=%d / %d. Skipped without searchable values=%d.",
        stats["whodat_hits"],
        stats["to_whodat"],
        stats["skipped_no_searchable"],
    )

    return df, stats
