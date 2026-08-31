import pandas as pd
from dapla_whodat import Whodat

from .fileconfig import FileConfig, WhodatSearchStrategy
from .globals import logger


def _valid_fnr_mask(df: pd.DataFrame, fnr_col: str) -> pd.Series:
    fnr = df[fnr_col].astype("string[pyarrow]").str.strip()
    return fnr.notna() & (fnr != "") & fnr.str.match(r"^\d{11}$")


def _normalize_gender_for_whodat(
    gender: pd.Series,
    file_config: FileConfig,
) -> pd.Series:
    """Convert configured gender values to WhoDat format ('mann'/'kvinne')."""
    x = (
        gender.astype("string[pyarrow]")
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "", regex=True)
    )
    return x.map(file_config.gender_values).astype("string[pyarrow]")


def _prepare_whodat_work_columns(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> pd.DataFrame:
    """Normalize configured WhoDat columns to the formats expected by WhoDat."""
    df = df.copy()
    for column in file_config.whodat_columns:
        if column not in df.columns:
            continue
        if column == "kjoenn":
            df[column] = _normalize_gender_for_whodat(df[column], file_config)
        elif column == "foedselsdato":
            value = df[column]
            if pd.api.types.is_datetime64_any_dtype(value):
                df[column] = value.dt.strftime("%Y%m%d")
            else:
                df[column] = (
                    value.astype("string[pyarrow]")
                    .str.strip()
                    .str.replace(r"\D", "", regex=True)
                )
        else:
            df[column] = df[column].astype("string[pyarrow]").str.strip()
    return df


def _available_whodat_columns(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> list[str]:
    return [column for column in file_config.fnrleting_cols if column in df.columns]


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
    if file_config.fnrleting_search_strategies:
        for strategy in file_config.fnrleting_search_strategies:
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

    if file_config.add_relaxed_fnrleting_strategy and available_cols:
        strategies.append(
            WhodatSearchStrategy(
                variables=available_cols,
                inkluder_doede=True,
                soek_fonetisk=True,
            )
        )

    return _dedupe_strategies(strategies)


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
        file_config.bruk_fnrleting
        and file_config.fnr_col in df.columns
        and bool(_available_whodat_columns(df, file_config))
    )


def whodat_lookup_fnr(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> tuple[pd.DataFrame, dict[str, int | float]]:
    """Look up FNR values in WhoDat when they are missing or invalid.

    Lookup is only attempted for rows that have a missing or invalid FNR,
    contain at least one searchable WhoDat value, and stay within the maximum
    row count and share limits. Requests are processed in chunks to reduce the
    risk of 413 Payload Too Large responses.

    Args:
        df: Input data with configured WhoDat columns.
        file_config: File-specific processing configuration.

    Returns:
        tuple[pd.DataFrame, dict]: The updated DataFrame and lookup statistics.
    """
    df = _prepare_whodat_work_columns(df, file_config)
    base_stats = {
        "needs_lookup": 0,
        "missing_fnr": 0,
        "bad_fnr_format": 0,
        "skipped_no_searchable": 0,
        "to_whodat": 0,
        "to_whodat_share": 0.0,
        "whodat_hits": 0,
    }

    if not file_config.fnr_col or file_config.fnr_col not in df.columns:
        logger.info(
            "WhoDat: skipping lookup because the configured FNR column is missing."
        )
        return df, base_stats

    fnr = df[file_config.fnr_col].astype("string[pyarrow]").str.strip()
    missing = fnr.isna() | (fnr == "")
    bad = fnr.notna() & (fnr != "") & ~fnr.str.match(r"^\d{11}$")
    needs = missing | bad

    stats = {
        **base_stats,
        "needs_lookup": int(needs.sum()),
        "missing_fnr": int(missing.sum()),
        "bad_fnr_format": int(bad.sum()),
    }

    if not stats["needs_lookup"]:
        logger.info("WhoDat: no rows need lookup.")
        return df, stats

    available_cols = _available_whodat_columns(df, file_config)
    if not available_cols:
        logger.info("WhoDat: no configured helper columns are available.")
        return df, stats

    has_searchable = pd.Series(False, index=df.index)
    for column in available_cols:
        values = df[column].astype("string[pyarrow]").str.strip()
        has_searchable = has_searchable | (values.notna() & (values != ""))

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

    if (
        stats["to_whodat"] > file_config.max_whodat_rows
        or stats["to_whodat_share"] > file_config.max_whodat_share
    ):
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
    all_mappings: dict[int, str] = {}
    n_chunks = -(-len(lookup_indices) // file_config.chunk_size)
    strategies = _build_search_strategies(available_cols, file_config)

    logger.info(
        "WhoDat: starting lookup. Rows=%d, chunks=%d, share=%.4f, strategies=%s.",
        stats["to_whodat"],
        n_chunks,
        stats["to_whodat_share"],
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

        logger.info(
            "WhoDat chunk %d/%d: rows=%d.",
            chunk_no,
            n_chunks,
            len(work),
        )

        try:
            result = process.run()
        except Exception as e:
            logger.exception("WhoDat chunk %d/%d failed: %s", chunk_no, n_chunks, e)
            continue

        mapping = result.to_dict_from_original_indices()
        all_mappings.update(mapping)

        logger.info(
            "WhoDat chunk %d/%d: hits=%d.",
            chunk_no,
            n_chunks,
            len(mapping),
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
