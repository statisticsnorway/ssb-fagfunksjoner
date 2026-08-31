import pandas as pd
from dapla_whodat import Whodat

from .globals import CANON_FNR, CANON_GENDER, WHODAT_VARIABLE_MAP, WORK_COLS, logger


def whodat_lookup_fnr(
    df: pd.DataFrame,
    *,
    chunk_size: int = 250,
    max_whodat_share: float = 0.10,
    max_whodat_rows: int = 50_000,
) -> tuple[pd.DataFrame, dict]:
    """Look up FNR values in WhoDat when they are missing or invalid.

    Lookup is only attempted for rows that have a missing or invalid FNR,
    contain at least one searchable WhoDat value, and stay within the maximum
    row count and share limits. Requests are processed in chunks to reduce the
    risk of 413 Payload Too Large responses.

    Args:
        df: Input data with canonical and prepared WhoDat columns.
        chunk_size: Number of rows to include in each WhoDat request.
        max_whodat_share: Maximum share of input rows allowed for lookup.
        max_whodat_rows: Maximum number of input rows allowed for lookup.

    Returns:
        tuple[pd.DataFrame, dict]: The updated DataFrame and lookup statistics.
    """
    df = df.copy()

    base_stats = {
        "needs_lookup": 0,
        "missing_fnr": 0,
        "bad_fnr_format": 0,
        "skipped_no_searchable": 0,
        "to_whodat": 0,
        "to_whodat_share": 0.0,
        "whodat_hits": 0,
    }

    if CANON_FNR not in df.columns:
        logger.info("WhoDat: hopper over, mangler fnr-kolonne.")
        return df, base_stats

    fnr = df[CANON_FNR].astype("string[pyarrow]").str.strip()
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
        logger.info("WhoDat: ingen rader trenger oppslag.")
        return df, stats

    work_cols_available = [c for c in WORK_COLS if c in df.columns]
    if not work_cols_available:
        logger.info("WhoDat: ingen arbeidskolonner tilgjengelig.")
        return df, stats

    searchable_cols = [c for c in ("navn", "kjoenn", "foedselsdato") if c in df.columns]

    has_searchable = pd.Series(False, index=df.index)

    for col in searchable_cols:
        s = df[col].astype("string[pyarrow]").str.strip()
        has_searchable = has_searchable | (s.notna() & (s != ""))

    mask_lookup = needs & has_searchable

    stats["skipped_no_searchable"] = int((needs & ~has_searchable).sum())
    stats["to_whodat"] = int(mask_lookup.sum())
    stats["to_whodat_share"] = stats["to_whodat"] / len(df) if len(df) else 0.0

    if not stats["to_whodat"]:
        logger.info(
            "WhoDat: ingen rader til oppslag. Skippet uten søkbar opplysning=%d.",
            stats["skipped_no_searchable"],
        )
        return df, stats

    if (
        stats["to_whodat"] > max_whodat_rows
        or stats["to_whodat_share"] > max_whodat_share
    ):
        logger.warning(
            "WhoDat: hopper over oppslag fordi for mange rader kvalifiserer. "
            "to_whodat=%d, andel=%.4f, max_rader=%d, max_andel=%.4f. "
            "Dette tyder på feil/blank fnr-kolonne eller for lav datakvalitet.",
            stats["to_whodat"],
            stats["to_whodat_share"],
            max_whodat_rows,
            max_whodat_share,
        )
        return df, stats

    if "fnr_orig" not in df.columns:
        df["fnr_orig"] = df[CANON_FNR]

    cols_prio = [
        c
        for c in ("navn", "kjoenn", "foedselsdato", "kommunenummer", "fylkesnummer")
        if c in work_cols_available
    ]

    lookup_indices = df.index[mask_lookup].tolist()
    all_mappings: dict = {}
    n_chunks = -(-len(lookup_indices) // chunk_size)

    logger.info(
        "WhoDat: starter oppslag. Rader=%d, chunks=%d, andel=%.4f, variabler=%s.",
        stats["to_whodat"],
        n_chunks,
        stats["to_whodat_share"],
        cols_prio,
    )

    for i in range(0, len(lookup_indices), chunk_size):
        chunk_no = i // chunk_size + 1
        chunk_idx = lookup_indices[i : i + chunk_size]
        work = df.loc[chunk_idx, work_cols_available].copy()

        process = Whodat.from_pandas(work).search_fnr()

        for j in range(1, len(cols_prio) + 1):
            process = process.with_search_strategy(variables=cols_prio[:j])
            if j == len(cols_prio):
                process = process.with_search_strategy(
                    variables=cols_prio[:j],
                    inkluder_doede=True,
                    soek_fonetisk=True,
                )

        logger.info(
            "WhoDat chunk %d/%d: rader=%d. Variabler=%s.",
            chunk_no,
            n_chunks,
            len(work),
            cols_prio,
        )

        try:
            result = process.run()
        except Exception as e:
            logger.exception("WhoDat chunk %d/%d feilet: %s", chunk_no, n_chunks, e)
            continue

        mapping = result.to_dict_from_original_indices()
        all_mappings.update(mapping)

        logger.info(
            "WhoDat chunk %d/%d: treff=%d.",
            chunk_no,
            n_chunks,
            len(mapping),
        )

    stats["whodat_hits"] = len(all_mappings)

    df.loc[mask_lookup, CANON_FNR] = (
        df.loc[mask_lookup]
        .index.to_series()
        .map(all_mappings)
        .combine_first(df.loc[mask_lookup, "fnr_orig"])
    )

    logger.info(
        "WhoDat: treff=%d / %d. Skippet uten søkbar opplysning=%d.",
        stats["whodat_hits"],
        stats["to_whodat"],
        stats["skipped_no_searchable"],
    )

    return df, stats


def _normalize_gender_for_whodat(pers_kjoenn: pd.Series) -> pd.Series:
    """Convert canonical pers_kjoenn ('1'/'2') to WhoDat format ('mann'/'kvinne')."""
    x = (
        pers_kjoenn.astype("string[pyarrow]")
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "", regex=True)
    )
    out = pd.Series(pd.NA, index=pers_kjoenn.index, dtype="string[pyarrow]")
    out = out.where(~x.isin({"2", "k", "kvinne", "f"}), "kvinne")
    out = out.where(~x.isin({"1", "m", "mann"}), "mann")
    return out


def _prepare_whodat_work_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Derive WhoDat work columns from canonical columns.

    WhoDat requires specific column names (navn, kjoenn, foedselsdato,
    kommunenummer, fylkesnummer), which are derived from canonical names.
    """
    df = df.copy()
    for canon_col, work_col in WHODAT_VARIABLE_MAP.items():
        if canon_col not in df.columns or work_col in df.columns:
            continue
        if canon_col == CANON_GENDER:
            df[work_col] = _normalize_gender_for_whodat(df[canon_col])
        else:
            df[work_col] = df[canon_col].astype("string[pyarrow]").str.strip()
    return df


def drop_work_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop WhoDat work columns from a DataFrame.

    Args:
        df: DataFrame that may contain WhoDat work columns.

    Returns:
        pd.DataFrame: The DataFrame without WhoDat work columns.
    """
    drop_cols = [c for c in WORK_COLS if c in df.columns]
    return df.drop(columns=drop_cols) if drop_cols else df


def should_run_whodat(df: pd.DataFrame) -> bool:
    """Return whether WhoDat lookup should run for a DataFrame.

    Args:
        df: DataFrame to inspect for the canonical FNR column.

    Returns:
        bool: True when the DataFrame has the canonical FNR column.
    """
    return CANON_FNR in df.columns
