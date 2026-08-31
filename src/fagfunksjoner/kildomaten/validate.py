import pandas as pd

from .globals import (
    CANON_BIRTH,
    CANON_FNR,
    CANON_GENDER,
    CANON_PERSNAVN,
    CANON_SNR,
    CANON_SNR_MRK,
    PERSON_COLS,
    WHODAT_VARIABLE_MAP,
    WORK_COLS,
    logger,
)


def _has_person_data(df: pd.DataFrame) -> bool:
    """Return True if the file contains at least one person column."""
    return any(c in df.columns for c in PERSON_COLS)


def assert_prepped_input(df: pd.DataFrame) -> None:
    """Validate that input has expected canonical columns from PREPPING.

    Files without person data, such as code lists and register files, pass
    without person validation. Files with person data must contain FNR and
    birth date columns. Other person columns are optional and are used by
    WhoDat when available.

    Args:
        df: Input DataFrame to validate.

    Returns:
        None: This function raises AssertionError when validation fails.

    Raises:
        AssertionError: If required canonical columns are missing.
    """
    if not _has_person_data(df):
        logger.info(
            "Ingen personkolonner — fil uten persondata, hopper over person-validering."
        )
        return

    missing = [c for c in (CANON_FNR, CANON_BIRTH) if c not in df.columns]
    if missing:
        raise AssertionError(
            f"Mangler canonical-kolonner fra PREPPING: {missing}. "
            f"Fant {len(df.columns)} kolonner totalt."
        )

    tilgjengelige_whodat = [c for c in WHODAT_VARIABLE_MAP if c in df.columns]
    if not tilgjengelige_whodat:
        logger.warning("Ingen whodat-hjelpvariabler funnet — WhoDat hoppes over.")
    else:
        logger.info("Tilgjengelige whodat-variabler: %s", tilgjengelige_whodat)


def summarize_input(df: pd.DataFrame) -> dict:
    """Summarize basic input dimensions and missing canonical values.

    Args:
        df: Input DataFrame to summarize.

    Returns:
        dict: Counts for rows, columns, and missing canonical person values.
    """

    def _na_blank(col: pd.Series) -> int:
        if col not in df.columns:
            return None
        s = df[col].astype("string[pyarrow]").str.strip()
        return int((s.isna() | (s == "")).sum())

    return {
        "n_rows": len(df),
        "n_cols": len(df.columns),
        "fnr_na_blank": _na_blank(CANON_FNR),
        "birth_na_blank": _na_blank(CANON_BIRTH),
        "gender_na_blank": _na_blank(CANON_GENDER),
        "elevnavn_na_blank": _na_blank(CANON_PERSNAVN),
    }


# ------------------------------------------------------------
# Output-validering
# ------------------------------------------------------------
def validate_output(df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
    """Validate output consistency after pseudonymization and WhoDat lookup.

    The output must contain SNR and SNR marker columns, use bool[pyarrow] for
    the SNR marker, exclude WhoDat work columns, and avoid unexpected new
    columns.

    Args:
        df_in: Input DataFrame before processing.
        df_out: Output DataFrame after processing.

    Returns:
        None: This function raises AssertionError when validation fails.

    Raises:
        AssertionError: If output columns or dtypes are not consistent.
    """
    for col in (CANON_SNR, CANON_SNR_MRK):
        if col not in df_out.columns:
            raise AssertionError(f"{col} mangler i output.")

    if str(df_out[CANON_SNR_MRK].dtype) != "bool[pyarrow]":
        raise AssertionError(
            f"snr_mrk skal være bool[pyarrow], er {df_out[CANON_SNR_MRK].dtype}."
        )

    work_present = [c for c in WORK_COLS if c in df_out.columns]
    if work_present:
        raise AssertionError(f"Whodat-arbeidskolonner finnes i output: {work_present}")

    added = sorted(set(df_out.columns) - set(df_in.columns))
    unexpected = [c for c in added if c not in {CANON_SNR, CANON_SNR_MRK}]
    if unexpected:
        raise AssertionError(f"Uventede nye kolonner i output: {unexpected}")
