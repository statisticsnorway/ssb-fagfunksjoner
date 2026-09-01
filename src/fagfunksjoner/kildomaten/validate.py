import pandas as pd

from .fileconfig import FileConfig
from .kilde_logging import logger


def _has_person_data(df: pd.DataFrame, file_config: FileConfig) -> bool:
    """Return True if the file contains at least one configured person column."""
    return any(column in df.columns for column in file_config.person_columns)


def assert_prepped_input(df: pd.DataFrame, file_config: FileConfig) -> None:
    """Validate that input has expected columns from the file configuration.

    Files without configured person data pass without person validation. Files
    with configured person data must contain their configured FNR and
    pseudonymization columns. Other person columns are optional and are used by
    WhoDat when available.

    Args:
        df: Input DataFrame to validate.
        file_config: File-specific processing configuration.

    Returns:
        None: This function raises AssertionError when validation fails.

    Raises:
        AssertionError: If required configured columns are missing.
    """
    if not file_config.person_columns:
        logger.info("No configured person columns found, skipping person validation.")
        return

    required = [*file_config.pseudo_cols]
    if file_config.fnr_col:
        required.append(file_config.fnr_col)

    missing = sorted({column for column in required if column not in df.columns})
    if missing:
        logger.error("Configured person columns are missing from input: %s", missing)
        raise AssertionError(
            f"Missing configured person columns: {missing}. "
            f"Found {len(df.columns)} columns in total."
        )

    available_whodat = [
        column for column in file_config.whodat_columns if column in df.columns
    ]
    if file_config.use_fnrsearch and not available_whodat:
        logger.warning("No configured WhoDat helper columns found, skipping WhoDat.")
    elif available_whodat:
        logger.info("Available WhoDat variables: %s", available_whodat)


def summarize_input(df: pd.DataFrame, file_config: FileConfig) -> dict[str, int | None]:
    """Summarize basic input dimensions and missing configured person values.

    Args:
        df: Input DataFrame to summarize.
        file_config: File-specific processing configuration.

    Returns:
        dict: Counts for rows, columns, and missing configured person values.
    """

    def _na_blank(column: str) -> int | None:
        if column not in df.columns:
            return None
        s = df[column].astype("string[pyarrow]").str.strip()
        return int((s.isna() | (s == "")).sum())

    summary: dict[str, int | None] = {
        "n_rows": len(df),
        "n_cols": len(df.columns),
    }
    if file_config.fnr_col:
        summary[f"{file_config.fnr_col}_na_blank"] = _na_blank(file_config.fnr_col)
    for column in file_config.pseudo_cols:
        summary[f"{column}_na_blank"] = _na_blank(column)
    for column in file_config.whodat_columns:
        summary[f"{column}_na_blank"] = _na_blank(column)
    return summary


def validate_output(
    df_in: pd.DataFrame,
    df_out: pd.DataFrame,
    file_config: FileConfig,
) -> None:
    """Validate output consistency after pseudonymization and WhoDat lookup.

    The output must contain the configured SNR column when person data was
    processed and avoid unexpected new columns.

    Args:
        df_in: Input DataFrame before processing.
        df_out: Output DataFrame after processing.
        file_config: File-specific processing configuration.

    Returns:
        None: This function raises AssertionError when validation fails.

    Raises:
        AssertionError: If output columns or dtypes are not consistent.
    """
    if _has_person_data(df_in, file_config):
        if file_config.snr_col not in df_out.columns:
            raise AssertionError(f"{file_config.snr_col} is missing in output.")

    added = sorted(set(df_out.columns) - set(df_in.columns))
    expected = {file_config.snr_col}
    unexpected = [column for column in added if column not in expected]
    if unexpected:
        raise AssertionError(f"Unexpected new columns in output: {unexpected}")
