from pathlib import Path

import pandas as pd

from .config import KildomatConfig
from .dtypes import normalize_dtypes
from .kilde_logging import logger
from .pii import _drop_original_fnr_columns, drop_configured_columns
from .preprocess import _apply_configured_preprocessing
from .pseudo import pseudo_and_snr
from .read import _load_input
from .validate import (
    _has_person_data,
    assert_prepped_input,
    summarize_input,
    validate_output,
)
from .whodat import (
    should_run_whodat,
    whodat_lookup_fnr,
)
from .write import _resolve_output_path


def _log_retained_whodat_columns(
    df: pd.DataFrame,
    file_config: KildomatConfig,
) -> None:
    retained = sorted(column for column in file_config.whodat_columns if column in df)
    if retained:
        logger.info(
            "Keeping configured fnrsearch columns in output because they are not "
            "listed in drop_cols: %s",
            retained,
        )


def run_kildomaten_pipeline(
    df: pd.DataFrame | str | Path,
    file_config: KildomatConfig,
    *,
    dry_run: bool = False,
) -> Path | pd.DataFrame:
    """Run the Kildomaten processing pipeline for one DataFrame or parquet file.

    Args:
        df: Input DataFrame or path to a parquet file.
        file_config: File-specific processing configuration.
        dry_run: Whether to skip environment-dependent services and writing.

    Returns:
        Path | pd.DataFrame: Path to the written parquet output, or the
            processed DataFrame during dry-runs.

    Raises:
        Exception: Re-raises any exception from the processing step that fails.
    """
    step = "start"
    try:
        logger.info("Starting kildomat pipeline.")

        step = "read_input"
        input_df, source_path = _load_input(df)

        step = "preprocess"
        input_df = _apply_configured_preprocessing(input_df, file_config)
        logger.info(
            "Read input: rows=%d columns=%d", len(input_df), len(input_df.columns)
        )

        step = "resolve_output_path"
        output_path = _resolve_output_path(
            source_path,
            file_config,
            dry_run=dry_run,
        )
        logger.info("Output: %s", output_path)

        step = "assert_prepped_input"
        assert_prepped_input(input_df, file_config)

        step = "summarize_input"
        logger.info("Input summary: %s", summarize_input(input_df, file_config))

        working_df = input_df

        if not _has_person_data(working_df, file_config):
            logger.info("No configured person data found, writing data unchanged.")
            out_df = normalize_dtypes(working_df)
        else:
            step = "whodat"
            if should_run_whodat(working_df, file_config):
                working_df, whodat_stats = whodat_lookup_fnr(
                    working_df,
                    file_config,
                    dry_run=dry_run,
                )
                logger.info("WhoDat stats: %s", whodat_stats)
            else:
                logger.info("WhoDat: not configured or no helper variables available.")

            step = "pseudo_and_snr"
            pseudo_df, pseudo_stats = pseudo_and_snr(
                working_df,
                file_config,
                dry_run=dry_run,
            )
            logger.info("Pseudo stats: %s", pseudo_stats)

            step = "cleanup"
            out_df = _drop_original_fnr_columns(pseudo_df, file_config)
            out_df = drop_configured_columns(out_df, file_config)
            _log_retained_whodat_columns(out_df, file_config)

            step = "normalize_dtypes"
            out_df = normalize_dtypes(out_df)

            step = "validate_output"
            validate_output(
                df_in=working_df,
                df_out=out_df,
                file_config=file_config,
            )

        step = "write_parquet"
        if dry_run:
            logger.info(
                "Dry-run: skipping parquet write for output with rows=%d columns=%d. Outputting changed dataframe instead of path: %s",
                len(out_df),
                len(out_df.columns),
                output_path,
            )
            return out_df

        logger.info(
            "Writing output: rows=%d columns=%d", len(out_df), len(out_df.columns)
        )
        out_df.to_parquet(output_path, engine="pyarrow", index=False)

        step = "done"
        logger.info("Done: %s", output_path)
        return output_path

    except Exception as e:
        logger.exception("Error in step '%s': %s", step, e)
        raise
