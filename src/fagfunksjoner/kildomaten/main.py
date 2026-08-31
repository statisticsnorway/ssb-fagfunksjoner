from pathlib import Path

import pandas as pd

from .dtypes import normalize_dtypes
from .fileconfig import FileConfig
from .globals import logger
from .pii import drop_sensitive_columns
from .pseudo import pseudo_and_snr
from .validate import (
    _has_person_data,
    assert_prepped_input,
    summarize_input,
    validate_output,
)
from .whodat import (
    drop_work_columns,
    should_run_whodat,
    whodat_lookup_fnr,
)
from .write import build_output_name


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df


def _load_input(source: pd.DataFrame | str | Path) -> tuple[pd.DataFrame, Path | None]:
    if isinstance(source, pd.DataFrame):
        return source.copy(), None

    source_path = Path(source)
    if source_path.suffix.lower() != ".parquet":
        raise ValueError(f"Expected a parquet file, got: {source_path}")

    df = pd.read_parquet(source_path, dtype_backend="pyarrow")
    return df, source_path


def _apply_configured_preprocessing(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> pd.DataFrame:
    df = _normalize_column_names(df)
    if file_config.preprocess_func:
        df = file_config.preprocess_func(df)
        df = _normalize_column_names(df)
    if file_config.rename_map:
        df = df.rename(columns=file_config.rename_map)
        df = _normalize_column_names(df)
    return df


def _resolve_output_path(
    source_path: Path | None,
    file_config: FileConfig,
) -> Path:
    if file_config.output_path:
        return file_config.output_path

    if source_path is None:
        raise ValueError("file_config.output_path is required for DataFrame input")

    output_name = build_output_name(
        source_path.name,
        insert=file_config.output_name_insert,
    )
    output_dir = file_config.output_dir or source_path.parent
    return output_dir / output_name


def _drop_original_fnr_columns(
    df: pd.DataFrame,
    file_config: FileConfig,
) -> pd.DataFrame:
    original_cols = [
        f"{file_config.fnr_col}_orig" if file_config.fnr_col else "",
    ]
    drop_cols = [column for column in original_cols if column in df.columns]
    return df.drop(columns=drop_cols) if drop_cols else df


def run_kildomaten_pipeline(
    df: pd.DataFrame | str | Path,
    file_config: FileConfig,
) -> Path:
    """Run the Kildomaten processing pipeline for one DataFrame or parquet file.

    Args:
        df: Input DataFrame or path to a parquet file.
        file_config: File-specific processing configuration.

    Returns:
        Path: Path to the written parquet output.

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
        output_path = _resolve_output_path(source_path, file_config)
        logger.info("Output: %s", output_path)

        step = "assert_prepped_input"
        assert_prepped_input(input_df, file_config)

        step = "summarize_input"
        logger.info("Input summary: %s", summarize_input(input_df, file_config))

        working_df = input_df

        if not _has_person_data(working_df, file_config):
            logger.info("No configured person data found, writing data unchanged.")
            out_df = normalize_dtypes(working_df, file_config)
        else:
            step = "whodat"
            if should_run_whodat(working_df, file_config):
                working_df, whodat_stats = whodat_lookup_fnr(working_df, file_config)
                logger.info("WhoDat stats: %s", whodat_stats)
            else:
                logger.info("WhoDat: not configured or no helper variables available.")

            step = "pseudo_and_snr"
            pseudo_df, pseudo_stats = pseudo_and_snr(working_df, file_config)
            logger.info("Pseudo stats: %s", pseudo_stats)

            step = "cleanup"
            out_df = drop_work_columns(pseudo_df, file_config)
            out_df = _drop_original_fnr_columns(out_df, file_config)
            out_df = drop_sensitive_columns(out_df, file_config)

            step = "normalize_dtypes"
            out_df = normalize_dtypes(out_df, file_config)

            step = "validate_output"
            validate_output(
                df_in=working_df,
                df_out=out_df,
                file_config=file_config,
            )

        step = "write_parquet"
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
