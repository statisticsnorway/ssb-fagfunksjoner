import pandas as pd

from .dtypes import normalize_dtypes
from .globals import DESTINATION_BUCKET, logger
from .pii import drop_sensitive_columns
from .pseudo import pseudo_and_snr
from .validate import (
    _has_person_data,
    assert_prepped_input,
    summarize_input,
    validate_output,
)
from .whodat import (
    _prepare_whodat_work_columns,
    drop_work_columns,
    should_run_whodat,
    whodat_lookup_fnr,
)
from .write import build_output_name


def run_kildomaten_pipeline(source_file: str) -> None:
    """Run the Kildomaten processing pipeline for one parquet file.

    Args:
        source_file: Source parquet path to process.

    Returns:
        None: The processed parquet file is written to the destination bucket.

    Raises:
        Exception: Re-raises any exception from the processing step that fails.
    """
    step = "start"
    try:
        logger.info("Starter kildomat: %s", source_file)

        step = "validate_input_file"
        if not source_file.endswith(".parquet"):
            logger.error("Ikke parquet: %s", source_file)
            return

        source_file_end = source_file.rsplit("/", 1)[-1]
        new_name = build_output_name(source_file_end, insert="_inndata_")
        output_path = "/".join([DESTINATION_BUCKET, "vgu", "inndata", new_name])
        logger.info("Output: %s", output_path)

        step = "read_parquet"
        df = pd.read_parquet(source_file, dtype_backend="pyarrow")
        df.columns = df.columns.str.strip().str.lower()
        logger.info("Lest input: rader=%d kolonner=%d", len(df), len(df.columns))

        step = "assert_prepped_input"
        assert_prepped_input(df)

        step = "summarize_input"
        logger.info("Input-oppsummering: %s", summarize_input(df))

        step = "drop_work_cols_if_any"
        df = drop_work_columns(df)

        if not _has_person_data(df):
            # Vei 1: ingen persondata (kodeverk, registerfiler)
            # Lagres direkte — ingen whodat, ingen pseudo, ingen snr/snr_mrk
            logger.info("Ingen persondata — lagres urørt.")
            out_df = normalize_dtypes(df)

        else:
            # Vei 2: har persondata — gjennom whodat (hvis mulig) og pseudo

            step = "whodat"
            if should_run_whodat(df):
                # Avled whodat-arbeidskolonner fra canonical navn
                df = _prepare_whodat_work_columns(df)
                df, whodat_stats = whodat_lookup_fnr(df)
                logger.info("WhoDat-stats: %s", whodat_stats)
            else:
                logger.info("WhoDat: ingen hjelpvariabler tilgjengelig — hopper over.")

            step = "pseudo_and_snr"
            df_pseudo, pseudo_stats = pseudo_and_snr(df)
            logger.info("Pseudo-stats: %s", pseudo_stats)

            step = "cleanup"
            out_df = drop_work_columns(df_pseudo)
            if "fnr_orig" in out_df.columns:
                out_df = out_df.drop(columns=["fnr_orig"])
            out_df = drop_sensitive_columns(out_df)

            step = "normalize_dtypes"
            out_df = normalize_dtypes(out_df)

            step = "validate_output"
            validate_output(df_in=df, df_out=out_df)

        step = "write_parquet"
        logger.info("Lagrer: rader=%d kolonner=%d", len(out_df), len(out_df.columns))
        out_df.to_parquet(output_path, engine="pyarrow", index=False)

        step = "done"
        logger.info("Ferdig: %s", source_file)

    except Exception as e:
        logger.exception("Feil i steg '%s': %s", step, e)
        raise
