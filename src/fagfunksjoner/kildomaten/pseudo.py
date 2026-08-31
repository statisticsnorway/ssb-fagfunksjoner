import pandas as pd
from dapla_pseudo import Pseudonymize

from .fileconfig import FileConfig
from .globals import logger
from .snr_uuid import _fill_uuid_for_missing_snr


def _valid_fnr_mask(df: pd.DataFrame, fnr_col: str) -> pd.Series:
    fnr = df[fnr_col].astype("string[pyarrow]").str.strip()
    return fnr.notna() & (fnr != "") & fnr.str.match(r"^\d{11}$")


def pseudo_and_snr(
    df: pd.DataFrame,
    file_config: FileConfig,
    *,
    dry_run: bool = False,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Pseudonymize FNR values into SNR values.

    Rows with a valid configured FNR are pseudonymized into a stable SNR. Rows
    without a valid FNR, or without a pseudonymization hit, receive a
    UUID-filled SNR and are marked with snr_mrk=True to indicate that the SNR
    is not stable across datasets.

    Args:
        df: Input DataFrame containing the configured FNR columns.
        file_config: File-specific processing configuration.
        dry_run: Whether to skip external pseudonymization calls.

    Returns:
        tuple[pd.DataFrame, dict]: The pseudonymized DataFrame and processing statistics.
    """
    if not file_config.fnr_col:
        return df.copy(), {
            "has_fnr_11digits": 0,
            "fnr_missing_or_invalid": len(df),
            "pseudo_ran": False,
            "snr_from_stable_id": 0,
            "snr_uuid_filled": 0,
            "pseudo_cols": {},
            "dry_run": dry_run,
        }

    df = df.copy()
    has_fnr = _valid_fnr_mask(df, file_config.fnr_col)
    pseudo_cols = [column for column in file_config.pseudo_cols if column in df.columns]

    stats = {
        "has_fnr_11digits": int(has_fnr.sum()),
        "fnr_missing_or_invalid": int((~has_fnr).sum()),
        "pseudo_ran": False,
        "snr_from_stable_id": 0,
        "snr_uuid_filled": 0,
        "pseudo_cols": {
            column: int(_valid_fnr_mask(df, column).sum()) for column in pseudo_cols
        },
        "dry_run": dry_run,
    }

    df[file_config.snr_col] = pd.Series(
        pd.NA,
        index=df.index,
        dtype="string[pyarrow]",
    )
    df[file_config.snr_mark_col] = pd.Series(
        False,
        index=df.index,
        dtype="bool[pyarrow]",
    )

    if stats["has_fnr_11digits"]:
        df.loc[has_fnr, file_config.snr_col] = (
            df.loc[has_fnr, file_config.fnr_col].astype("string[pyarrow]").str.strip()
        )

    if dry_run:
        logger.info(
            "Pseudo dry-run: skipping Pseudonymize service calls for columns=%s.",
            pseudo_cols,
        )
        df[file_config.snr_col] = pd.Series(
            pd.NA,
            index=df.index,
            dtype="string[pyarrow]",
        )
        df[file_config.snr_mark_col] = pd.Series(
            True,
            index=df.index,
            dtype="bool[pyarrow]",
        )
        return df, stats

    if not stats["has_fnr_11digits"] and not pseudo_cols:
        logger.info("Pseudo: no valid FNR values found, filling SNR with UUIDs.")
        df, n_uuid = _fill_uuid_for_missing_snr(
            df,
            snr_col=file_config.snr_col,
        )
        df[file_config.snr_mark_col] = pd.Series(
            True,
            index=df.index,
            dtype="bool[pyarrow]",
        )
        stats["snr_uuid_filled"] = n_uuid
        return df, stats

    process = Pseudonymize.from_pandas(df)
    if stats["has_fnr_11digits"]:
        process = process.on_fields(file_config.snr_col).with_stable_id()
    for column in pseudo_cols:
        process = process.on_fields(column).with_papis_compatible_encryption()

    res = process.run().to_pandas()
    stats["pseudo_ran"] = True

    res[file_config.snr_col] = (
        res[file_config.snr_col].astype("string[pyarrow]").str.strip()
    )
    good_snr = (
        res[file_config.snr_col].notna()
        & (res[file_config.snr_col] != "")
        & (res[file_config.snr_col].str.len() == 7)
    )
    stats["snr_from_stable_id"] = int(good_snr.sum())
    res.loc[~good_snr, file_config.snr_col] = pd.NA

    miss_before = res[file_config.snr_col].isna()
    res, n_uuid = _fill_uuid_for_missing_snr(
        res,
        snr_col=file_config.snr_col,
    )
    stats["snr_uuid_filled"] = n_uuid
    res[file_config.snr_mark_col] = miss_before.astype("bool[pyarrow]")

    return res, stats
