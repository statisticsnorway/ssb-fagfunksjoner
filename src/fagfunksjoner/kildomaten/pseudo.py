import pandas as pd
from dapla_pseudo import Pseudonymize

from .globals import CANON_FNR, CANON_SNR, CANON_SNR_MRK, logger
from .snr_uuid import _fill_uuid_for_missing_snr


def pseudo_and_snr(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Pseudonymize FNR values into SNR values.

    Rows with a valid FNR are pseudonymized into a stable SNR. Rows without a
    valid FNR, or without a pseudonymization hit, receive a UUID-filled SNR and
    are marked with snr_mrk=True to indicate that the SNR is not stable across
    datasets.

    Args:
        df: Input DataFrame containing the canonical FNR column.

    Returns:
        tuple[pd.DataFrame, dict]: The pseudonymized DataFrame and processing statistics.
    """
    df = df.copy()

    fnr = df[CANON_FNR].astype("string[pyarrow]").str.strip()
    has_fnr = fnr.notna() & (fnr != "") & fnr.str.match(r"^\d{11}$")

    stats = {
        "has_fnr_11digits": int(has_fnr.sum()),
        "fnr_missing_or_invalid": int((~has_fnr).sum()),
        "pseudo_ran": False,
        "snr_from_stable_id": 0,
        "snr_uuid_filled": 0,
    }

    df[CANON_SNR] = pd.Series(pd.NA, index=df.index, dtype="string[pyarrow]")
    df[CANON_SNR_MRK] = pd.Series(False, index=df.index, dtype="bool[pyarrow]")

    if stats["has_fnr_11digits"] == 0:
        logger.info("Pseudo: ingen gyldige fnr → UUID-fyll for alle.")
        df, n_uuid = _fill_uuid_for_missing_snr(df)
        df[CANON_SNR_MRK] = pd.Series(True, index=df.index, dtype="bool[pyarrow]")
        stats["snr_uuid_filled"] = n_uuid
        return df, stats

    # Sett fnr som snr-input for rader med gyldig fnr
    df.loc[has_fnr, CANON_SNR] = fnr.loc[has_fnr]

    res = (
        Pseudonymize.from_pandas(df)
        .on_fields(CANON_SNR)
        .with_stable_id()
        .on_fields(CANON_FNR)
        .with_papis_compatible_encryption()
        .run()
        .to_pandas()
    )
    stats["pseudo_ran"] = True

    res[CANON_SNR] = res[CANON_SNR].astype("string[pyarrow]").str.strip()
    good_snr = (
        res[CANON_SNR].notna()
        & (res[CANON_SNR] != "")
        & (res[CANON_SNR].str.len() == 7)
    )
    stats["snr_from_stable_id"] = int(good_snr.sum())
    res.loc[~good_snr, CANON_SNR] = pd.NA

    # UUID-fyll for rader uten gyldig snr etter pseudo
    miss_before = res[CANON_SNR].isna()
    res, n_uuid = _fill_uuid_for_missing_snr(res)
    stats["snr_uuid_filled"] = n_uuid

    # snr_mrk=True kun for UUID-fylte rader
    res[CANON_SNR_MRK] = miss_before.astype("bool[pyarrow]")

    return res, stats
