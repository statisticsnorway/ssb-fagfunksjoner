import pandas as pd


def make_percentage(
    df: pd.DataFrame,
    percent_col: str,
    fillna_dict: dict[str, str],
    value_col: str = "antall",
    decimals: int = 1,
) -> pd.DataFrame:
    """
    Calculate percentages using Total rows as denominators.

    Parameters
    ----------
    df : pd.DataFrame
        Aggregated dataframe containing Total rows.
    percent_col : str
        Dimension to calculate percentages for.
    fillna_dict : dict[str, str]
        Mapping from dimension names to their total labels.
    value_col : str, default="antall"
        Value column.
    decimals : int, default=1
        Number of decimals in percentage column.

    Returns
    -------
    pd.DataFrame
        DataFrame with percentage column added.
    """
    total_label = fillna_dict[percent_col]
    result = df.copy()

    group_cols = [
        c
        for c in result.columns
        if c not in {percent_col, value_col, "level", "ways"}
    ]

    totals = (
        result.loc[result[percent_col] == total_label]
        .rename(columns={value_col: "_total"})
        [group_cols + ["_total"]]
    )

    result = result.merge(
        totals,
        on=group_cols,
        how="left",
    )

    result["andel"] = (result[value_col] / result["_total"] * 100).round(decimals)

    return result.drop(columns="_total")