"""The background for these functions is a common operation before publishing to the "statbank" at statistics Norway.

All combinations (including total-groups), over all categorical codes, in a set of columns, need to have their numbers aggregated.
This has some similar functionality to "proc means" in SAS.
"""

from collections.abc import Callable, Hashable, Mapping
from itertools import combinations
from typing import Any, TypeAlias, TypeVar

import numpy as np
import pandas as pd


# Having trouble importing these from pandas._typing
AggFuncTypeBase: TypeAlias = Callable[[Any], Any] | str | np.ufunc
HashableT = TypeVar("HashableT", bound=Hashable)
AggFuncTypeDictSeries: TypeAlias = Mapping[HashableT, AggFuncTypeBase]


def all_combos_agg(
    df: pd.DataFrame,
    groupcols: list[str],
    aggargs: AggFuncTypeBase | AggFuncTypeDictSeries[str] | dict[str, list[str]],
    fillna_dict: dict[str, Any] | None = None,
    keep_empty: bool = False,
    grand_total: dict[str, str] | str = "",
) -> pd.DataFrame:
    """Generate all aggregation levels for a set of columns in a dataframe.

    Args:
        df (pd.DataFrame): dataframe to aggregate.
        groupcols (list[str]): List of columns to group by.
        aggargs (AggFuncTypeBase | AggFuncTypeDictSeries): how to aggregate, is sent to the agg function in pandas, look at its documentation.
        fillna_dict (dict[str, str]): Fills "totals" in the groupcols, by filling their NA values.
            Send a dict with col names as keys, and string-values to put in cells as values.
        keep_empty (bool): Keep groups without observations through the process.
            Removing them is default behaviour of Pandas
        grand_total (str | dict[str|str]): Fill this value, if you want a grand total in your aggregations.
            If you use a string, this will be input in the fields in the groupcol columns.
            If you send a dict, like to the fillna_dict parameter, the values in the cells in the grand_total will reflect the values in the dict.

    Returns:
        pd.DataFrame: with all the group-by columns, all the aggregation columns combined
            with the aggregation functions, a column called aggregation_level which
            separates the different aggregation levels, and a column called aggregation_ways which
            counts the number of group columns used for the aggregation..

    Known problems:
        You should not use dataframes with multi-index columns as they cause trouble.

    Examples::

        import pandas as pd
        from fagfunksjoner.data.pandas_combinations import all_combos_agg

        data = {'alder': [20, 60, 33, 33, 20],
                'kommune': ['0301', '3001', '0301', '5401', '0301'],
                'kjonn': ['1', '2', '1', '2', '2'],
                'inntekt': [1000000, 120000, 220000, 550000, 50000],
                'formue': [25000, 50000, 33000, 44000, 90000]
                }

        pers = pd.DataFrame(data)

        agg1 = all_combos_agg(pers, groupcols=['kjonn'], keep_empty=True, aggargs={'inntekt':['mean', 'sum']})
        display(agg1)

        agg2 = all_combos_agg(pers, groupcols=['kjonn', 'alder'], aggargs={'inntekt':['mean', 'sum']})
        display(agg2)

        agg3 = all_combos_agg(pers, groupcols=['kjonn', 'alder'], grand_total=True, grand_total='Grand total', aggargs={'inntekt':['mean', 'sum']})
        display(agg3)
        agg4 = all_combos_agg(pers, groupcols=['kjonn', 'alder'], fillna_dict={'kjonn': 'Total kjønn', 'alder': 'Total alder'}, aggargs={'inntekt':['mean', 'sum'], 'formue': ['count', 'min', 'max']}, grand_total="Total")
        display(agg4)
        pers['antall'] = 1
        groupcols = pers.columns[0:3].tolist()
        func_dict = {'inntekt':['mean', 'sum'], 'formue': ['sum', 'std', 'count']}
        fillna_dict = {'kjonn': 'Total kjønn', 'alder': 'Total alder', 'kommune': 'Total kommune'}
        agg5 = all_combos_agg(pers, groupcols=groupcols, aggargs=func_dict, fillna_dict=fillna_dict, grand_total=fillna_dict )
        display(agg5)
    """
    dataframe, combos = prepare_combinations(df, groupcols, keep_empty)
    all_levels = calculate_aggregates(dataframe, combos, aggargs, keep_empty)
    final_df = finalize_dataframe(
        all_levels, df, groupcols, aggargs, grand_total, fillna_dict, keep_empty
    )
    return final_df


def prepare_combinations(
    df: pd.DataFrame, groupcols: list[str], keep_empty: bool
) -> tuple[pd.DataFrame, list[tuple[str, ...]]]:
    """Prepare the dataframe and generate all possible combinations of group columns.

    Args:
        df (pd.DataFrame): The dataframe to process.
        groupcols (list[str]): List of columns to group by.
        keep_empty (bool): Whether to keep groups without observations.

    Returns:
        tuple[pd.DataFrame, list[tuple[str]]]: The prepared dataframe and list of group column combinations.
    """
    dataframe = df.copy()
    if keep_empty:
        dataframe = dataframe.astype({col: "category" for col in groupcols})

    combos: list[tuple[str, ...]] = []
    for r in range(len(groupcols) + 1, 0, -1):
        combos += [tuple(combo) for combo in combinations(groupcols, r)]

    return dataframe, combos


def calculate_aggregates(
    df: pd.DataFrame,
    combos: list[tuple[str, ...]],
    aggargs: AggFuncTypeBase | AggFuncTypeDictSeries[str] | dict[str, list[str]],
    keep_empty: bool,
) -> pd.DataFrame:
    """Calculate aggregates for each combination of group columns.

    Args:
        df (pd.DataFrame): The dataframe to aggregate.
        combos (list[tuple[str]]): List of group column combinations.
        aggargs (AggFuncTypeBase | AggFuncTypeDictSeries[str] | dict[str, list[str]]): Aggregation functions to apply.
        keep_empty (bool): Whether to keep groups without observations.

    Returns:
        pd.DataFrame: The dataframe with calculated aggregates for each combination.
    """
    all_levels = pd.DataFrame()

    for i, comb in enumerate(combos):
        if keep_empty:
            result_grps = df.groupby(list(comb), observed=False)
        else:
            result_grps = df.groupby(list(comb))

        result = result_grps.agg(aggargs).reset_index(names=list(comb))  # type: ignore[arg-type]
        result["level"] = len(combos) - i
        result["ways"] = int(len(comb))
        all_levels = pd.concat([all_levels, result], ignore_index=True)

    return all_levels


def finalize_dataframe(
    all_levels: pd.DataFrame,
    df: pd.DataFrame,
    groupcols: list[str],
    aggargs: AggFuncTypeBase | AggFuncTypeDictSeries[str] | dict[str, list[str]],
    grand_total: dict[str, str] | str,
    fillna_dict: dict[str, Any] | None,
    keep_empty: bool,
) -> pd.DataFrame:
    """Finalize the dataframe by calculating the grand total and filling missing values.

    Args:
        all_levels (pd.DataFrame): The dataframe with calculated aggregates.
        df (pd.DataFrame): The original dataframe.
        groupcols (list[str]): List of columns to group by.
        aggargs (AggFuncTypeBase | AggFuncTypeDictSeries[str] | dict[str, list[str]]): Aggregation functions to apply.
        grand_total (dict[str, str] | str): Value(s) to use for the grand total row.
        fillna_dict (dict[str, Any] | None): Values to fill in missing data.
        keep_empty (bool): Whether to keep groups without observations.

    Returns:
        pd.DataFrame: The finalized dataframe.

    Raises:
        ValueError: On sending in a grand_total-parameter we dont understand, not a string or a dict
    """
    all_levels = flatten_col_multiindex(all_levels)

    if grand_total:
        cat_groupcols = df[groupcols].select_dtypes("category").columns
        if len(cat_groupcols):
            for col in cat_groupcols:
                all_levels[col] = all_levels[col].add_categories(grand_total)

        gt: pd.Series | pd.DataFrame = df.agg(aggargs)  # type: ignore[type-arg, arg-type]
        if isinstance(gt, pd.DataFrame):
            gt_df = flatten_col_multiindex(pd.DataFrame(gt.unstack()).T)
        else:
            gt_df = flatten_col_multiindex(pd.DataFrame(gt).T)

        gt_df["level"] = 0
        gt_df["ways"] = 0
        if isinstance(grand_total, str):
            gt_df[groupcols] = grand_total
        elif isinstance(grand_total, dict):
            for col, val in grand_total.items():
                gt_df[col] = val
        else:
            err = "Invalid grand_total argument"
            raise ValueError(err)

        gt_df = gt_df[all_levels.columns]
        all_levels = pd.concat([all_levels, gt_df], ignore_index=True)

    all_levels = all_levels.sort_values(["level", *groupcols])

    if fillna_dict:
        all_levels = fill_na_dict(all_levels, fillna_dict)

    if keep_empty:
        reset_types = {col: df[col].dtype.name for col in groupcols}
        all_levels = all_levels.astype(reset_types)

    return all_levels.reset_index(drop=True)


def fill_na_dict(df: pd.DataFrame, mapping: dict[str, Any]) -> pd.DataFrame:
    """Fills NAs in the passed dataframe with a dict.

    Keys in dict should be column names, the values what should be inputed in the cells.
    Also handles categorical columns if they exist in the dataframe.

    Args:
        df (pd.DataFrame): The DataFrame to fill NAs on.
        mapping (dict): What each of the columns should have their NAs filled with.

    Returns:
        pd.DataFrame: The DataFrame with filled NAs.
    """
    df = df.copy()
    for col, fill_val in mapping.items():
        if df[col].dtype == "category":
            df[col] = df[col].cat.add_categories(fill_val)
        df[col] = df[col].fillna(fill_val)
    return df


def flatten_col_multiindex(df: pd.DataFrame, sep: str = "_") -> pd.DataFrame:
    """If the dataframe has a multiindex as a column.

    Flattens it by combining the names of the multiindex, using the seperator (sep).

    Args:
        df (pd.DataFrame): The DataFrame with multiindexed columns.
        sep (str): What should seperate the names of the levels in the multiindex. Defaults to "_".

    Returns:
        pd.DataFrame: The DataFrame with the flattened column headers.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = pd.Index(
            [sep.join(col).strip().strip(sep) for col in df.columns.values]
        )
    return df
