"""The background for these functions is a common operation before publishing to the "statbank" at statistics Norway.

Returns:
        pd.DataFrame: The dataframe with calculated aggregates for each combination.

All combinations (including total-groups), over all categorical codes, in a set of columns, need to have their numbers aggregated.
This has some similar functionality to "proc means" in SAS.
"""

import copy
from collections.abc import Callable, Hashable, Mapping
from itertools import combinations, product
from typing import Any, TypeAlias, TypeVar, cast

import numpy as np
import pandas as pd

from fagfunksjoner.fagfunksjoner_logger import logger


# Having trouble importing these from pandas._typing
AggFuncTypeBase: TypeAlias = Callable[[Any], Any] | str | np.ufunc
HashableT = TypeVar("HashableT", bound=Hashable)
AggFuncTypeDictSeries: TypeAlias = Mapping[HashableT, AggFuncTypeBase]


def all_combos_agg(
    df: pd.DataFrame,
    groupcols: list[str],
    valuecols: list[str] | None = None,
    aggargs: (
        AggFuncTypeBase | AggFuncTypeDictSeries[str] | dict[str, list[str]] | None
    ) = None,
    fillna_dict: dict[str, Any] | None = None,
    keep_empty: bool = False,
    grand_total: dict[str, str] | str = "",
) -> pd.DataFrame:
    """Generate all aggregation levels for a set of columns in a dataframe.

    Creates aggregations over all combinations of categorical variables specified in `groupcols`
    and applies aggregation functions on `valuecols`. Allows for inclusion of grand totals
    and customized fill values for missing groups, similar to "proc means" in SAS.

    Args:
        df: DataFrame to aggregate.
        groupcols: List of columns to group by.
        valuecols: List of columns to apply aggregation functions on. Defaults to None, in which case all numeric columns are used.
        aggargs: Dictionary or function specifying aggregation for each column in `valuecols`. If None, defaults to 'sum' for each column in `valuecols`.
        fillna_dict: Dictionary specifying values to fill NA in each column of `groupcols`. Useful for indicating totals in the final table.
        keep_empty: If True, preserves empty groups in the output.
        grand_total: Dictionary or string to indicate a grand total row. If a dictionary, the values are applied in each corresponding `groupcols`.

    Returns:
        DataFrame with all aggregation levels, including:
            - `groupcols`: group-by columns with filled total values as needed.
            - `level`: indicates aggregation level.
            - `ways`: counts the number of grouping columns used for each aggregation.

    Examples:
        >>> data = pd.DataFrame({
                'age': [20, 60, 33, 33, 20],
                'region': ['0301', '3001', '0301', '5401', '0301'],
                'gender': ['1', '2', '1', '2', '2'],
                'income': [1000000, 120000, 220000, 550000, 50000],
                'wealth': [25000, 50000, 33000, 44000, 90000]
            })
        >>> all_combos_agg(data, groupcols=['gender', 'age'], aggargs={'income': ['mean', 'sum']})
    """
    df_cols, aggdict = check_column_arguments(
        df=df, groupcols=groupcols, valuecols=valuecols, aggargs=aggargs
    )
    dataframe = prepare_dataframe(df=df, groupcols=groupcols, collist=df_cols)
    combinations = prepare_combinations(groupcols=groupcols)
    calculated_aggregates = calculate_aggregates(
        df=dataframe, combos=combinations, aggargs=aggdict, keep_empty=keep_empty
    )
    final_df = finalize_dataframe(
        all_levels=calculated_aggregates,
        df=dataframe,
        groupcols=groupcols,
        aggargs=aggdict,
        grand_total=grand_total,
        fillna_dict=fillna_dict,
        keep_empty=keep_empty,
    )
    return final_df


def check_column_arguments(
    df: pd.DataFrame,
    groupcols: list[str],
    valuecols: list[str] | None = None,
    aggargs: (
        AggFuncTypeBase | AggFuncTypeDictSeries[str] | dict[str, list[str]] | None
    ) = None,
) -> tuple[
    list[str], AggFuncTypeBase | AggFuncTypeDictSeries[str] | dict[str, list[str]]
]:
    """Validate and set defaults for grouping and aggregation arguments.

    Confirms that columns in `groupcols` and `valuecols` exist in `df`, assigns default
    aggregations if none are provided, and ensures all columns are numeric if aggregations
    are unspecified.

    Args:
        df: The input DataFrame to check.
        groupcols: List of column names to group by.
        valuecols: List of columns to aggregate. Defaults to None, in which case all numeric columns are used or the keys of `aggargs` if provided.
        aggargs: Aggregation functions for `valuecols`. Defaults to 'sum' for all numeric columns.

    Returns:
        - required_columns: List of columns needed for grouping and aggregation.
        - aggargs: Updated aggregation functions for each column in `valuecols`.

    Raises:
        ValueError: If a column in `groupcols` or `valuecols` is not in `df`. Or
            if any column in `valuecols` is non-numeric and lacks an aggregation function.

    Example:
        >>> data = pd.DataFrame({
                'A': [1, 2, 3],
                'B': [4, 5, 6],
                'C': [7, 8, 9]
            })
        >>> check_column_arguments(data, groupcols=['A'], valuecols=['B', 'C'])
        (['A', 'B', 'C'], {'B': 'sum', 'C': 'sum'})
    """
    default_aggregation = "sum"
    numeric_columns = list(df.select_dtypes(include=[np.number]).columns)

    if valuecols is None:
        if isinstance(aggargs, dict) and aggargs:
            valuecols = list(aggargs.keys())
        elif aggargs is None:
            valuecols = numeric_columns
        else:
            logger.warning(f"Did not find which columns to aggregate from: {aggargs}.")
            valuecols = []

    required_columns = groupcols + valuecols  # No error expected here now
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Columns {', '.join(missing_columns)} are not present in the dataframe!"
        )

    if aggargs is None:
        non_numeric_cols = [col for col in valuecols if col not in numeric_columns]
        if non_numeric_cols:
            raise ValueError(
                f"Columns {', '.join(non_numeric_cols)} in aggregation function are not numeric! Specify aggregation functions accordingly."
            )
        aggargs = {col: default_aggregation for col in valuecols}

    return required_columns, aggargs


def prepare_dataframe(
    df: pd.DataFrame, groupcols: list[str], collist: list[str]
) -> pd.DataFrame:
    """Prepare DataFrame by selecting necessary columns and setting empty groups.

    Args:
        df: The dataframe to process.
        groupcols: List of columns to group by.
        collist: List of all required columns for aggregation.

    Returns:
        The DataFrame with required columns, optionally converted to category dtype.
    """
    dataframe = df[collist].copy()
    dataframe = dataframe.astype({col: "category" for col in groupcols})

    return dataframe


def prepare_combinations(groupcols: list[str]) -> list[tuple[str, ...]]:
    """Generate all possible combinations of group columns.

    Args:
        groupcols: List of columns to group by.

    Returns:
        List of tuples representing all group column combinations.
    """
    combos = [
        tuple(combo)
        for r in range(len(groupcols) + 1, 0, -1)
        for combo in combinations(groupcols, r)
    ]

    return combos


def calculate_aggregates(
    df: pd.DataFrame,
    combos: list[tuple[str, ...]],
    aggargs: AggFuncTypeBase | AggFuncTypeDictSeries[str] | dict[str, list[str]],
    keep_empty: bool,
) -> pd.DataFrame:
    """Calculate aggregates for each combination of group columns.

    Args:
        df: The dataframe to aggregate.
        combos: List of group column combinations.
        aggargs: Aggregation functions to apply.
        keep_empty: Whether to keep groups without observations.

    Returns:
        pd.DataFrame: The dataframe with calculated aggregates for each combination.
    """
    all_levels = pd.DataFrame()

    for i, comb in enumerate(combos):
        result = (
            df.groupby(list(comb), observed=not keep_empty)
            .agg(aggargs)  # type: ignore[arg-type]
            .reset_index(names=list(comb))
        )
        result["level"] = len(combos) - i
        result["ways"] = len(comb)
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
        all_levels: The dataframe with calculated aggregates.
        df: The original dataframe.
        groupcols: List of columns to group by.
        aggargs: Aggregation functions to apply.
        grand_total: Value(s) to use for the grand total row.
        fillna_dict: Values to fill in missing data.
        keep_empty: Whether to keep groups without observations.

    Returns:
        pd.DataFrame: Final DataFrame with all aggregations and filled values.
    """
    all_levels = flatten_col_multiindex(all_levels)

    if grand_total:
        all_levels = handle_grand_total(all_levels, df, groupcols, grand_total, aggargs)

    all_levels = all_levels.sort_values(["level", *groupcols])

    if fillna_dict:
        all_levels = fill_na_dict(all_levels, fillna_dict)

    if keep_empty:
        reset_types = {col: df[col].dtype.name for col in groupcols}
        all_levels = all_levels.astype(reset_types)

    return all_levels.reset_index(drop=True)


def handle_grand_total(
    all_levels: pd.DataFrame,
    df: pd.DataFrame,
    groupcols: list[str],
    grand_total: dict[str, str] | str,
    aggargs: AggFuncTypeBase | AggFuncTypeDictSeries[str] | dict[str, list[str]],
) -> pd.DataFrame:
    """Handle the totals of groupcols, in addition to a grand total for the whole dataset?

    Args:
        all_levels: The inherited dataset from the previous step.
        df: The original dataframe.
        groupcols: List of columns to group by.
        grand_total: Value(s) to use for the grand total row.
        aggargs: Aggregation functions to apply.

    Returns:
        pd.DataFrame: The modified original dataset that now should contain the grand totals.

    Raises:
        ValueError: If 'grand_total' is not a string or a dictionary.
        TypeError: If the result of the aggregation is not a pandas series or dataframe.
    """
    for col in groupcols:
        if isinstance(grand_total, dict):
            if col in grand_total:
                all_levels[col] = all_levels[col].cat.add_categories([grand_total[col]])
            else:
                all_levels[col] = all_levels[col].cat.add_categories([grand_total])
        else:
            all_levels[col] = all_levels[col].astype("object")

    gt: pd.Series | pd.DataFrame = df.agg(aggargs)  # type: ignore[type-arg, arg-type]

    if isinstance(gt, pd.Series):
        gt_df = flatten_col_multiindex(gt.to_frame().T)
    elif isinstance(gt, pd.DataFrame):
        gt_df = _unstack_agg_dataframe(gt)
    else:
        raise TypeError(
            "Excpecting the aggregated data to be a pandas series or a dataframe at this point."
        )

    gt_df["level"] = 0
    gt_df["ways"] = 0

    if isinstance(grand_total, str):
        for col in groupcols:
            gt_df[col] = grand_total
        gt_df = gt_df[all_levels.columns]
    elif isinstance(grand_total, dict):
        for col in groupcols:
            gt_df[col] = grand_total.get(col, None)
        gt_df = gt_df[all_levels.columns]
    else:
        raise ValueError("grand_total must be a string or a dictionary")

    all_levels = pd.concat([all_levels, gt_df], ignore_index=True)

    return all_levels


def _unstack_agg_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    unstack = cast(Callable[[], pd.Series], df.unstack)
    return flatten_col_multiindex(unstack().to_frame().T)


def fill_na_dict(df: pd.DataFrame, mapping: dict[str, Any]) -> pd.DataFrame:
    """Fills NAs in the passed dataframe with a dict.

    Keys in dict should be column names, the values what should be inputed in the cells.
    Also handles categorical columns if they exist in the dataframe.

    Args:
        df: The DataFrame to fill NAs on.
        mapping: What each of the columns should have their NAs filled with.

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
        df: The DataFrame with multiindexed columns.
        sep: What should seperate the names of the levels in the multiindex. Defaults to "_".

    Returns:
        pd.DataFrame: The DataFrame with the flattened column headers.
    """
    if isinstance(df.columns, pd.MultiIndex):
        pd.Index(
            [
                (
                    sep.join(filter(None, map(str, col))).strip(sep)
                    if isinstance(col, tuple)
                    else str(col)
                )
                for col in df.columns.values
            ]
        )
    return df


def parse_mapping(
    mapping: list[Any] | str | Any, x: pd.Series  # type: ignore
) -> list[Any] | str | Any:
    """Parse the input mapping from `category_mappings`.

    Basically just returns `mapping` if it is not the string "__ALL__".

    Args:
        mapping: The mapping to parse.
        x: The Series to use for the mapping.

    Returns:
        list[Any] | str: The parsed mapping.
    """
    if isinstance(mapping, str) and mapping == "__ALL__":
        return list(x.unique())
    else:
        return mapping


def all_combos_agg_inclusive(
    df: pd.DataFrame,
    groupcols: None | list[str] = None,
    category_mappings: None | dict[str, dict[str, list[Any] | str] | Any] = None,
    valuecols: None | list[str] = None,
    aggargs: None | dict[str, Any] | Callable[..., Any] | str | list[Any] = None,
    totalcodes: None | dict[str, str] = None,
    keep_empty: bool = False,
    grand_total: bool = True,
) -> pd.DataFrame:
    """Generate all aggregation levels for a set of columns in a dataframe, for non-exclusive categories.

    Creates aggregations over all combinations of categorical variables specified in `groupcols`
    and applies aggregation functions on `valuecols`. Allows for inclusion of grand totals
    and customized fill values for missing groups. It is basically a more general version of the
    `all_combos_agg` function, allowing for inclusive (non-exclusive) categories. Inclusive categories are
    defined by a dictionary of mappings in `category_mappings`. Variables
    in `groupcols` are assumed to be categorical, and their categories are treated as mutually exclusive.

    Args:
        df: DataFrame to aggregate.
        groupcols: List of columns to group by.
        category_mappings: Dictionary of dictionaries, where each key is a column name and each value is a dictionary of mappings.
            '__ALL__' can be used to indicate 'all values' in a column, and is used for totals.
        valuecols: List of columns to apply aggregation functions on. Defaults to None, in which case all numeric columns are used.
        aggargs: Dictionary or function specifying aggregation for each column in `valuecols`. If None, defaults to 'sum' for each column in `valuecols`.
        totalcodes: Dictionary specifying values to use as labels representing totals in each column.
        keep_empty: If True, preserves empty groups in the output.
        grand_total: Dictionary or string to indicate a grand total row. If a dictionary, the values are applied in each corresponding `groupcols`.

    Raises:
        ValueError: If a column in `groupcols` is not found in the DataFrame.

    Returns:
        pd.DataFrame: DataFrame with all aggregation levels for the specified columns.

    Examples:
        >>> # Define the categorical bins based on the metadata
        >>> gender_bins = {"1": "Menn", "2": "Kvinner"}

        >>> # Generate synthetic data
        >>> np.random.seed(42)
        >>> num_samples = 100

        >>> synthetic_data = pd.DataFrame({
                "Tid": np.random.choice(["2021", "2022", "2023"], num_samples),
                "UtdanningOppl": np.random.choice(list(range(1,19)), num_samples),
                "Kjonn": np.random.choice(list(gender_bins.keys()), num_samples),
                "Alder": np.random.randint(15, 67, num_samples),  # Ages between 15 and 66
                "syss_student": np.random.choice(["01", "02", "03", "04"], num_samples),
                "n": 1
            })

        >>> category_mappings = {
                "Alder": {
                    "15-24": range(15, 25),
                    "25-34": range(25, 35),
                    "35-44": range(35, 45),
                    "45-54": range(45, 55),
                    "55-66": range(55, 67),
                    "15-21": range(15, 22),
                    "22-30": range(22, 31),
                    "31-40": range(31, 41),
                    "41-50": range(41, 51),
                    "51-66": range(51, 67),
                    "15-30": range(15, 31),
                    "31-45": range(31, 46),
                    "46-66": range(46, 67),
                },
                "syss_student": {
                    "01": ["01", "02"],
                    "02": ["03", "04"],
                    "03": ["02"],
                    "04": ["04"],
                },
                "Kjonn": {
                    "Menn": ["1"],
                    "Kvinner": ["2"],
                }
            }

        >>> totalcodes = {
                    "Alder": "Total",
                    "syss_student": "Total",
                    "Kjonn": "Begge"
            }

        >>> all_combos_agg_inclusive(synthetic_data,
                                         groupcols = [],
                                         category_mappings=category_mappings,
                                         totalcodes=totalcodes,
                                         valuecols = ["n"],
                                         aggargs={"n": "sum"},
                                         grand_total=True)
    """
    groupcols = groupcols or []
    valuecols = valuecols or []
    aggargs = aggargs or {}
    totalcodes = totalcodes or {}
    category_mappings = category_mappings or {}

    # create copies of mutable inputs which are modified
    totalcodes = copy.deepcopy(totalcodes)
    category_mappings = copy.deepcopy(category_mappings)

    for col in groupcols:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame.")

        items: list[Any] = list(df[col].unique())
        keys: list[str] = [str(x) for x in items]
        values: list[list[Any]] = [[x] for x in items]
        mapping: dict[str, list[Any] | str] = dict(zip(keys, values, strict=True))

        if col not in category_mappings.keys():
            category_mappings[col] = mapping

    all_cols: list[str] = valuecols + list(category_mappings.keys())
    df = df.copy()[all_cols]

    if totalcodes:
        for var, code in totalcodes.items():
            category_mappings[var][code] = "__ALL__"

    pivot_vars: list[str] = list(category_mappings.keys())
    pivot_names: dict[str, list[str]] = {}
    all_pivot_names: list[str] = []

    # fill in default for the rest, used for `grand_total`
    for var in pivot_vars:
        if var not in totalcodes.keys():
            totalcodes[var] = "Total"

    for var in category_mappings.keys():
        ncat: int = len(category_mappings[var])

        pivot_names[var] = ["__" + var + "__" + str(i) for i in range(ncat)]
        all_pivot_names = all_pivot_names + pivot_names[var]

        x: pd.Series = df[var].astype("str")  # type: ignore
        x[:] = "__NA__"  # using pd.NA does not work as intended with .melt()

        for i, pairmap in enumerate(category_mappings[var].items()):
            y: pd.Series = x.copy()  # type: ignore
            newval: str = pairmap[0]
            oldvals: list[Any] | str | Any = parse_mapping(pairmap[1], x=df[var])
            pivot_name: str = pivot_names[var][i]

            y.loc[df[var].isin(oldvals)] = newval
            df[pivot_name] = y

    tbl: pd.DataFrame = df.groupby(all_pivot_names).agg(aggargs).reset_index()

    id_vars: set[str] = set(all_pivot_names + valuecols)
    for var in category_mappings.keys():
        id_vars = id_vars - set(pivot_names[var])
        tbl = tbl.melt(
            id_vars=list(id_vars),
            value_vars=pivot_names[var],
            var_name="__variable__",
            value_name=var,
        )
        tbl = tbl.loc[tbl[var] != "__NA__", :].drop(labels=["__variable__"], axis=1)
        id_vars = id_vars.union([var])

    tbl = tbl.groupby(pivot_vars).agg(aggargs).reset_index()

    if grand_total:
        total_df: pd.DataFrame = df.copy()

        for var in pivot_vars:
            total_df[var] = totalcodes[var]
        tbl = pd.concat((tbl, total_df.groupby(pivot_vars).agg(aggargs).reset_index()))
        tbl = tbl.reset_index(
            drop=True
        ).drop_duplicates()  # drop duplicates if grand_total is already in the data

    if keep_empty:
        all_combos: list[Any] = list(product(*[tbl[v].unique() for v in pivot_vars]))
        all_combos_df: pd.DataFrame = pd.DataFrame(
            np.array(all_combos), columns=pivot_vars
        )

        tbl = pd.merge(all_combos_df, tbl, on=pivot_vars, how="left")

    return tbl
