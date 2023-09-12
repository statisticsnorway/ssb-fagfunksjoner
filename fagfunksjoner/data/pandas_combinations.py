"""The background for these functions is a common operation before publishing to the "statbank" at statistics Norway.
All combinations (including total-groups), over all categorical codes, in a set of columns, need to have their numbers aggregated.
This has some similar functionality to "proc means" in SAS.
"""
from typing import Dict, Callable

import pandas as pd
from itertools import combinations

def all_combos_agg(df: pd.DataFrame,
                  groupcols: list,
                  aggargs: Dict[str, Callable],
                  fillna_dict: dict = None,
                  keep_empty: bool = False,
                  grand_total: str = '',) -> pd.DataFrame:
    """ Generate all aggregation levels for a set of columns in a dataframe

        Parameters:
        -----------
            df: pandas.DataFrame
                dataframe to aggregate.
            groupcols: list[str]
                List of columns to group by.
            aggargs: Dict[str, Callable]
                how to aggregate, is sent to the agg function in pandas, look at its documentation.
            fillna_dict: Dict[str, str]
                Fills "totals" in the groupcols, by filling their NA values. 
                Send a dict with col names as keys, and string-values to put in cells as values.
            keep_empty: bool
                Keep groups without observations through the process.
                Removing them is default behaviour of Pandas
            grand_total: str
                Fill this value, if you want a grand total in your aggregations.
                This string will be input in the fields in the groupcol columns.


        Returns:
        --------
            dataframe with all the group-by columns, all the aggregation columns combined 
            with the aggregation functions, a column called aggregation_level which 
            separates the different aggregation levels, and a column called aggregation_ways which
            counts the number of group columns used for the aggregation.

        Known problems:
        ---------------
            You should not use dataframes with multi-index columns as they cause trouble.

        Examples:
        import pandas as pd
        from fagfunksjoner.data.pandas_combinations import all_combos_agg
        data = {
                'alder': [20, 60, 33, 33, 20],
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

        agg3 = all_combos_agg(pers, groupcols=['kjonn', 'alder'], grand_total=True,
                                                       grand_total='Grand total',
                                                       aggargs={'inntekt':['mean', 'sum']})
        display(agg3)
        agg4 = all_combos_agg(pers, groupcols=['kjonn', 'alder'], 
                               fillna_dict={'kjonn': 'Total kjønn', 'alder': 'Total alder'}, 
                               aggargs={'inntekt':['mean', 'sum'], 'formue': ['count', 'min', 'max']}, 
                               grand_total="Total"
                              )
        display(agg4)
        pers['antall'] = 1
        groupcols = pers.columns[0:3].tolist()
        func_dict = {'inntekt':['mean', 'sum'], 'formue': ['sum', 'std', 'count']}
        fillna_dict = {'kjonn': 'Total kjønn', 'alder': 'Total alder', 'kommune': 'Total kommune'}
        agg5 = all_combos_agg(pers, groupcols=groupcols, 
                               aggargs=func_dict,
                               fillna_dict=fillna_dict, 
                               grand_total="All"
                              )
        display(agg5)    """
        
    dataframe = df.copy()
    
    # Hack using categoricals to keep all unobserved groups
    if keep_empty:
        dataframe = dataframe.astype({col: "category" for col in groupcols})
    
    # Generate all possible combinations of group columns
    combos = []
    for r in range(len(groupcols) + 1, 0, -1):
        combos += list(combinations(groupcols, r))
    # Create an empty DataFrame to store the results
    all_levels = pd.DataFrame()

    # Calculate aggregates for each combination
    for i, comb in enumerate(combos):
        # Calculate statistics using groupby
        if keep_empty:
            # Hack using categoricals to keep all unobserved groups
            result = dataframe.groupby(list(comb), observed=False)
        else:
            result = dataframe.groupby(list(comb))

        result = result.agg(aggargs).reset_index(names=list(comb))

        # Add a column to differentiate the combinations
        result['level'] = len(combos) - i

        # Add a column with number of group columns used in the aggregation
        result['ways'] = int(len(comb))

        # Concatenate the current result with the combined results
        all_levels = pd.concat([all_levels, result], ignore_index=True)
    
    # Flatten the multindex columns if agg made some
    all_levels = flatten_col_multiindex(all_levels)
    
    # Calculate the grand total
    if grand_total:
        # Add category to categoricals
        cat_groupcols = df[groupcols].select_dtypes("category").columns
        if len(cat_groupcols):
            for col in cat_groupcols:
                all_levels[col] = all_levels[col].add_categories(grand_total)
        gt = dataframe.agg(aggargs)
        #return gt
        if isinstance(gt, pd.DataFrame):
            gt = gt.unstack()
        gt = flatten_col_multiindex(pd.DataFrame(gt).T)
        #display(gt)
        #return gt
        gt['level'] = 0
        gt['ways'] = 0
        gt[groupcols] = grand_total
        gt = gt[all_levels.columns]

        # Append the grand total row to the combined results and sort by levels and groupcols
        all_levels = pd.concat([all_levels, gt], ignore_index=True)
    all_levels = all_levels.sort_values(['level'] + groupcols)

    # Fill missing group columns with value
    if fillna_dict:
        all_levels = fill_na_dict(all_levels, fillna_dict)

    # Sett datatype tilbake til det den hadde i utgangpunktet
    if keep_empty:
        reset_types = {col: df[col].dtype.name for col in groupcols}
        all_levels = all_levels.astype(reset_types)
    return all_levels.reset_index(drop=True)


def fill_na_dict(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    df = df.copy()
    for col, fill_val in mapping.items():
        if df[col].dtype == "category":
            df[col] = df[col].cat.add_categories(fill_val)
        df[col] = df[col].fillna(fill_val)
    return df


def flatten_col_multiindex(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(col).strip().strip("_") for col in df.columns.values]
    return df