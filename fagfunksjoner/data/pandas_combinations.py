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
                  grand_total: bool = False,
                  grand_total_text: str = 'Total',) -> pd.DataFrame:
    """ Generate all aggregation levels for a set of columns in a dataframe

        Parameters:
        -----------
            df: dataframe to aggregate.
            groupcols: List of columns to group by.
            fillna_dict: Dict. 
            *aggargs: aggregation arguments.
            *kwargs: other arguments.


        Returns:
        --------
            dataframe with all the group-by columns, all the aggregation columns combined 
            with the aggregation functions, a column called aggregation_level which 
            separates the different aggregation levels, and a column called aggregation_ways which
            counts the number of group columns used for the aggregation.

        Advice:
        -------
            Make sure that you don't have any values in the group columns that are the same as your fillna value.

        Known problems:
        ---------------
            You should not use dataframes with multi-index columns as they cause trouble.

        Examples:
        data = {
                'alder': [20, 60, 33, 33, 20],
                'kommune': ['0301', '3001', '0301', '5401', '0301'],
                'kjonn': ['1', '2', '1', '2', '2'],
                'inntekt': [1000000, 120000, 220000, 550000, 50000],
                'formue': [25000, 50000, 33000, 44000, 90000]
            }
        pers = pd.DataFrame(data)

        agg1 = pandas_combinations_lono.all_combos_agg(pers, groupcols=['kjonn'], keep_empty=True, func={'inntekt':['mean', 'sum']})
        display(agg1)

        agg2 = pandas_combinations_lono.all_combos_agg(pers, groupcols=['kjonn', 'alder'], func={'inntekt':['mean', 'sum']})
        display(agg2)

        agg3 = pandas_combinations_lono.all_combos_agg(pers, groupcols=['kjonn', 'alder'], grand_total=True,
                                                       grand_total_text='Grand total',
                                                       func={'inntekt':['mean', 'sum']})
        display(agg3)
        agg4 = pandas_combinations_lono.all_combos_agg(pers, groupcols=['kjonn', 'alder'], 
                                                       fillna_dict={'kjonn': 'Total kjønn', 'alder': 'Total alder'}, 
                                                       func={'inntekt':['mean', 'sum'], 'formue': ['count', 'min', 'max']}, 
                                                       grand_total=True
                                                      )
        display(agg4)        
        pers['antall'] = 1
        groupcols = pers.columns[0:3].tolist()
        func_dict = {'inntekt':['mean', 'sum'], 'formue': ['sum', 'std', 'count']}
        fillna_dict = {'kjonn': 'Total kjønn', 'alder': 'Total alder', 'kommune': 'Total kommune'}
        agg5 = pandas_combinations_lono.all_combos_agg(pers, groupcols=groupcols, 
                                                       func=func_dict,
                                                       fillna_dict=fillna_dict, 
                                                       grand_total=True,
                                                       grand_total_text='All'
                                                      )
        display(agg5)    """
    
    
    #print(f"{groupcols=}")
    #print(f"{fillna_dict=}")
    #print(f"{keep_empty=}")
    #print(f"{grand_total=}")
    #print(f"{grand_total_text=}")
    #print(f"{aggargs=}")
    
    dataframe = df.copy()
    
    # Hack using categoricals to keep all unobserved groups
    if keep_empty:
        dataframe = dataframe.astype({col: "category" for col in groupcols})
    
    # Generate all possible combinations of group columns
    combos = []
    for r in range(len(groupcols) + 1, 0, -1):
        combos += list(combinations(groupcols, r))
    #print(combos)
    # Create an empty DataFrame to store the results
    all_levels = pd.DataFrame()

    # Calculate aggregates for each combination
    for i, comb in enumerate(combos):
        # Calculate statistics using groupby
        #print(list(comb))
        if keep_empty:
            # Hack using categoricals to keep all unobserved groups
            result = dataframe.groupby(list(comb), observed=False)
        else:
            result = dataframe.groupby(list(comb))
        
        
        #print(vars(result))
        #print(type(result))
        
        result = result.agg(aggargs).reset_index(names=list(comb))

        # Add a column to differentiate the combinations
        result['level'] = len(combos) - i

        # Add a column with number of group columns used in the aggregation
        result['ways'] = int(len(comb))

        # Concatenate the current result with the combined results
        all_levels = pd.concat([all_levels, result], ignore_index=True)

    # Calculate the grand total
    if grand_total:
        # Add category to categoricals
        cat_groupcols = df[groupcols].select_dtypes("category").columns
        for col in cat_groupcols:
            all_levels[col] = all_levels[col].add_categories(grand_total_text)
        
        
        gt = pd.DataFrame(dataframe.agg(aggargs)).T
        gt['level'] = 0
        gt['ways'] = 0
        gt[groupcols] = grand_total_text
        gt = gt[all_levels.columns]

        # Append the grand total row to the combined results and sort by levels and groupcols
        all_levels = pd.concat([all_levels, gt], ignore_index=True)
    
    all_levels = all_levels.sort_values(['level'] + groupcols)

    # Fill missing group columns with value
    if fillna_dict:
        #print("filling na")
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