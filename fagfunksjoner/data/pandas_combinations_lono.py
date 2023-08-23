import pandas as pd
from itertools import combinations

def all_combos_agg(df: pd.DataFrame,
                  groupcols: list,
                  fillna_dict: dict = None,
                  keep_empty: bool = False,
                  grand_total: bool = False,
                  grand_total_text: str = 'Total',
                  *aggargs, **aggkwargs) -> pd.DataFrame:
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
    dataframe = df.copy()
    
    # Hack using categoricals to keep all unobserved groups
    if keep_empty:
        dataframe[groupcols] = dataframe[groupcols].astype("category")
    
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
            result = dataframe.groupby(list(comb), as_index=False, observed=False)
        else:
            result = dataframe.groupby(list(comb), as_index=False)

        result = result.agg(*aggargs, **aggkwargs)

        # Add a column to differentiate the combinations
        result['level'] = len(combos) - i

        # Add a column with number of group columns used in the aggregation
        result['ways'] = int(len(comb))

        # Concatenate the current result with the combined results
        all_levels = pd.concat([all_levels, result], ignore_index=True)

    # Calculate the grand total
    if grand_total:
        gt = pd.DataFrame(dataframe.agg(*aggargs, **aggkwargs)).reset_index()
        gt['x','z'] = 1
        #aggcols=gt.columns.get_level_values(0).unique()
        gt = gt.pivot(index=[('x','z')], columns='index').reset_index().drop(columns=[('x','z')])
#        gt = pd.DataFrame(dataframe.agg(*aggargs, **aggkwargs)).reset_index()
#        gt = (pd.DataFrame(gt
#                           .agg(*aggargs, **aggkwargs)
#                           .T)
#              .T)
        gt['level'] = 0
        gt['ways'] = 0
        gt[groupcols] = grand_total_text
        
        # Append the grand total row to the combined results and sort by levels and groupcols
        all_levels = pd.concat([all_levels, gt], ignore_index=True).sort_values(['level'] + groupcols)
    else:
        all_levels = all_levels.sort_values(['level'] + groupcols)
        
    # Fill missing group columns with value
    if fillna_dict:
        all_levels = fill_na_dict(all_levels, fillna_dict).dropna(how='all', axis=1)
    #all_levels[groupcols] = all_levels[groupcols].fillna(fillna)

    # change columns with multi-index to normal index
    # all_levels.columns = np.where(all_levels.columns.get_level_values(1) == '',
    #                               all_levels.columns.get_level_values(0),
    #                               all_levels.columns.get_level_values(0) + '_' + all_levels.columns.get_level_values(1)
    #                              )
    
    # Sett datatype tilbake til det den hadde i utgangpunktet
    if keep_empty:
        for col in groupcols:
            all_levels[col] = all_levels[col].astype(dataframe[col].dtype)
    
    return all_levels


def fill_na_dict(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    for col, fill_val in mapping.items():
        df[col] = df[col].fillna(fill_val)
    return df
