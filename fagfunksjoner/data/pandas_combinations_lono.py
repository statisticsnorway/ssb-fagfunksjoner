import pandas as pd
from itertools import combinations

def aggregate_all(df: pd.DataFrame,
                  groupcols: list,
                  fillna_dict: dict = None,
                  keep_empty: bool = False,
                  totals: bool = False,
                  *aggargs, **aggkwargs):
    """ Generate all aggregation levels for a set of columns in a dataframe
    
        Parameters:
            dataframe: Name of dataframe to aggregate.
            groupcols: List of columns to group by.
            aggcols: List of columns to aggregate.
            aggfunc: List of aggregation function(s), like sum, mean, min, count.
            fillna: Value to fill the NaN values for the group-by columns.
            
        Returns:
            dataframe with all the group-by columns, all the aggregation columns combined 
            with the aggregation functions, a column called aggregation_level which 
            separates the different aggregation levels, and a column called aggregation_ways which
            counts the number of group columns used for the aggregation.
            
        Advices:    
            When you want the frequency, create a column with the value 1 for each row first and then use that as the aggcol.
            Make sure that you don't have any values in the group columns that are the same as your fillna value.
        
        Known problems: 
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

        agg1 = aggregate_all(pers, groupcols=['kjonn'], aggcols=['inntekt'])
        display(agg1)

        agg2 = aggregate_all(pers, groupcols=['kommune', 'kjonn', 'alder'], aggcols=['inntekt', 'formue'])
        display(agg2)

        agg3 = aggregate_all(pers, groupcols=['kommune', 'kjonn', 'alder'], aggcols=['inntekt'], fillna='T', aggfunc=['mean', 'std'])
        display(agg3)

        pers['antall'] = 1
        groupcols = pers.columns[0:2].tolist()
        aggcols = pers.columns[3:5].tolist()
        aggcols.extend(['antall'])
        agg4 = aggregate_all(pers, groupcols=groupcols, aggcols=aggcols, fillna='T')
        display(agg4)
    """
    dataframe = df.copy()
    
    # Hack using categoricals to keep all unobserved groups
    if keep_empty:
        dataframe[groupcols] = dataframe[groupcols].astype("category")
    
    # Generate all possible combinations of group columns
    combos = []
    for r in range(1, len(groupcols) + 1):
        combos.extend(list(combinations(groupcols, r)))

    # Reverse the combinations to aggregate the most detailed first. This will give us the right column order of the output dataframe    
    combos.reverse()

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

        result = result.agg(*aggargs, **aggkwargs).reset_index()

        # Add a column to differentiate the combinations
        result['level'] = len(combos) - i

        # Add a column with number of group columns used in the aggregation
        result['ways'] = int(len(comb))

        # Concatenate the current result with the combined results
        all_levels = pd.concat([all_levels, result], ignore_index=True)

    # Calculate the grand total
    gt = pd.DataFrame(dataframe.agg(*aggargs, **aggkwargs)).reset_index()
    gt = (pd.DataFrame(gt
                       .agg(*aggargs, **aggkwargs)
                       .T)
          .T)
    gt['level'] = 0
    gt['ways'] = 0

    # Append the grand total row to the combined results and sort by levels and groupcols
    all_levels = pd.concat([all_levels, gt], ignore_index=True).sort_values(list(itertools.chain(['level'], groupcols)))

    # Fill missing group columns with value
    if fillna_dict:
        all_levels = fill_na_dict(all_levels, fillna_dict)
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
    for col, fill_val in vmapping.items():
        df[col] = df[col].fillna(fill_val)
    return df