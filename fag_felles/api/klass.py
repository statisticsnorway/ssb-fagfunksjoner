import requests
import json
import pandas as pd
from tqdm.notebook import tqdm_notebook as pbar

def klass_get(URL: str, level: str, return_df=False):
    """
    Parameter1: URL, the uri to a KLASS-API endpoint. Like https://data.ssb.no/api/klass/v1/classifications/533/codes.json?from=2020-01-01&includeFuture=True
    Parameter2: Level, defined by the API, like "codes"
    Parameter3: return_df, returns json if set to False, a pandas dataframe if True
    Returns: a pandas dataframe with the classification
    """
    
    if URL[:8] != "https://":
        raise requests.HTTPError("Please use https, not http.")
    
    r = requests.get(URL)
    
    # HTTP-errorcode handling
    if r.status_code != 200:
        raise requests.HTTPError(f"Connection error: {r.status_code}. Try using https on Dapla?")
    
    # Continue munging result
    r = json.loads(r.text)[level]
    if return_df:
        return pd.json_normalize(r)
    return r


def klass_df(URL: str, level: str):
    """
    By using this function to imply that you want a dataframe back.
    Parameter1: URL, the uri to a KLASS-API endpoint. Like https://data.ssb.no/api/klass/v1/classifications/533/codes.json?from=2020-01-01&includeFuture=True
    Parameter2: Level, defined by the API, like "codes"
    Returns: a pandas dataframe with the classification
    """
    return klass_get(URL, level, return_df=True)



def korrespondanse_summer(data: pd.DataFrame,
                 korresp: pd.DataFrame,
                 id_col: str='Region',
                 val_cols: list=['value'],
                 ignore_cols: list=['region']):
    """
    Parameter1: data, the dataframe, usuallt from STATBANKen?
    Parameter2: korresp, correspondance-tables from KLASS? Needs to have a column "sourceCode" and "targetCode".
    Parameter3: id_col, the column that the correspondance refers to in "sourceCode" and "targetCode"
    Parameter4: val_cols, a list of the columns that contain the values, often "value" from statbanken
    Parameter5: ignore_cols, columns to ignore for the row-matching, usually different values represented by the id or value-columns.
    Returns: a pandas dataframe with the modified sums
    """
    # Avoid mutability with deep-copy
    df = data.copy()
    # Forventer at korrespondansen inn er dataframe med target og sourcecodes
    try:
        corr = dict(zip(korresp['targetCode'], korresp['sourceCode']))
    except KeyError as e:
        print('Forventer kolonnene "targetCode" og "sourceCode" fra Klass-korrespondanse tabeller.')
        raise e
    # Loop over korrespondansen
    for t, s in pbar(corr.items()):
        # Radene vi skal kopiere fra
        #print(t, s)
        t_rows = df[df[id_col] == t]
        # Radene vi skal kopiere til
        s_rows = df[df[id_col] == s]
        # Loop over radene vi skal kopiere fra
        for i, t_r in t_rows.iterrows():
            # Finn raden vi skal kopiere til
            needle = t_r.drop([id_col] + val_cols + ignore_cols)
            haystack = s_rows.drop([id_col] + val_cols + ignore_cols, axis=1)
            result = haystack[(haystack == needle).all(1)]
            # Om det bare er en rad å kopiere fra
            if len(result) == 1:
                # For hver verdikolonne, pluss på verdien fra target til source
                for col in val_cols:
                    df.loc[result.iloc[0].name, col] += df.loc[i, col]
                    #print(col, i, df.loc[i, col])
            else:
                raise ValueError("More than one row to paste into...")
    return df