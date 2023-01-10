import requests
import json
import pandas as pd


def klass_get(URL: str, level: str, return_df=False):
    """
    Parameter1: URL, the uri to a KLASS-API endpoint. Like 
    https://data.ssb.no/api/klass/v1/classifications/533/codes.json?from=2020-01-01&includeFuture=True
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
    Parameter1: URL, the uri to a KLASS-API endpoint. Like 
    https://data.ssb.no/api/klass/v1/classifications/533/codes.json?from=2020-01-01&includeFuture=True
    Parameter2: Level, defined by the API, like "codes"
    Returns: a pandas dataframe with the classification
    """
    return klass_get(URL, level, return_df=True)


def correspondance_dict(corr_id: str) -> dict:
    """Get a correspondance from its ID and
    return a dict of the correspondanceMaps["sourceCode"] as keys
    to the correspondanceMaps["targetCode"] as values.
    Apply this to a column in pandas with the .map method for example."""
    if isinstance(corr_id, float):
        corr_id = int(corr_id)
    if isinstance(corr_id, int):
        corr_id = str(corr_id)
    url = 'https://data.ssb.no/api/klass/v1/correspondencetables/' + corr_id
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers).text
    corr = json.loads(response)["correspondenceMaps"]
    return {s: t for s, t in zip([x["sourceCode"] for x in corr],
                                 [x["targetCode"] for x in corr])}