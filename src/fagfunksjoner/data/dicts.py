"""Extra functionality operating on baseline dicts added to this module"""


def get_key_by_value(data: dict, value: str) -> str | list:
    """Searches through the values in a dict for a match, returns the key,
    if a single match, a list of keys, if more than one match.

    Parameters
    ----------
    data: dict
        The data to search through, will only look in top most level...
    value: str
        The value to look for in the dict

    Returns
    -------
    str | list
        Returns a str if a single match, otherwise returns a list of keys with the same value
        
    Raises
    ------
    ValueError
        If no matches are found on values
    """
    result = []
    for key, heystack_value in data.items():
        if heystack_value == value:
            result.append(key)
    if len(result) == 1:
        return result[0]
    elif result:
        return result
    raise ValueError("Can’t find search-value")
