"""Extra functionality operating on baseline dicts added to this module."""

from collections.abc import Hashable
from typing import Any, TypeVar


# Define a type variable that will match the type of the keys in the dictionary
KeyType = TypeVar("KeyType", bound=Hashable)


def get_key_by_value(data: dict[KeyType, Any], value: Any) -> KeyType | list[KeyType]:
    """Searches through the values in a dict for a match, returns the key.

    Args:
        data (dict[KeyType, Any]): The data to search through, will only look in top most level...
        value (Any): The value to look for in the dict

    Returns:
        KeyType | list[KeyType]: Returns a single key if a single match on value,
            otherwise returns a list of keys with the same value.

    Raises:
        ValueError: If no matches are found on values
    """
    result = []
    for key, heystack_value in data.items():
        if heystack_value == value:
            result.append(key)
    if len(result) == 1:
        return result[0]
    elif result:
        return result
    raise ValueError("Can't find search-value")
