import json
from pathlib import Path
from typing import Any

import pandas as pd
from pandas._libs.missing import NAType


SSBFORMAT_INPUT_TYPE = dict[str | int, Any] | dict[str, Any]


class SsbFormat(dict[Any, Any]):
    """Custom dictionary class designed to handle specific formatting conventions, including mapping intervals (defined as range strings) even when they map to the same value."""

    def __init__(self, start_dict: SSBFORMAT_INPUT_TYPE | None = None) -> None:
        """Initializes the SsbFormat instance.

        Args:
            start_dict (dict, optional): Initial dictionary to populate SsbFormat.
        """
        super().__init__()
        self.cached = True  # Switching the default to False might f-up __setitem__
        if start_dict:
            for k, v in start_dict.items():
                dict.__setitem__(self, k, v)

        self.update_format()

    def update_format(self) -> None:
        """Update method to set special instance attributes."""
        self.set_na_value()
        self.store_ranges()
        self.set_other_as_lowercase()

    def __setitem__(self, key: str | int | float | NAType | None, value: Any) -> None:
        """Overrides the '__setitem__' method of dictionary to perform custom actions on setting items.

        Args:
            key: Key of the item to be set.
            value: Value to be set for the corresponding key.
        """
        if self.cached:
            dict.__setitem__(self, key, value)
            if isinstance(key, str):
                if "-" in key and key.count("-") == 1:
                    self.store_ranges()  # update ranges after adding a new range key
                if key.lower() == "other" and key != "other":
                    self.set_other_as_lowercase()
            if self.check_if_na(key):
                self.set_na_value()

    def __missing__(self, key: str | int | float | NAType | None) -> Any:
        """Overrides the '__missing__' method of dictionary to handle missing keys.

        Checks for integer/string confusion, NA values, or membership in a defined range.
        If none apply and an 'other' key exists, its value is returned.

        Args:
            key (str | int | float | NAType | None): Key that is missing in the dictionary.

        Returns:
            Any: The corresponding mapped value based on special conditions.

        Raises:
            ValueError: If the key is not found and no 'other' key is defined.
        """
        int_str_confuse = self.int_str_confuse(key)
        if int_str_confuse is not None:
            if self.cached:
                self[key] = int_str_confuse
            return int_str_confuse

        if self.check_if_na(key) and self.set_na_value():
            if self.cached:
                self[key] = self.na_value
            return self.na_value

        key_in_range = self.look_in_ranges(key)
        if key_in_range is not None:
            if self.cached:
                self[key] = key_in_range
            return key_in_range

        other = self.get("other", "")
        if other:
            if self.cached:
                self[key] = other
            return other

        raise ValueError(f"{key} not in format, and no other-key is specified.")

    def store_ranges(self) -> None:
        """Stores ranges by converting range-string keys into tuple keys.

        For example, a key "0-18" with value "A" will be stored as
        {(0.0, 18.0): "A"}.
        """
        self.ranges: dict[tuple[float, float], Any] = {}
        for key, value in self.items():
            if isinstance(key, str) and "-" in key and key.count("-") == 1:
                self._range_to_floats(key, value)

    def _range_to_floats(self, key: str, value: Any) -> None:
        """Converts a range-string key to a tuple of floats and stores it.

        Args:
            key: Key to be converted to a tuple of floats.
            value (Any): Value to be associated with the converted range.
        """
        parts = key.split("-")
        if len(parts) != 2:
            return
        bottom_str, top_str = parts[0].strip(), parts[1].strip()
        if (bottom_str.isdigit() or bottom_str.lower() == "low") and (
            top_str.isdigit() or top_str.lower() == "high"
        ):
            bottom_float = (
                float("-inf") if bottom_str.lower() == "low" else float(bottom_str)
            )
            top_float = float("inf") if top_str.lower() == "high" else float(top_str)
            self.ranges[(bottom_float, top_float)] = value

    def look_in_ranges(self, key: str | int | float | NAType | None) -> None | Any:
        """Returns the mapping value for the key if it falls within any defined range.

        The method attempts to convert the key to a float and then checks if it lies within
        any of the stored range intervals. If the key is None, NA, or not of a convertible type,
        the method returns None.
        """
        if key is None or pd.isna(key) or not isinstance(key, str | int | float):
            return None

        try:
            key_value = float(key)
        except (ValueError, TypeError):
            return None

        for (bottom, top), mapping_value in self.ranges.items():
            if bottom <= key_value <= top:
                return mapping_value
        return None

    def int_str_confuse(self, key: str | int | float | NAType | None) -> None | Any:
        """Handles conversion between integer and string keys.

        Args:
            key: Key to be converted or checked for existence in the dictionary.

        Returns:
            The value associated with the key (if found) or None.
        """
        if isinstance(key, str):
            try:
                int_key = int(key)
                if int_key in self:
                    return self[int_key]
            except ValueError:
                return None
        elif isinstance(key, int):
            str_key = str(key)
            if str_key in self:
                return self[str_key]
        return None

    def set_other_as_lowercase(self) -> None:
        """Ensures that the 'other' key is stored in lowercase.

        If a key matching 'other' in any other case is found, its value is reassigned to 'other'.
        """
        keys_to_update = [
            k
            for k in self
            if isinstance(k, str) and k.lower() == "other" and k != "other"
        ]
        for k in keys_to_update:
            value = self[k]
            del self[k]
            self["other"] = value

    def set_na_value(self) -> bool:
        """Sets the value for NA (Not Available) keys in the SsbFormat.

        Returns:
            bool: True if NA value is successfully set, False otherwise.
        """
        for key, value in self.items():
            if self.check_if_na(key):
                self.na_value = value
                return True
        self.na_value = None
        return False

    @staticmethod
    def check_if_na(key: Any) -> bool:
        """Checks if the specified key represents a NA (Not Available) value.

        Args:
            key: Key to be checked for NA value.

        Returns:
            bool: True if the key represents NA, False otherwise.
        """
        if pd.isna(key):
            return True
        if isinstance(key, str):
            if key in [".", "none", "None", "", "NA", "<NA>", "<NaN>", "nan", "NaN"]:
                return True
        return False

    def store(
        self,
        output_path: str | Path,
        force: bool = False,
    ) -> None:
        """Stores the SsbFormat instance in a specified output path.

        Args:
            output_path (str): Path where the format will be stored.
            force (bool): Flag to force storing even for cached instances.

        Raises:
            ValueError: If storing a cached SsbFormat might lead to an unexpectedly large number of keys.
        """
        if not isinstance(output_path, Path):
            output_path = Path(output_path)
        if self.cached and not force:
            error_msg = """Storing a cached SsbFormat might lead to many more keys than you want.
            Please check the amount of keys before storing.
            You can reopen the dict, set it as False on .cached, then store again, or send force=True to the store method."""
            raise ValueError(error_msg)
        # store_format({format_name: self}, output_path)
        store_format(self, output_path)


def get_format(filepath: str | Path) -> SsbFormat | None:
    """Retrieves the format from a json-format-file from path.

    Args:
        filepath (str|Path): Send in the full path to the format directly.

    Returns:
        dict or defaultdict: The formatted dictionary or defaultdict for the specified format and date. If the format contains a "other" key, a defaultdict will be returned. If the
            format contains the SAS-value for missing: ".", or another recognized "empty-datatype":
            Many known keys for empty values, will be inserted in the dict, to hopefully map these correctly.
    """
    with open(filepath) as format_json:
        ord_dict = json.load(format_json)
    return SsbFormat(ord_dict)


def store_format(
    anyformat: SsbFormat | dict[Any, Any],
    output_path: str | Path,
) -> None:
    """Takes a nested or unnested dictionary and saves it to prodsone-folder as a timestamped json.

    Args:
        anyformat (dict[str, str]): Dictionary containing format information.
            The values of the dictionary are the dict contents of the formats.¨
        output_path (str): Path to store the format data. Not including the filename itself, only the base folder.
    """
    if not isinstance(output_path, Path):
        output_path = Path(output_path)
    if not isinstance(anyformat, SsbFormat):
        anyformat = SsbFormat(anyformat)
    if str(output_path).endswith(".json"):
        output_path = str(output_path).rsplit(".", 1)[0]
    with open(str(output_path) + ".json", "w") as json_file:
        json.dump(anyformat, json_file)
