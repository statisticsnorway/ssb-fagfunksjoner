import json
from pathlib import Path
from typing import Any

import pandas as pd
from pandas._libs.missing import NAType


SSBFORMAT_INPUT_TYPE = dict[str | int, Any] | dict[str, Any]


class SsbFormat(dict[Any, Any]):
    """Custom dictionary class designed to handle specific formatting conventions."""

    def __init__(self, start_dict: SSBFORMAT_INPUT_TYPE | None = None) -> None:
        """Initializes the SsbFormat instance.

        Args:
            start_dict (dict, optional): Initial dictionary to populate SsbFormat.
        """
        super(dict, self).__init__()
        self.cached = True  # Switching the default to False, will f-up __setitem__
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
                    self.store_ranges()
                if key.lower() == "other" and key != "other":
                    self.set_other_as_lowercase()
            if self.check_if_na(key):
                self.set_na_value()

    def __missing__(self, key: str | int | float | NAType | None) -> Any:
        """Overrides the '__missing__' method of dictionary to handle missing keys.

        Args:
            key (str | int | float | NAType | None): Key that is missing in the dictionary.

        Returns:
            Any: Value of key in any special conditions: confusion int/str, in one of the ranges, NA or if other is defined.

        Raises:
            ValueError: If the key is not found in the format and no 'other' key is specified.
        """
        int_str_confuse = self.int_str_confuse(key)
        if int_str_confuse:
            if self.cached:
                self[key] = int_str_confuse
            return int_str_confuse

        if self.check_if_na(key) and self.set_na_value():
            if self.cached:
                self[key] = self.na_value
            return self.na_value

        key_in_range = self.look_in_ranges(key)
        if key_in_range:
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
        """Stores ranges based on specified keys in the dictionary."""
        self.ranges: dict[str, tuple[float, float]] = {}
        for key, value in self.items():
            if isinstance(key, str) and "-" in key and key.count("-") == 1:
                self._range_to_floats(key, value)

    def _range_to_floats(self, key: str, value: str) -> None:
        """Converts a range key to a tuple of floats.

        Args:
            key: Key to be converted to a tuple of floats.
            value (str): Value to be associated with the converted range.
        """
        bottom, top = key.split("-")[0].strip(), key.split("-")[1].strip()
        if (bottom.isdigit() or bottom.lower() == "low") and (
            top.isdigit() or top.lower() == "high"
        ):
            if bottom.lower() == "low":
                bottom_float = float("-inf")
            else:
                bottom_float = float(bottom)
            if top.lower() == "high":
                top_float = float("inf")
            else:
                top_float = float(top)
            self.ranges[value] = (bottom_float, top_float)

    def look_in_ranges(self, key: str | int | float | NAType | None) -> None | str:
        """Looks for the specified key within the stored ranges.

        Args:
            key: Key to search within the stored ranges.

        Returns:
            The value associated with the range containing the key, if found; otherwise, None.
        """
        if isinstance(key, str | int | float):
            try:
                key = float(key)
            except ValueError:
                return None
            for range_key, (bottom, top) in self.ranges.items():
                if key >= bottom and key <= top:
                    return range_key
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
                key = int(key)
                if key in self:
                    return self[key]
            except ValueError:
                return None
        elif isinstance(key, int):
            key = str(key)
            if key in self:
                return self[key]
        return None

    def set_other_as_lowercase(self) -> None:
        """Sets the key 'other' to lowercase if mixed cases are found."""
        found = False
        for key in self:
            if isinstance(key, str) and key.lower() == "other":
                found = True
                break
        if found:
            value = self[key]
            del self[key]
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
    def check_if_na(key: str | Any) -> bool:
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


def get_format(filepath: str | Path | None = "") -> SsbFormat | None:
    """Retrieves the format from a json-format-file from path.

    Args:
        filepath (str): Send in the full path to the format directly, this will ignore the name and date args.

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
