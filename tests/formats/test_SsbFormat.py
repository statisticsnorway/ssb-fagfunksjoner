import os
import shutil
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from fagfunksjoner import SsbFormat


class TestSsbFormat(unittest.TestCase):
    def setUp(self) -> None:
        # setting up folder to store formats
        template_dir = Path(os.getcwd())
        self.path = template_dir / "test_formats"
        # ensuring test folder does not exist
        self.tearDown()
        os.makedirs(self.path, exist_ok=True)

        # Initialize common objects or variables needed for tests
        self.test_dict = {"key1": "value1", "key2": "value2"}
        self.range_dict = {
            "low-10": "barn",
            "11-20": "ungdommer",
            "21-30": "unge_voksne",
            "31-high": "voksne",
        }
        self.str_nan = [".", "none", "None", "", "NA", "<NA>", "<NaN>", "nan", "NaN"]

    def test_initialization(self) -> None:
        ssb_format = SsbFormat(self.test_dict)
        self.assertIsInstance(ssb_format, SsbFormat)
        self.assertIsInstance(ssb_format, dict)

    def test_setitem_method(self) -> None:
        ssb_format = SsbFormat()
        ssb_format["test_key"] = "test_value"
        self.assertEqual(ssb_format["test_key"], "test_value")

    def test_missing_method(self) -> None:
        ssb_format = SsbFormat()
        with self.assertRaises(ValueError):
            ssb_format["nonexistent_key"]
        ssb_format["other"] = "other_value"
        assert ssb_format["nonexistent_key"] == "other_value"

    def test_store_ranges_method(self) -> None:
        ssb_format = SsbFormat()
        ssb_format["0 - 10"] = "range_1"
        ssb_format["15 - 20"] = "range_2"
        # Assert for correct storage of ranges
        self.assertIsNone(ssb_format.look_in_ranges("-1"))
        for i in range(0, 11):
            assert ssb_format[str(i)] == "range_1", f"{i}"
        for j in range(15, 21):
            assert ssb_format[str(j)] == "range_2", f"{j}"
        self.assertIsNone(ssb_format.look_in_ranges("21"))

    def test_look_in_ranges_method(self) -> None:
        ssb_format = SsbFormat(self.range_dict)
        ssb_format["other"] = "rest"
        ssb_format["."] = "NaN"
        assert ssb_format.look_in_ranges("0") == "barn"
        assert ssb_format.look_in_ranges("5") == "barn"
        assert ssb_format.look_in_ranges("18") == "ungdommer"
        assert ssb_format.look_in_ranges("110") == "voksne"

    def test_int_str_confuse(self) -> None:
        ssb_format = SsbFormat()
        ssb_format["1"] = "value1"
        ssb_format[2] = "value2"
        assert ssb_format[1] == "value1"
        assert ssb_format["2"] == "value2"

    def test_check_if_na(self) -> None:
        ssb_format = SsbFormat()
        for nan in self.str_nan:
            assert ssb_format.check_if_na(nan)
        assert ssb_format.check_if_na(np.nan)
        assert ssb_format.check_if_na(pd.NA)
        assert ssb_format.check_if_na(None)

    def test_NaNs(self) -> None:
        ssb_format = SsbFormat(self.range_dict)
        ssb_format[np.nan] = "NaN"
        assert ssb_format[np.nan] == "NaN"

        ssb_format["OtHer"] = "rest"
        assert ssb_format["other"] == "rest"

        # the dictionary should still recognize other NaN-values than the one specifically saved above, even with an "other" category
        for nan in self.str_nan:
            assert ssb_format[nan] == "NaN"

        # also checking non-string nan values
        assert ssb_format[np.nan] == "NaN"
        assert ssb_format[pd.NA] == "NaN"
        assert ssb_format[None] == "NaN"

        assert ssb_format["nonexistent_key"] == "rest"

    def test_store(self) -> None:
        ssb_format = SsbFormat(self.range_dict)
        assert len(os.listdir(self.path)) == 0
        format_name = "/test_format"
        ssb_format.store(output_path=str(self.path) + format_name, force=True)
        assert len(os.listdir(self.path)) == 1
        assert Path(str(self.path) + format_name + ".json").exists()
        ssb_format.store(output_path=str(self.path) + format_name + "2", force=True)
        assert Path(str(self.path) + format_name + "2.json").exists()

    def tearDown(self) -> None:
        # Clean up test files and folders after tests
        shutil.rmtree(self.path, ignore_errors=True)
