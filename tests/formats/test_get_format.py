import json
import os
import shutil
import unittest
from pathlib import Path

from fagfunksjoner import SsbFormat
from fagfunksjoner.formats import get_format


class TestGetFormat(unittest.TestCase):
    def setUp(self) -> None:
        # Create a temporary folder and add test JSON files for testing
        template_dir = Path(os.getcwd())
        self.path = template_dir / "test_formats"
        os.makedirs(self.path, exist_ok=True)

        # Create test JSON files
        self.test_files = ["file_2023-05-10.json", "anotherfile_2024-01-09.json"]
        frmt1 = {
            "file": dict(
                zip(
                    [f"key{i}" for i in range(1, 6)],
                    [f"value{j}" for j in range(1, 6)],
                    strict=False,
                )
            )
        }
        frmt2 = {
            "anotherfile": dict(
                zip(
                    [f"{i}" for i in range(1, 6)],
                    [f"category{j}" for j in range(1, 6)],
                    strict=False,
                )
            )
        }
        self.dictionaries = [frmt1, frmt2]

        for k, file_name in enumerate(self.test_files):
            with open(os.path.join(self.path, file_name), "w") as json_file:
                json.dump(self.dictionaries[k], json_file)

    # @mock.patch("ssb_utdanning.format.formats.get_path", side_effect=mock_get_path)
    def test_get_format(self) -> None:
        frmt = get_format(self.path / self.test_files[0])
        assert isinstance(frmt, SsbFormat)
        assert frmt == self.dictionaries[0]

    def tearDown(self) -> None:
        # Clean up test files and folders after tests
        shutil.rmtree(self.path, ignore_errors=True)
