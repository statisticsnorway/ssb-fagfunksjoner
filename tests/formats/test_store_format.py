import numpy as np
from fagfunksjoner import SsbFormat
from fagfunksjoner.formats import store_format
import json
import os
import shutil
from pathlib import Path


class TestStoreFormat(unittest.TestCase):
    def setUp(self) -> None:
        # Create a temporary folder and add test JSON files for testing
        template_dir = Path(os.getcwd())
        self.path = template_dir / "test_formats"
        # make sure the tree is clean
        self.tearDown()
        os.makedirs(self.path, exist_ok=True)
        # Create test JSON files
        self.test_files, self.dates, self.dictionaries = write_test_formats(
            self.path, store=False
        )
    def test_store_format_prod(self, mock_get: mock.MagicMock) -> None:
        # checking for file in folder, write file, then check again
        for i, file in enumerate(self.test_files):
            shortnames = self.shortname_files_in_path()
            assert self.test_files[i].split("_")[0] not in shortnames
            store_format_prod(
                {
                    list(self.dictionaries[i].keys())[0]: UtdFormat(
                        self.dictionaries[i][list(self.dictionaries[i].keys())[0]]
                    )
                },
                output_path=self.path,
            )
            shortnames = self.shortname_files_in_path()
            assert self.test_files[i].split("_")[0] in shortnames
        # try and write file again, sohuld not be possible because content is the same
        shortnames = self.shortname_files_in_path()
        n_file_files = len(shortnames)
        assert n_file_files == 2
        store_format_prod(
            {
                list(self.dictionaries[0].keys())[0]: UtdFormat(
                    self.dictionaries[0][list(self.dictionaries[0].keys())[0]]
                )
            },
            output_path=self.path,
        )
        shortnames = self.shortname_files_in_path()
        n_file_files = len(shortnames)
        assert n_file_files == 2

        # change dictionary slightly and write again. check that new file is written to folder
        frmt = self.dictionaries[0]
        self.dictionaries[0]["file"]["testkey"] = "testvalue"
        # timedelay to not overwrite file with same timestamp as previous file
        time.sleep(3)
        store_format_prod(
            {
                list(self.dictionaries[0].keys())[0]: UtdFormat(
                    self.dictionaries[0][list(self.dictionaries[0].keys())[0]]
                )
            },
            output_path=self.path,
        )
        shortnames = self.shortname_files_in_path()
        n_file_files = len(shortnames)
        assert n_file_files == 3

    def tearDown(self) -> None:
        # Clean up test files and folders after tests
        shutil.rmtree(self.path, ignore_errors=True)
