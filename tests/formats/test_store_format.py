import numpy as np
from fagfunksjoner import SsbFormat
from fagfunksjoner.formats import store_format
import json
import os
import shutil
from pathlib import Path
import unittest


class TestStoreFormat(unittest.TestCase):
    def setUp(self) -> None:
        # Create a temporary folder and add test JSON files for testing
        template_dir = Path(os.getcwd())
        self.path = template_dir / "test_formats"
        # make sure the tree is clean
        self.tearDown()
        os.makedirs(self.path, exist_ok=True)
        self.testfiles = ["file_2023-05-10.json", "anotherfile_2024-01-09.json"]
        self.frmt1 = dict(
                zip([f"key{i}" for i in range(1, 6)], [f"value{j}" for j in range(1, 6)])
            )
        
        self.frmt2 = dict(
                zip([f"{i}" for i in range(1, 6)], [f"category{j}" for j in range(1, 6)])
            )

    def test_store_format(self) -> None:
        assert not Path(str(self.path)+'/'+self.testfiles[0]).exists()
        store_format(self.frmt1, str(self.path)+'/'+self.testfiles[0])
        assert Path(str(self.path)+'/'+self.testfiles[0]).exists()
        
        assert not Path(str(self.path)+'/'+self.testfiles[1]).exists()
        store_format(self.frmt1, str(self.path)+'/'+self.testfiles[1].rsplit('.',1)[0])
        assert Path(str(self.path)+'/'+self.testfiles[1]).exists()

    def tearDown(self) -> None:
        # Clean up test files and folders after tests
        shutil.rmtree(self.path, ignore_errors=True)