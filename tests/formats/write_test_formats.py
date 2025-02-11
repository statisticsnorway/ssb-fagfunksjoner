import json
import os
from pathlib import Path


def write_test_formats(
    path: Path, store: bool = True
) -> tuple[list[str], list[str], list[dict[str, dict[str, str]]]]:
    test_files = ["file_2023-05-10.json", "anotherfile_2024-01-09.json"]
    dates = [test_files[0][-15:-5], test_files[1][-15:-5]]
    frmt1 = {
        "file": dict(
            zip([f"key{i}" for i in range(1, 6)], [f"value{j}" for j in range(1, 6)])
        )
    }
    frmt2 = {
        "anotherfile": dict(
            zip([f"{i}" for i in range(1, 6)], [f"category{j}" for j in range(1, 6)])
        )
    }
    dictionaries = [frmt1, frmt2]
    # when testing store_format function this next step is not needed
    if store == True:
        for k, file_name in enumerate(test_files):
            with open(os.path.join(path, file_name), "w") as json_file:
                json.dump(dictionaries[k], json_file)
    return test_files, dates, dictionaries