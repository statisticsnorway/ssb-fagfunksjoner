from abc import ABC, abstractproperty, abstractmethod
from pathlib import PurePosixPath, PurePath
import re
import warnings
from os import PathLike

import datetime
import duckdb
import dapla as dp
import geopandas as gpd
import numpy as np
import pandas as pd
import pandas.io.formats.format as fmt
from pandas.api.types import is_dict_like
import pyarrow
import pyarrow.parquet as pq
import sgis as sg


VERSION_PATTERN = r"_v(\d+)"
VERSION_PREFIX = "_v"
PRE_TEAMNAME = "ssb-prod-"
GS_URI_PREFIX = "gs://"


def _pathseries_constructor_with_fallback(data=None, index=None, **kwargs):
    series = pd.Series(data, index, **kwargs)
    # TODO introduce coupling with Path for predictability?
    if pd.api.types.is_numeric_dtype(series):
        return series
    return PathSeries(series)


class PathSeries(pd.Series):
    _version_pattern = VERSION_PATTERN
    _version_prefix = VERSION_PREFIX
    _metadata = ["_version_pattern", "_max_rows", "_max_colwidth", "_defined_name", "name"]

    def __init__(
        self,
        data: list[str],
        index=None,
        max_rows: int | None = 10,
        max_colwidth: int = 75,
        **kwargs,
    ):
        super().__init__(data, index=index, **kwargs)
        
        self._max_rows = max_rows
        self._max_colwidth = max_colwidth
        pd.set_option("display.max_colwidth", max_colwidth)
        
    @property
    def _constructor(self):
        return _pathseries_constructor_with_fallback

    @property
    def _constructor_expanddim(self):
        return pd.DataFrame

    @property
    def base(self):
        """The common path amongst all paths in the Series."""
        if len(self) <= 1:
            return Path("")

        splitted_path: list[str] = PurePosixPath(self.iloc[0]).as_posix().split("/")

        common_path = [
            folder for folder in splitted_path if self.str.contains(folder).all()
        ]

        return Path("/".join(common_path))

    def files(self):
        """Return only the files."""
        return self.loc[self.is_file()]

    def dirs(self):
        """Return only the directories."""
        return self.loc[self.is_dir()]

    def ls_dirs(self, recursive: bool = False) -> list:
        """List the contents of the subdirectories."""

        if recursive:
            return get_files_in_subfolders(self.dirs())

        return [path.ls() for path in self.dirs()]

    def is_file(self) -> pd.Series:
        return self.kb > 0

    def is_dir(self) -> pd.Series:
        return self.kb == 0

    def containing(self, pat: str, *args, **kwargs):
        """Convenience method for selecting paths containing string."""
        return self.loc[lambda x: x.str.contains(pat, *args, **kwargs)]

    def within_minutes(self, minutes: int):
        """Select files with an 'updated' timestamp within the given number of minutes."""
        time_then = pd.Timestamp.now() - pd.Timedelta(minutes=minutes)
        return self.loc[lambda x: x.updated > time_then]

    def within_hours(self, hours: int):
        """Select files with an 'updated' timestamp within the given number of hours."""
        time_then = pd.Timestamp.now() - pd.Timedelta(minutes=hours * 60)
        return self.loc[lambda x: x.updated > time_then]

    def within_days(self, hours: int):
        """Select files with an 'updated' timestamp within the given number of days."""
        time_then = pd.Timestamp.now() - pd.Timedelta(minutes=hours * 60 * 24)
        return self.loc[lambda x: x.updated > time_then]

    @property
    def updated(self):
        return self.index.get_level_values(0)

    @property
    def mb(self):
        return self.index.get_level_values(1)

    @property
    def gb(self):
        return self.index.get_level_values(1) / 1000

    @property
    def kb(self):
        return self.index.get_level_values(1) * 1000

    @property
    def stem(self):
        return self.apply(lambda x: x.stem)

    @property
    def names(self):
        return self.apply(lambda x: x.name)
    
    @property
    def version_number(self):
        return self.apply(lambda x: x.version_number)

    @property
    def versionless(self):
        return self.apply(lambda x: x.versionless)

    @property
    def versionless_stem(self):
        return self.apply(lambda x: x.versionless_stem)

    @property
    def parent(self):
        return self.apply(lambda x: x.parent)

    def keep_highest_numbered_versions(self):
        """Strips all version numbers (and '_v') off the file paths in the folder and keeps only the highest.

        Does a regex search for the pattern '_v' followed by any integer.

        """
        self = self.sort_values()
        return self._drop_version_number_and_keep_last()

    def keep_latest_versions(self, include_versionless: bool = True):
        """Strips all version numbers (and '_v') off the file paths in the folder and keeps only the newest.

        Does a regex search for the pattern '_v' followed by any integer.

        """
        self = self.sort_index(level=0)
        return self._drop_version_number_and_keep_last(include_versionless)

    def _drop_version_number_and_keep_last(
        self, include_versionless: bool | None = None
    ):
        stems = self.stem.reset_index(drop=True)

        if include_versionless is False:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                stems = stems.loc[stems.str.contains(self._version_pattern)]

        without_version_number = stems.str.replace(
            self._version_pattern, "", regex=True
        )

        only_newest = without_version_number.loc[lambda x: ~x.duplicated(keep="last")]

        return self.iloc[only_newest.index]
    
    def __str__(self):
        repr_params = fmt.get_series_repr_params()
        repr_params["max_rows"] = self._max_rows

        max_len = max(len(x) for x in self) if len(self) else 0

        if self.base and max_len > self._max_colwidth:
            s = pd.Series(self, name=self.base).str.replace(self.base, "...")
        else:
            s = pd.Series(self)

        if len(s):
            s.index = pd.MultiIndex.from_arrays(
                [s.index.get_level_values(0), s.index.get_level_values(1).astype(int)],
                names=["updated", "mb (int)"],
            )

        return s.to_string(**repr_params)
        
    def __repr__(self):
        return self.__str__()

    def _repr_html_(self):
        df = pd.DataFrame({"path": self})#self + '#' + self.stem})
        df.index = pd.MultiIndex.from_arrays(
            [self.index.get_level_values(0), self.index.get_level_values(1).astype(int)],
            names=["updated", "mb (int)"],
        )
        
        if len(df) <= self._max_rows:
            return df.style.format({'path': split_path_and_make_copyable_html}).to_html()
        
        # the Styler puts the elipsis row last. I want it in the middle. Doing it manually...
        first_rows = df.head(self._max_rows // 2).style.format({'path': split_path_and_make_copyable_html})
        last_rows = df.tail(self._max_rows // 2).style.format({'path': split_path_and_make_copyable_html})

        elipsis_row = df.iloc[[0]]
        elipsis_row.index = pd.MultiIndex.from_arrays([["..."], ["..."]], names=elipsis_row.index.names)
        elipsis_row.iloc[[0]] = ["..."] * len(elipsis_row.columns)
        elipsis_row = elipsis_row.style
        
        return first_rows.concat(elipsis_row).concat(last_rows).to_html()


def split_path_and_make_copyable_html(
    path: str, 
    split: str | None = "/", 
    display_prefix: str | None = ".../"
) -> str:
    """Get html text that displays the last part, but makes the full path copyable to clipboard."""

    copy_to_clipboard_js = f"""
    <script>
    function copyToClipboard(text) {{
        navigator.clipboard.writeText(text)
            .then(() => {{
                const alertBox = document.createElement('div');
                const selection = window.getSelection();

                alertBox.style.position = 'fixed';
                alertBox.style.top = (selection.getRangeAt(0).getBoundingClientRect().top + window.scrollY) + 'px';
                alertBox.style.left = (selection.getRangeAt(0).getBoundingClientRect().left + window.scrollX) + 'px';
                alertBox.style.backgroundColor = '#f2f2f2';
                alertBox.style.border = '1px solid #ccc';
                alertBox.style.padding = '10px';
                alertBox.innerHTML = 'Copied to clipboard';
                document.body.appendChild(alertBox);

                setTimeout(function() {{
                    alertBox.style.display = 'none';
                }}, 1500);  // 1.5 seconds
            }})
            .catch(err => {{
                console.error('Could not copy text: ', err);
            }});
    }}
    </script>
    """
    
    if split is not None:
        name = path.split(split)[-1]
        displayed_text = f"{display_prefix}{name}" if display_prefix else name
    else:
        displayed_text = path

    return (
        copy_to_clipboard_js + 
        f'<a href="{displayed_text}" onclick="copyToClipboard(\'{path}\')">{displayed_text}</a>'
    )


def as_str(obj):
    if isinstance(obj, str):
        return obj
    if hasattr(obj, "__fspath__"):
        return obj.__fspath__()
    if hasattr(obj, "_str"):
        try:
            return str(obj._str())
        except TypeError:
            return str(obj._str)
    raise TypeError(type(obj))


class Path(str):
    """Path that works like a string, with methods like exists and ls for Dapla."""

    _version_pattern = VERSION_PATTERN
    _version_prefix = VERSION_PREFIX

    def __new__(cls, gcs_path: str | PurePath):
        gcs_path = cls._fix_path(gcs_path)
        return str.__new__(cls, gcs_path)
    
    def __init__(self, gcs_path: str | PathLike):
        self._path = PurePosixPath(self)

    @staticmethod
    def _fix_path(path: str | PurePosixPath) -> str:
        return (str(path)
            .replace("\\", "/")
            .replace(r"\"", "/")
            .replace("//", "/")
            .strip("/")
        )

    def versions(self) -> PathSeries:
        """Returns all versions of the file."""
        files_in_folder = self.parent.files()
        return files_in_folder.loc[lambda x: x.str.contains(self.versionless_stem)]

    def new_version(self):
        """Return the path with the highest existing version number plus 1."""
        versions: PathSeries = self.versions()

        highest_numbered: Path = self.highest_numbered_version()

        return highest_numbered.add_to_version_number(1)

    def latest_version(self, include_versionless: bool = True):
        """Get the newest version of the file path.

        Lists files in the parent directory with the same versionless stem
        and selects the one with the latest timestamp (updated).
        """
        versions: PathSeries = self.versions()

        last = versions.keep_latest_versions()
        assert len(last) == 1
        return last.iloc[0]

    def highest_numbered_version(self):
        """Get the highest number version of the file path.

        Lists files in the parent directory with the same versionless stem
        and selects the one with the highest version number.
        """
        versions: PathSeries = self.versions()

        last = versions.keep_highest_numbered_versions()
        assert len(last) == 1
        return last.iloc[0]

    @property
    def version_number(self) -> int | None:
        try:
            last_match = re.findall(self._version_pattern, self)[-1]
            return int(last_match)
        except IndexError:
            return None

    @property
    def versionless_stem(self):
        return Path(re.sub(self._version_pattern, "", self._path.stem))

    @property
    def versionless(self):
        return Path(f"{self.parent}/{self.versionless_stem}{self.suffix}")

    @property
    def parent(self):
        return Path(self._path.parent)

    @property
    def root(self):
        return Path(self.parts[0])

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def stem(self) -> str:
        return self._path.stem

    @property
    def parts(self) -> tuple[str]:
        try:
            return self._path._pparts
        except AttributeError:
            return tuple(self._path._parts)

    @property
    def suffix(self) -> str:
        return str(self._path.suffix)

    @property
    def suffixes(self):
        return self._path.suffixes

    @property
    def dtypes(self) -> pd.Series:
        with self.open() as f:
            schema = pyarrow.parquet.read_schema(f)
            return pd.Series(schema.types, index=schema.names)

    @property
    def columns(self) -> pd.Index:
        with self.open() as f:
            return pd.Index(pyarrow.parquet.read_schema(f).names)

    @property
    def shape(self) -> tuple[int, int]:
        with self.open() as f:
            meta = pyarrow.parquet.read_metadata(f)
            return meta.num_rows, meta.num_columns
    
    def mv(self, new_path: str):
        dp.FileClient.get_gcs_file_system().mv(self, as_str(new_path))
        return Path(new_path)

    def cp(self, new_path: str):
        dp.FileClient.get_gcs_file_system().cp(self, as_str(new_path))
        return Path(new_path)

    def rm_file(self) -> None:
        dp.FileClient.get_gcs_file_system().rm_file(self)

    def is_dir(self) -> bool:
        return dp.FileClient.get_gcs_file_system().isdir(self)

    def is_file(self) -> bool:
        return not dp.FileClient.get_gcs_file_system().isdir(self)

    def exists(self) -> bool:
        return dp.FileClient.get_gcs_file_system().exists(self)

    def open(self):
        return dp.FileClient.get_gcs_file_system().open(self)

    def dirs(self, recursive: bool = False) -> PathSeries:
        """Lists all child directories."""
        if not recursive:
            info: list[dict] = dp.FileClient.get_gcs_file_system().ls(self, detail=True)
            return PathSeries(self._get_directory_series(info).apply(Path).sort_index(level=0))

        return self.ls(recursive=recursive).dirs()

    def files(self, recursive: bool = False) -> PathSeries:
        """Lists all files in the directory."""
        if not recursive:
            info: list[dict] = dp.FileClient.get_gcs_file_system().ls(self, detail=True)
            return PathSeries(self._get_file_series(info).apply(Path).sort_index(level=0))

        return self.ls(recursive=recursive).files()

    def ls(self, recursive: bool = False) -> PathSeries:
        """Lists the contents of a GCS bucket path.

        Returns a PathSeries with paths as values and timestamps
        and file size as index.
        """
        info: list[dict] = dp.FileClient.get_gcs_file_system().ls(self, detail=True)

        files: pd.Series = self._get_file_series(info).apply(Path)

        dirs: pd.Series = self._get_directory_series(info).apply(Path)

        out = PathSeries(pd.concat([x for x in [files, dirs] if len(x)]))

        if not len(dirs) or not recursive:
            return out.sort_index(level=0)

        more_files: list[PathSeries] = get_files_in_subfolders(dirs)

        if not len(more_files):
            return out.sort_index(level=0)

        return pd.concat([out] + more_files).sort_index(level=0)

    def read_pandas(
        self,
        columns: list[str] | dict[str, str] | None = None,
        dtypes: dict | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        return self.read(dp.read_pandas, columns, dtypes, **kwargs)

    def read_geopandas(
        self,
        columns: list[str] | dict[str, str] | None = None,
        dtypes: dict | None = None,
        **kwargs,
    ) -> gpd.GeoDataFrame:
        return self.read(sg.read_geopandas, columns, dtypes, **kwargs)

    def read(
        self,
        func,
        columns: list[str] | dict[str, str] | None = None,
        dtypes: dict | None = None,
        **kwargs,
    ) -> pd.DataFrame | gpd.GeoDataFrame:
        if self.isdir():
            raise ValueError("Cannot open a folder as a pandas DataFrame")
        if columns:
            df = func(self, columns=list(columns), **kwargs)
        else:
            df = func(self, **kwargs)

        if dtypes:
            df = df.astype(dtypes)

        if is_dict_like(columns):
            new_cols = {
                old_col: new_col
                for old_col, new_col in columns.items()
                if new_col is not None
            }

            df = df.rename(columns=new_cols, errors="raise")

        return df

    def write_new_version(
        self, df: pd.DataFrame, check_if_equal: bool = False, timeout: int | None = 30
    ) -> None:
        """Find the newest saved version of the file, adds 1 to the version number and saves the DataFrame.

        Args:
            df: DataFrame to write.
            check_if_equal: Whether to read the newest existing version and only write the new
                version if the newest is not equal to 'df'. Defaults to False.
            timeout: Minutes that must pass between each new version is written.
                To avoid accidental loop writes.
        """
        latest = self.latest_version()

        if timeout:
            updated = latest.ls().updated
            assert len(updated) == 1
            time_should_be_at_least = pd.Timestamp.now() - pd.Timedelta(minutes=timeout)
            if updated[0] > time_should_be_at_least:
                raise ValueError(
                    f"Latest version of the file was updated {updated[0]}, which "
                    f"is less than the timeout period of {timeout} minutes. "
                    "Change the timeout argument, but be sure to not save new "
                    "versions in a loop."
                )

        if check_if_equal:
            if isinstance(df, gpd.GeoDataFrame):
                latest_df = latest.read_geopandas()
            else:
                latest_df = latest.read_pandas()
            if latest_df.equals(df):
                return

        f = sg.write_geopandas if isinstance(df, gpd.GeoDataFrame) else dp.write_pandas
        return f(df, latest.add_to_version_number(1))

    def write_versionless(self, df: pd.DataFrame) -> None:
        """Write a DataFrame to the versionless path."""
        f = sg.write_geopandas if isinstance(df, gpd.GeoDataFrame) else dp.write_pandas

        return f(df, self.versionless)

    def with_suffix(self, suffix: str):
        return Path(f"{self.parent}/{self.stem}{suffix}")

    def with_version(self, version: int):
        return Path(
            f"{self.parent}/{self.versionless_stem}{self._version_prefix}{version}{self.suffix}"
        )

    def _get_directory_series(self, info):
        dirs = np.array([x["name"] for x in info if x["storageClass"] == "DIRECTORY"])
        return pd.Series(
            dirs,
            index=pd.MultiIndex.from_arrays(
                [np.zeros(dirs.shape), np.zeros(dirs.shape)]
            ),
        )

    def _get_file_series(self, info: list[dict]) -> pd.Series:
        # 2d numpy array
        fileinfo = np.array(
            [
                (x["updated"], x["size"], x["name"])
                for x in info
                if x["storageClass"] != "DIRECTORY"
            ]
        )

        if not len(fileinfo):
            return pd.Series()

        updated: pd.Index = (
            pd.to_datetime(pd.Index(fileinfo[:, 0], name="updated"))
            .round("s")
            .tz_convert("Europe/Oslo")
            .tz_localize(None)
            .round("s")
        )
        mb = pd.Index(fileinfo[:, 1], name="mb").astype(float) / 1_000_000

        index = pd.MultiIndex.from_arrays([updated, mb])

        return (
            pd.Series(fileinfo[:, 2], index=index, name="path")
            .loc[lambda x: ~x.str.endswith("/")]
            .sort_index(level=0)
        )
  
    def add_to_version_number(self, number: int):
        """Add a number to the version number."""
        new_version = self.version_number + number
        return self.with_version(new_version)

    def __truediv__(self, other: str | PathLike | PurePath):
        """Append a string or Path to the path with a forward slash."""
        if not isinstance(other, (str, PurePath, PathLike)):
            raise TypeError(
                "unsupported operand type(s) for /: "
                f"{self.__class__.__name__} and {other.__class__.__name__}"
            )
        return Path(f"{self}/{as_str(other)}")


def get_files_in_subfolders(folderinfo: PathSeries) -> list[PathSeries]:
    folderinfo: PathSeries = folderinfo.copy()

    fileinfo: list[PathSeries] = []

    while len(folderinfo):
        new_folderinfo: list[PathSeries] = []
        for path in folderinfo:
            lst: PathSeries = path.ls(recursive=False)
            fileinfo.append(lst.files())
            new_folderinfo.append(lst.dirs())

        folderinfo = pd.concat(new_folderinfo)

    return fileinfo

