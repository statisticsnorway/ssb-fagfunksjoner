"""
Functions that helps Statistics Norway following their specified standards.
Docs: https://statistics-norway.atlassian.net/wiki/spaces/MPD/pages/2953084957/Standardformater
"""

from typing import List, Dict
import datetime as dt
from pandas import Timestamp


FORMATS: dict = {
    "date_time": "%Y-%m-%dT%H:%M:%S",
    "date": "%Y-%m-%d",
    "month": "%Y-%m",
    "year": "%Y",
    "week": "%GW%V",
    "year_days": "%Y-%j",
}


def date_time(date: dt.datetime = None) -> str:
    """
    Gives date and time with standard format.
    See: https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes

    Parameters:
    -----------
    date:
        A specified datetime you want to convert to string format.
        If not specified, it will give the datetime right now.

    Returns:
    --------
    Datetime in string format, YYYY-MM-DDThh:mm:ss, and according to the standards.
    """
    if date is None:
        date = dt.datetime.now()
    return date.strftime(FORMATS["date_time"])


def timestamp() -> str:
    """
    Gives date and time right now with standard format.
    """
    return date_time()


def date(date: dt.date = None) -> str:
    """
    Gives date with standard format.

    Parameters:
    -----------
    date:
        A specified date you want to convert to string format.
        If not specified, it will give the date today.

    Returns:
    --------
    Date in string format YYYY-MM-DD.
    """
    if date is None:
        date = dt.date.today()
    return date.strftime(FORMATS["date"])


def month(date: dt.date = None) -> str:
    """
    Gives month period with standard format.

    Parameters:
    -----------
    date:
        A specified date you want to convert to string format.
        If not specified, it will give the date today.

    Returns:
    --------
    Month period in string format YYYY-MM.
    """
    if date is None:
        date = dt.date.today()
    return date.strftime(FORMATS["month"])


def year(date: dt.date = None) -> str:
    """
    Gives year period with standard format.

    Parameters:
    -----------
    date:
        A specified date you want to convert to string format.
        If not specified, it will give the date today.

    Returns:
    --------
    Month period in string format YYYY.
    """
    if date is None:
        date = dt.date.today()
    return date.strftime(FORMATS["year"])


def week(date: dt.date = None) -> str:
    """
    Gives week period with standard format.

    Parameters:
    -----------
    date:
        A specified date you want to convert to string format.
        If not specified, it will give the date today.

    Returns:
    --------
    Month period in string format YYYYWww.
    """
    if date is None:
        date = dt.date.today()
    return date.strftime(FORMATS["week"])


def year_days(date: dt.date = None) -> str:
    """
    Gives day of year period with standard format.

    Parameters:
    -----------
    date:
        A specified date you want to convert to string format.
        If not specified, it will give the date today.

    Returns:
    --------
    Day of year period in string format YYYY-DDD.
    """
    if date is None:
        date = dt.date.today()
    return date.strftime(FORMATS["year_days"])


def quarterly(date: dt.date = None) -> str:
    """
    Gives quarter period with standard format.

    Parameters:
    -----------
    date:
        A specified date you want to convert to string format.
        If not specified, it will give the date today.

    Returns:
    --------
    Quarter period in string format YYYY-Qq.
    """
    if date is None:
        date = dt.date.today()
    return f"{year(date)}-Q{Timestamp(date).quarter}"


def bimester(date: dt.date = None) -> str:
    """
    Gives bimester period with standard format.

    Parameters:
    -----------
    date:
        A specified date you want to convert to string format.
        If not specified, it will give the date today.

    Returns:
    --------
    Bimester period in string format YYYY-Bb.
    """
    if date is None:
        date = dt.date.today()
    bi = {
        "1": [1, 2],
        "2": [3, 4],
        "3": [5, 6],
        "4": [7, 8],
        "5": [9, 10],
        "6": [11, 12],
    }
    period = _find_period(bi, date.month)
    return f"{year(date)}-B{period}"


def triannual(date: dt.date = None) -> str:
    """
    Gives triannual period with standard format.

    Parameters:
    -----------
    date:
        A specified date you want to convert to string format.
        If not specified, it will give the date today.

    Returns:
    --------
    Triannual period in string format YYYY-Tt.
    """
    if date is None:
        date = dt.date.today()
    tri = {"1": [1, 2, 3, 4], "2": [5, 6, 7, 8], "3": [9, 10, 11, 12]}
    period = _find_period(tri, date.month)
    return f"{year(date)}-T{period}"


def halfyear(date: dt.date = None) -> str:
    """
    Gives halfyear period with standard format.

    Parameters:
    -----------
    date:
        A specified date you want to convert to string format.
        If not specified, it will give the date today.

    Returns:
    --------
    Halfyear period in string format YYYY-Hh.
    """
    if date is None:
        date = dt.date.today()
    hy = {"1": [1, 2, 3, 4, 5, 6], "2": [7, 8, 9, 10, 11, 12]}
    period = _find_period(hy, date.month)
    return f"{year(date)}-H{period}"


def _find_period(period_dict: Dict[str, List[int]], month: int) -> str:
    """
    Finds any self-made period based on a dict where keys are
    the self-mabe period, and the values are list of int that
    represent a month between 1 and 12.

    Parameters:
    -----------
    period_dict:
        Dict with self-made period standard. The keys represent the
        self-made period, and the values are list of int that represent
        a month between 1 and 12.
    month:
        Current month in number, between 1 and 12.

    Returns:
    --------
    The self-made period, the key, fram the period_dict.
    """
    for k, v in period_dict.items():
        if month in v:
            period = k
    return period
