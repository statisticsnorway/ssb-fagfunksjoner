import datetime as dt
from pandas import Timestamp


class StandardTimeFormats:
    """
    Class for standard timeformats according to ISO 8601 which Statistics Norway are following.
    Docs: https://statistics-norway.atlassian.net/wiki/spaces/MPD/pages/2953084957/Standardformater#Dato--og-tidsformater-(ISO-8601)
    See: https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    """

    @staticmethod
    def date_time():
        "Date and time format YYYY-MM-DDThh:mm:ss"
        return "%G-%m-%dT%H:%M:%S"

    @staticmethod
    def date():
        "Date format YYYY-MM-DD"
        return "%G-%m-%d"

    @staticmethod
    def month():
        "Gives year and month format YYYY-MM"
        return "%G-%m"

    @staticmethod
    def year():
        "Gives only year format YYYY"
        return "%G"

    @staticmethod
    def week():
        "Gives year and week format YYYY-WW"
        return "%GW%V"

    @staticmethod
    def year_days():
        "Gives year and day of year format YYYY-DDD"
        return "%Y-%j"


class TimeStandards:
    """
    Class for use time and periods with standard formats.
    """

    @staticmethod
    def date_time(dates: dt.datetime = None):
        "Gives date and time with standard format"
        if dates is None:
            dates = dt.datetime.now()
        return dates.strftime(TimeFormats.date_time())

    @staticmethod
    def timestamp():
        "Gives date and time right now with standard format"
        return TimeStandards.date_time()

    @staticmethod
    def date(dates: dt.date = None):
        "Gives date with standard format"
        if dates is None:
            dates = dt.date.today()
        return dates.strftime(TimeFormats.date())

    @staticmethod
    def month(dates: dt.date = None):
        "Gives month period with standard format"
        if dates is None:
            dates = dt.date.today()
        return dates.strftime(TimeFormats.month())

    @staticmethod
    def year(dates: dt.date = None):
        "Gives year period with standard format"
        if dates is None:
            dates = dt.date.today()
        return dates.strftime(TimeFormats.year())

    @staticmethod
    def week(dates: dt.date = None):
        "Gives week period with standard format"
        if dates is None:
            dates = dt.date.today()
        return dates.strftime(TimeFormats.week())

    @staticmethod
    def year_days(dates: dt.date = None):
        "Gives day of year period with standard format"
        if dates is None:
            dates = dt.date.today()
        return dates.strftime(TimeFormats.year_days())

    @staticmethod
    def quarterly(dates: dt.date = None):
        "Gives quarter period with standard format"
        if dates is None:
            dates = dt.date.today()
        return f"{TimeStandards.year(dates)}-Q{Timestamp(dates).quarter}"

    @staticmethod
    def bimester(dates: dt.date = None):
        "Gives bimester period with standard format"
        if dates is None:
            dates = dt.date.today()
        bi = {
            "1": [1, 2],
            "2": [3, 4],
            "3": [5, 6],
            "4": [7, 8],
            "5": [9, 10],
            "6": [11, 12]
        }
        period = TimeStandards._find_period(bi, dates.month)
        return f"{TimeStandards.year(dates)}-B{period}"

    @staticmethod
    def triannual(dates: dt.date = None):
        "Gives triannual period with standard format"
        if dates is None:
            dates = dt.date.today()
        tri = {
            "1": [1, 2, 3, 4],
            "2": [5, 6, 7, 8],
            "3": [9, 10, 11, 12]
        }
        period = TimeStandards._find_period(tri, dates.month)
        return f"{TimeStandards.year(dates)}-T{period}"

    @staticmethod
    def halfyear(dates: dt.date = None):
        "Gives halfyear period with standard format"
        if dates is None:
            dates = dt.date.today()
        hy = {
            "1": [1, 2, 3, 4, 5, 6],
            "2": [7, 8, 9, 10, 11, 12]
        }
        period = TimeStandards._find_period(hy, dates.month)
        return f"{TimeStandards.year(dates)}-H{period}"

    @staticmethod
    def _find_period(period_dict: dict, month: int) -> str:
        for k, v in period_dict.items():
            if month in v:
                period = k
        return period