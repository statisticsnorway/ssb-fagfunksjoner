import pathlib
from datetime import date

import pandas as pd
import requests

URL_NORGES_BANK = (
    "https://data.norges-bank.no/api/data/EXR/{frequency}.{currency}.NOK.SP?"
    "format=csv&startPeriod={date_from}&endPeriod={date_to}"
    "&locale={language}&detail={detail}"
)


def exchange_rates(
    csv_data: str | pathlib.Path, dec_point: str = ",", time_column: int = 14
) -> pd.DataFrame:
    """Convert exchange rate data to a dataframe by reading as CSV.

    Args:
        csv_data (str): CSV data as a string.
        dec_point (str): The decimal character used in the CSV. Defaults to ",".
        time_column (int): Time column, the column number to convert to dates. Defaults to 14.

    Returns:
        pd.DataFrame: The data retrieved from the API.
    """
    return pd.read_csv(csv_data, sep=";", decimal=dec_point, parse_dates=[time_column])


def download_exchange_rates(
    currency: str = "",
    frequency: str = "M",
    date_from: str = "2021-01-01",
    date_to: str | None = None,
    language: str = "no",
    detail: str = "full",
) -> pd.DataFrame:
    """Fetch exchange rates from Norges Bank's API.

    See https://app.norges-bank.no/query/index.html#/no/

    Args:
        currency (str): Specified in UPPER case letters. For multiple currencies, use a
            plus sign (e.g., 'GBP+EUR+USD'). No value gives all currencies.
        frequency (str): Can be B (Business, daily rates), M (monthly rates),
            A (annual rates). For multiple frequencies, use a plus sign
            (e.g., 'A+M'). No value gives all frequencies. For annual rates,
            the time interval must cover a full year, similarly for months.
        date_from (str): Specified in the format YYYY-MM-DD.
        date_to (str | None): Specified in the format YYYY-MM-DD. If None, defaults to today's date.
        language (str): 'no' for Norwegian, 'en' for English.
        detail (str): 'full' gives both data and attributes, 'dataonly' gives only data,
            'serieskeysonly' gives series without data or attributes,
            'nodata' gives series and attributes without data.

    Returns:
        pd.DataFrame: The data retrieved from the API.
    """
    if date_to is None:
        date_to = date.today().strftime("%Y-%m-%d")
    cache_path = pathlib.Path(f"exchange_rates_{frequency}-{currency}-{date_to}.csv")

    if not cache_path.exists():
        url = URL_NORGES_BANK.format(
            frequency=frequency,
            currency=currency,
            date_from=date_from,
            date_to=date_to,
            language=language,
            detail=detail,
        )
        response = requests.get(url)
        response.raise_for_status()
        cache_path.write_text(response.text)

    dec_point = "," if language == "no" else "."
    time_column = 14 if detail == "full" else 8
    return exchange_rates(cache_path, dec_point=dec_point, time_column=time_column)
