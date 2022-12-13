from datetime import date
import pathlib

import pandas as pd
import requests

URL_NORGES_BANK = (
    "https://data.norges-bank.no/api/data/EXR/{frequency}.{currency}.NOK.SP?"
    "format=csv&startPeriod={date_from}&endPeriod={date_to}"
    "&locale={language}&detail={detail}"
)


def exchange_rates(csv_data, dec_point=",", time_column=14):
    """Gjør om valutakursdata til en DataFrame."""
    return pd.read_csv(csv_data, sep=";", decimal=dec_point, parse_dates=[time_column])


def download_exchange_rates(
    currency="",
    frequency="M",
    date_from="2021-01-01",
    date_to=None,
    language="no",
    detail="full",
):
    """Henter valutakurser fra Norges Bank sitt API.

    Se https://app.norges-bank.no/query/index.html#/no/

    Parametere:
        frekvens: kan være B (Business, daglige kurser), M (månedlige kurser),
                  A (annual, årlige kurser). Hvis flere ønskes settes det en
                  pluss i mellom ('A+M').
                  Ingen verdi gir alle frekvenser. For å få ut årskurser må
                  tidsintervallet inneholdet en hel årgang, tilsvarende gjelder for måneder.
        valuta: angis med STORE bokstaver. Hvis flere kurser ønskes settes det et pluss
                i mellom (f.eks. 'GBP+EUR+USD'). Ingen verdi gir alle kurser.
        fradato: angis på formen ÅÅÅÅ-MM-DD
        tildato: angis på formen ÅÅÅÅ-MM-DD
        spraak: no for norsk, en for engelsk
        detalj: full gir både data og attributter, dataonly gir kun data,
                serieskeysonly gir kun serier uten data eller attributter,
                nodata gir serier og attributter uten data.
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
        if not response:
            response.raise_for_status()
        cache_path.write_text(response.text)

    dec_point = "," if language == "no" else "."
    time_column = 14 if detail == "full" else 8
    return exchange_rates(cache_path, dec_point=dec_point, time_column=time_column)
