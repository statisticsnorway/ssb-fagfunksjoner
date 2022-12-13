import pandas as pd
import datetime.datetime as date


def valutakurser(frekvens='M', 
                 valuta='', 
                 fradato='2021-01-01', 
                 tildato=date.today().strftime('%Y-%m-%d'), 
                 spraak='no', detalj='full'):
    """ 
    Henter valutakurser fra Norges Bank sitt API, se https://app.norges-bank.no/query/index.html#/no/
    Parametere:
        frekvens: kan være B (Business, daglige kurser), M (månedlige kurser), A (annual, årlige kurser). Hvis flere ønskes settes det en pluss i mellom ('A+M'). 
                  Ingen verdi gir alle frekvenser. For å få ut årskurser må tidsintervallet inneholdet en hel årgang, tilsvarende gjelder for måneder.
        valuta: angis med STORE bokstaver. Hvis flere kurser ønskes settes det et pluss i mellom (f.eks. 'GBP+EUR+USD'). Ingen verdi gir alle kurser.
        fradato: angis på formen ÅÅÅÅ-MM-DD
        tildato: angis på formen ÅÅÅÅ-MM-DD
        spraak: no for norsk, en for engelsk
        detalj: full gir både data og attributter, dataonly gir kun data, serieskeysonly gir kun serier uten data eller attributter,
                nodata gir serier og attributter uten data.
    """
    desimal = ',' if spraak == 'no' else '.'   
    tidskolonne = [14] if detalj == 'full' else [8]   
    url = f'https://data.norges-bank.no/api/data/EXR/{frekvens}.{valuta}.NOK.SP?format=csv&startPeriod={fradato}&endPeriod={tildato}&locale={spraak}&detail={detalj}'
    utdata = pd.read_csv(url, sep=';', decimal=desimal, parse_dates=tidskolonne)
    return utdata
