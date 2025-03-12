import json
from fagfunksjoner import ProjectRoot
from fagfunksjoner import QualIndLogger

general_format = {
    'title': 'title of quality indicator.',
    'description': 'description of quality indicator.',
    "type": 'type of indicator, should be some pre-defined category.',
    'unit': 'percent, count, etc. ',
    'data_period': 'YYYY-MM',
    'data_source': 'dataset filepaths.',
    'value': 'NaN'
}

# ### Setter opp noen eksempler på kvalitetsindikatorer som følger malen i 'general format'
#
# legger så hver kvalitetsindikator i en dictionary med et kortnavn på kvalitetsindikatoren som nøkkel.

# +
path_to_logs = '/buckets/produkt/logg/prosessdata/'
year = 2024; month = '03'

names = ['koble_inntmot_p_t', 
         'koble_inntmo_p_t+1', 
         'arbeidsforhold_filter', 
         'ant_kobl_p_t+1', 
         'aktive_jobber_filter', 
         'total_filter_arbforh']

titles = ['Andel som kobler mot inntmot for periode t', 
          'Andel som kobler mot inntmot for periode t+1', 
          'Andel filtrert bort på arb_type', 
          'Andel arbeidsforhold hentet fra måned etter statistikkmnd (t+1).',
          'Andel filtrert bort pga inaktivt arbeidsforhold', 
          'Total andel filtrert bort i uttrekk av arbeidsforhold']

descriptions = ['Kobler arbforh mot inntmot, andel som ikke kobler.', 
                'Kobler arbforh mot inntmot, andel som ikke kobler.', 
                "Filtrerer bort arb_type = 'pensjon'.",
                'Henter arbeidsforhold fra neste mnd.',
                'arbeidsforhold som ikke er aktive i statistikkmnd filtrerers bort.',
                'Total andel filtrert bort i hele uttrekket av arbeidsforhold.']

_type = ['koblingsrate', 'koblingsrate', 'filter', 'koblingsrate', 'koblingsrate', 'filter']

unit = ['percent',  'percent', 'percent', 'percent', 'percent', 'percent']


# dette må oppdateres mtp datoer og versjoner av filer i programmet som produserer kvalitetsindikatorene
data_source = [['path/to/input/file1.parquet', 'path/to/input/file2.parquet'],
               ['path/to/input/file1.parquet', 'path/to/input/file2.parquet'],
               ['path/to/input/file1.parquet'],
               ['path/to/input/file1.parquet'],
               ['path/to/input/file1.parquet'],
               ['path/to/input/file1.parquet']
              ]
               
kval_ind_templates = {}
for i in range(len(names)):
    kval_ind_templates[names[i]] = {'title': titles[i],
                                   'description': descriptions[i],
                                   'type': _type[i],
                                   'unit': unit[i],
                                   'data_period': f"{year}-{month}",
                                   'data_source': data_source[i],
                                   'value': 'NaN'
                                   }
# -

# ## Demo

# ### Initier klassen med filsti til logg-mappe og datoer for kjøring av produksjonsløp.
#
# Hvis det finnes en logg-fil med dette datostempelet, hvis klassen importere denne.

qual_ind_logger = QualIndLogger('/buckets/produkt/logg/prosessdata/', 2024, '03')

# ### Logger en kvalitetsindikator vha klasseinstansen

# +
# kortnavnet på en av kvalitetsindikatorene som er definert over. 
qual_ind = 'koble_inntmot_p_t'
# sender inn malen for valgt kvalitetsindikator 
qual_ind_logger.log_indicator(qual_ind, kval_ind_templates[qual_ind])

# oppdaterer med verdi for kvalitetsindikatoren
qual_ind_logger.update_indicator_value(qual_ind, 'value', '66')
# -

# ### Sammenlikne flere periode for en kvalitetsindikator
#
# Vi logger først noen flere kvalitetsindikatorer for flere periode, så vi har noe å sammenlikne

# +
# definerer syntetiske verdier på kvalitetsindikatorene, og datoer for "kjøring"
values = [i for i in range(45, 50)]
years = [2023 for i in range(5)]
months = [i for i in range(1, 6)]

for i in range(len(years)):
    value = values[i]
    year = years[i]
    month = months[i]
    
    qual_ind_logger = QualIndLogger('/buckets/produkt/logg/prosessdata/', year, month)
    qual_ind = qual_ind = 'koble_inntmot_p_t'
    qual_ind_logger.log_indicator(qual_ind, kval_ind_templates[qual_ind])
    qual_ind_logger.update_indicator_value(qual_ind, 'value', value)
# -

# ### Så bruker vi klassen til å printe en enkel sammenlikning

# +
# tar utgangspunkt i en statistikkmnd da
year = 2023; month = '05'
qual_ind_logger = QualIndLogger('/buckets/produkt/logg/prosessdata/', year, month)

# indikaktoren vi vil gjør en sammenlikning over tid med
qual_ind = qual_ind = 'koble_inntmot_p_t'

qual_ind_logger.compare_months(qual_ind, n_periods = 5)
# -


