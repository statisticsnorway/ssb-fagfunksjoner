# SSB Fag-fellesfunksjoner i Python

[![PyPI](https://img.shields.io/pypi/v/ssb-fagfunksjoner.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/ssb-fagfunksjoner.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/ssb-fagfunksjoner)][pypi status]
[![License](https://img.shields.io/pypi/l/ssb-fagfunksjoner)][license]

[![Documentation](https://github.com/statisticsnorway/ssb-fagfunksjoner/actions/workflows/docs.yml/badge.svg)][documentation]
[![Tests](https://github.com/statisticsnorway/ssb-fagfunksjoner/actions/workflows/tests.yml/badge.svg)][tests]
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-fagfunksjoner&metric=coverage)][sonarcov]
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-fagfunksjoner&metric=alert_status)][sonarquality]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)][poetry]

[pypi status]: https://pypi.org/project/ssb-fagfunksjoner/
[documentation]: https://statisticsnorway.github.io/ssb-fagfunksjoner
[tests]: https://github.com/statisticsnorway/ssb-fagfunksjoner/actions?workflow=Tests

[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-fagfunksjoner
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-fagfunksjoner
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

A place for "loose, small functionality" produced at Statistics Norway in Python.
Functionality might start here, if it is to be used widely within the organization, but later be moved to bigger packages if they "grow out of proportions".

## Team: ssb-pythonistas
We are a team of statisticians which hope to curate and generalize functionality which arizes from specific needs in specific production-environments.
We try to take responsibility for this functionality to be generalized and available to all of statistics Norway through this package.

[Pypi-account](https://pypi.org/user/ssb-pythonistas/)
[Github-team](https://github.com/orgs/statisticsnorway/teams/ssb-pythonistas)

## Contributing
Please make contact with one of our team members, to see if you can join, or how to send in a PR for approval into the package.


## Installing
```bash
poetry add ssb-fagfunksjoner
```

## Usage
Check if you are on Dapla or in prodsone.
```python
from fagfunksjoner import check_env


check_env()
```

Navigate to the root of your project and back again. Do stuff while in root, like importing local functions.
```python
from fagfunksjoner import ProjectRoot


with ProjectRoot():
    ... # Do your local imports here...
```


Setting up password with saspy
```python
from fagfunksjoner.prodsone import saspy_ssb


saspy_ssb.set_password() # Follow the instructions to set the password
saspy_ssb.saspy_df_from_path("path")
```


Aggregate on all combinations of codes in certain columns (maybe before sending to statbank? Like proc means?)
```python
from fagfunksjoner import all_combos_agg


ialt_koder = {
"skolefylk": "01-99",
"almyrk": "00",
"kjoenn_t": "0",
"sluttkomp": "00",
}
kolonner = list(ialt_koder.keys())
tab = all_combos_agg(vgogjen,
                     groupcols=kolonner,
                     aggargs={'antall': sum},
                     fillna_dict=ialt_koder)
```

Perform mapping using SsbFormat. Behaves like a dictionary. Has functionality for mapping ranges and 'other'-category and detecting different types of NaN-values.

```python
from fagfunksjoner import SsbFormat

age_frmt = {
'low-18': '-18',
'19-25': '19-25',
'26-35': '26-35',
'36-45': '36-45',
'46-55': '46-55',
'56-high': '56+',
'other': 'missing'
}

# convert dictionary to SsbFormat
ssb_age_frmt = SsbFormat(age_frmt)

# perform mapping of age using ranges in format. 
df['age_group'] = df['age'].map(ssb_age_frmt)

print(df['age_group'].value_counts())

# save format
from fagfunksjoner.formats import store_format
store_format(path+'format_name_p2025-02.json')

# or 
# NB! after performing range mapping using SsbFormat. The dictionary will be long. You should save a short version. Inspect the dictionary before saving/storing.
ssb_age_frmt.store(path + 'format_name_p2025-02.json', force=True)

# read format/import format (dictionary saved as .json) as SsbFormat
from fagfunksjoner.formats import get_format
some_frmt = get_format(path+'format_name.json')
```

### Opening archive-files based on Datadok-api in prodsone
We have "flat files", which are not comma seperated. These need metadata to correctly open. In SAS we do this with "lastescript". But there is an API to old Datadok in prodsone, so these functions let you just specify a path, and attempt to open the flat files directly into pandas, with the metadata also available.

```python
from fagfunksjoner import open_path_datadok


archive_object = open_path_datadok("$TBF/project/arkiv/filename/g2022g2023")
# The object now has several important attributes
archive_object.df  # The Dataframe of the archived data
archive_object.metadata_df  # Dataframe representing metadata
archive_object.codelist_df  # Dataframe representing codelists
archive_object.codelist_dict  # Dict of codelists
archive_object.names  # Column names in the archived data
archive_object.datatypes  # The datatypes the archivdata ended up having?
archive_object.widths  # Width of each column in the flat file

```
### Operation to Oracle database

Remember that any credidential values to the database should not be stored
in our code. Possibly use python-dotenv package to make this easier.

Example for a normal select query where we expect not too many records:
```python
import os

import pandas as pd
from doteng import load_dotenv

from fagfunksjoner.prodsone import Oracle


load_dotenv()

query = "select vare, pris from my_db_table"

ora = Oracle(pw=os.getenv("my-secret-password"),
             db=os.getenv("database-name"))

df = pd.DataFrame(ora.select(sql=query))

ora.close()
```

Example for a select query where possibly many records:
```python
import os

import pandas as pd
from doteng import load_dotenv

from fagfunksjoner.prodsone import Oracle


load_dotenv()

query = "select vare, pris from my_db_table"

ora = Oracle(pw=os.getenv("my-secret-password"),
             db=os.getenv("database-name"))

df = pd.DataFrame(ora.selectmany(sql=query, batchsize=10000))

ora.close()
```

Example for inserting new records into database(note that ordering of
the columns in sql query statement and data are important):
```python
import os

import pandas as pd
from doteng import load_dotenv

from fagfunksjoner.prodsone import Oracle


load_dotenv()

df = pd.DataFrame(
    {
        "vare": ["banan", "eple"],
        "pris": [11, 10]
    }
)

data = list(df.itertuples(index=False, name=None))

query = "insert into my_db_table(vare, pris) values(:vare, :pris)"

ora = Oracle(pw=os.getenv("my-secret-password"),
             db=os.getenv("database-name"))

ora.insert_or_update(sql=query, update=data)

ora.close()
```

Example for updating records in the database(note that ordering of
the columns in sql query statement and data are important. It is also
important that the query doesn't update other records than it should.
Having some kind of ID to the records will be very usefull!):
```python
import os
import pandas as pd
from doteng import load_dotenv
from fagfunksjoner.prodsone import Oracle
load_dotenv()

df = pd.DataFrame(
    {
        "id": ["12345", "54321"]
        "vare": ["banan", "eple"],
        "pris": [11, 10]
    }
)

data = list(df[["vare", "pris", "id"]].itertuples(index=False, name=None))

query = "update my_db_table set vare = :vare, pris = :pris where id = :id"

ora = Oracle(pw=os.getenv("my-secret-password"),
             db=os.getenv("database-name"))

ora.insert_or_update(sql=query, update=data)

ora.close()
```

It also support context manager. This is handy when working with big data,
and you then have to work more lazy. Or you want to do multiple operations
to several tables without closing the connections. Or any other reasons...
An easy case; reading large data from database and write it to a parquet
file, in batches:
```python
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from doteng import load_dotenv
from fagfunksjoner.prodsone import Oracle, OraError
load_dotenv()

select_query = "select vare, pris from my_db_table"
parquet_write_path = "write/to/path/datafile.parquet"

with pq.ParquetWriter(parquet_write_path) as pqwriter: # pyarrow schema might be needed
    try:
        # will go straight to cursor
        with Oracle(pw=os.getenv("my-secret-password"),
                db=os.getenv("database-name")) as concur:
            concur.execute(select_query)
            cols = [c[0].lower() for c in cur.description]
            while True:
                rows = cur.fetchmany(10_000) # 10.000 rows per batch
                if not rows:
                    break
                else:
                    data = [dict(zip(cols, row)) for row in rows]
                    tab = pa.Table.from_pylist(data)
                    # this will write data to one row group per batch
                    pqwriter.write_table(tab)
    except OraError as error:
        raise error
```

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_SSB Fagfunksjoner_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [Statistics Norway]'s [SSB PyPI Template].

[statistics norway]: https://www.ssb.no/en
[pypi]: https://pypi.org/
[ssb pypi template]: https://github.com/statisticsnorway/ssb-pypitemplate
[file an issue]: https://github.com/statisticsnorway/ssb-fagfunksjoner/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/statisticsnorway/ssb-fagfunksjoner/blob/main/LICENSE
[contributor guide]: https://github.com/statisticsnorway/ssb-fagfunksjoner/blob/main/CONTRIBUTING.md
[reference guide]: https://statisticsnorway.github.io/ssb-fagfunksjoner/reference.html
