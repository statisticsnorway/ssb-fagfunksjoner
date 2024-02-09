# SSB Fag-fellesfunksjoner i Python

A place for "loose, small functionality" produced at Statistics Norway in Python.
Functionality might start here, if it is to be used widely within the organization, but later be moved to bigger packages if they "grow out of proportions".

## Team: ssb-pythonistas
We are a team of statiticians which hope to curate and generalize functionality which arizes from specific needs in specific production-environments.
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