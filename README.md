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


Querying internal oracle-database "DB1P"
```python
from fagfunksjoner import query_db1p
sporring = "SELECT SNR_NUDB FROM NUDB_ADM.TAB_UTD_PERSON"
df = query_db1p(sporring)
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
