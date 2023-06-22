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
from fagfunksjoner.prodsone.miljo import sjekk_miljo
sjekk_miljo()
```

Navigate to the root of your project and back again. Do stuff while in root, like importing local functions.
```python
from fagfunksjoner.paths.project_root import navigate_root, return_to_work_dir
navigate_root()
# Do your local imports here...
return_to_work_dir()
```

Aggregate on all combinations of codes in certain columns (maybe before sending to statbank? Like proc means?)
```python
from fagfunksjoner.data.pandas_combinations import all_combos, fill_na_dict
ialt_koder = {
"skolefylk": "01-99",
"almyrk": "00",
"kjoenn_t": "0",
"sluttkomp": "00",
}
kolonner = list(ialt_koder.keys())
tab = all_combos(vgogjen, kolonner, {'antall': sum})
tab = fill_na_dict(tab, ialt_koder)
```