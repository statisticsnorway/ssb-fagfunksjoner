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

```python
from fagfunksjoner.prodsone.miljo import sjekk_miljo
sjekk_miljo()
```