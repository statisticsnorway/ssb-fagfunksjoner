from fagfunksjoner.data.pandas_combinations import all_combos_agg
import pandas as pd
import pytest
from unittest import mock


def dataset1():
    return pd.DataFrame({
        "sex" : ["Mann", "Kvinne", "Mann", "Mann", "Kvinne", "Mann"],
        "age" : ["40", "50", "60", "50", "60", "40"],
        "points" : [100, 90, 80, 70, 60, 50],
    })


def test_simple_sum_fillna():
    data_in = dataset1()
    result = all_combos_agg(data_in,
           ["sex", "age"],
           {"points": sum},
           fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
              )
    data_out = pd.DataFrame({'sex': {0: 'Begge kjoenn', 1: 'Begge kjoenn', 2: 'Begge kjoenn', 3: 'Kvinne', 4: 'Mann', 5: 'Kvinne', 6: 'Kvinne', 7: 'Mann', 8: 'Mann', 9: 'Mann'}, 'age': {0: '40', 1: '50', 2: '60', 3: 'Alle aldre', 4: 'Alle aldre', 5: '50', 6: '60', 7: '40', 8: '50', 9: '60'}, 'points': {0: 150, 1: 160, 2: 140, 3: 150, 4: 300, 5: 90, 6: 60, 7: 150, 8: 70, 9: 80}, 'level': {0: 1, 1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 3, 8: 3, 9: 3}, 'ways': {0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 2, 6: 2, 7: 2, 8: 2, 9: 2}})
    assert result.equals(data_out)


    
def test_fillna_dict_keep_empty():
    data_in = dataset1()
    result = all_combos_agg(data_in, 
           ["sex", "age"],
           {"points": sum},
           fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
           keep_empty=True
              )
    data_out = pd.DataFrame({'sex': {0: 'Begge kjoenn', 1: 'Begge kjoenn', 2: 'Begge kjoenn', 3: 'Kvinne', 4: 'Mann', 5: 'Kvinne', 6: 'Kvinne', 7: 'Kvinne', 8: 'Mann', 9: 'Mann', 10: 'Mann'}, 'age': {0: '40', 1: '50', 2: '60', 3: 'Alle aldre', 4: 'Alle aldre', 5: '40', 6: '50', 7: '60', 8: '40', 9: '50', 10: '60'}, 'points': {0: 150, 1: 160, 2: 140, 3: 150, 4: 300, 5: 0, 6: 90, 7: 60, 8: 150, 9: 70, 10: 80}, 'level': {0: 1, 1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 3, 8: 3, 9: 3, 10: 3}, 'ways': {0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 2, 6: 2, 7: 2, 8: 2, 9: 2, 10: 2}})
    assert result.equals(data_out)
    
    
def test_grand_total():
    data_in = dataset1()
    result = all_combos_agg(data_in, 
           ["sex", "age"],
           {"points": sum},
           fillna_dict={"sex": "Begge kjoenn", "age": "Alle aldre"},
           keep_empty=True,
           grand_total = "I alt"
              )
    assert "I alt" in result["sex"].to_list() and "I alt" in result["age"].to_list()
    assert "Begge kjoenn" in result["sex"].to_list() and "Alle aldre" in result["age"].to_list()