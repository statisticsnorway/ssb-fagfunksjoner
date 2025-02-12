from fagfunksjoner import SsbFormat
from fagfunksjoner.formats import store_format, get_format
import pandas as pd
import numpy as np


# ### SsbFormat is a Class to give python dictionaries some functionalities found in SAS-formats, and then some.  
# * Mapping ranges: Define range as keys and corresponding categories as values.
# * 'other'-category: Any values not found in keys during mapping will be grouped as 'other' if this key is spesified. You are free to specify corresponding value.
# * Detecting NaN-values: NaN values of different types will also be detected (e.g. '.', 'None', '', 'NA', 'NaN', 'nan', 'pd.NA', np.nan) if either an 'other'-key or a NaN-key is defined in the dictionary. If both 'other' and a NaN key is spesified, all types of nan-values will be mapped as NaN and anything not found as key is mapped to 'other'.

# generating some data with ages to group.
data = pd.DataFrame({'age': np.random.randint(1,100,1000), 
                     'id': np.random.randint(10000,99999, 1000)
                    })
# adding som NaN values to illustrate 'other' mapping
data = pd.concat([data, pd.DataFrame({'age': ['something_else' for i in  range(100)], 'id': np.random.randint(10000,99999, 100)})])

# defining a dictionary to perform range mapping of ages.
age_frmt = {'low-18': '-18',
            '19-25': '19-25',
            '26-35': '26-35',
            '36-45': '36-45',
            '46-55': '46-55',
            '56-high': '56+',
            'other': 'missing'
           }

# Initiating dictionary as SsbFormat
ssb_age_frmt = SsbFormat(age_frmt)
print(len(ssb_age_frmt))

# Notice that the length of the dictionary (format) has increased. SsbFormat creates a 1-1 correspondanse for each instance of the range groupings found in the data.
#
# The dictionary/format should not be saved in this long format. Save in short format and initiate SsbFormat before mapping. Import/read format with get_format(path) -> SsbFormat

data['age_group'] = data['age'].map(ssb_age_frmt)
print(len(ssb_age_frmt))

# Notice that the 'something_else' values in the data is recognised as missing using the 'other' category. 

print(data['age_group'].value_counts())

# You can save format either using the SsbFormat attribute .store(path, force=True) or with the function store_format(path). You should not save the long format. 

frmt_name = "age_group_long_p2025-02.json"
path = "/home/onyxia/work/ssb-fagfunksjoner/demos/test_formats/"
ssb_age_frmt.store(path + frmt_name, force=True)

# Or preferably save the short format using. In this case saving the original short dictionary using the store_format(path), function.

frmt_name = "age_group_short_p2025-02.json"
# age_frmt is the original short dictionary.
store_format(age_frmt, path+frmt_name)

# Read/import format (dictionary saved as .json) as SsbFormat

some_frmt = get_format(path+frmt_name)
print(type(some_frmt), some_frmt)

# Illustrating detection of NaN-values

nan_frmt = {'low-18': '-18',
            '19-25': '19-25',
            '26-35': '26-35',
            '36-45': '36-45',
            '46-55': '46-55',
            '56-high': '56+',
            'other': 'missing',
            'NaN': 'not-a-number'
           }
ssb_nan_frmt = SsbFormat(nan_frmt)

# Notice that the 150 np.nan-values are detected as 'not-a-number' even though the format key is 'NaN'.

# +
data = pd.concat([data, pd.DataFrame({'age': [np.nan for i in range(150)], 'id': np.random.randint(10000, 99999, 150)})])

data['nan_check'] = data['age'].map(ssb_nan_frmt)
print(data['nan_check'].value_counts(dropna=False))
