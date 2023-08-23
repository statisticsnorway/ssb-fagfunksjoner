# # Tester all_combos_agg

import pandas as pd
import pandas_combinations_lono

pandas_combinations_lono

data = {
        'alder': [20, 60, 33, 33, 20],
        'kommune': ['0301', '3001', '0301', '5401', '0301'],
        'kjonn': ['1', '2', '1', '2', '2'],
        'inntekt': [1000000, 120000, 220000, 550000, 50000],
        'formue': [25000, 50000, 33000, 44000, 90000]
    }
pers = pd.DataFrame(data)
pers

agg1 = pandas_combinations_lono.all_combos_agg(pers, groupcols=['kjonn'], keep_empty=True, func={'inntekt':['mean', 'sum']})
display(agg1)

# keep_empty=True gir feilmelding
#agg2 = pandas_combinations_lono.all_combos_agg(pers, groupcols=['kjonn', 'alder'], keep_empty=True, func={'inntekt':['mean', 'sum']})
agg2 = pandas_combinations_lono.all_combos_agg(pers, groupcols=['kjonn', 'alder'], func={'inntekt':['mean', 'sum']})
display(agg2)

agg3 = pandas_combinations_lono.all_combos_agg(pers, groupcols=['kjonn', 'alder'], grand_total=True,
                                               grand_total_text='Grand total',
                                               func={'inntekt':['mean', 'sum']})
display(agg3)

agg4 = pandas_combinations_lono.all_combos_agg(pers, groupcols=['kjonn', 'alder'], 
                                               fillna_dict={'kjonn': 'Total kjønn', 'alder': 'Total alder'}, 
                                               func={'inntekt':['mean', 'sum'], 'formue': ['count', 'min', 'max']}, 
                                               grand_total=True
                                              )
display(agg4)

groupcols = pers.columns[0:3].tolist()
func_dict = {'inntekt':['mean', 'sum'], 'formue': ['sum', 'std', 'count']}
fillna_dict = {'kjonn': 'Total kjønn', 'alder': 'Total alder', 'kommune': 'Total kommune'}
agg5 = pandas_combinations_lono.all_combos_agg(pers, groupcols=groupcols, 
                                               func=func_dict,
                                               fillna_dict=fillna_dict, 
                                               grand_total=True,
                                               grand_total_text='All'
                                              )
display(agg5)

agg5 = pandas_combinations_lono.all_combos_agg(pers, groupcols=['kjonn', 'alder'], 
                                               fillna_dict={'kjonn': 'Total kjønn', 'alder': 'Total alder'}, 
                                               func={'inntekt':['mean', 'sum'], 'formue': ['sum', 'std']},
                                               grand_total=True
                                              )
display(agg5)

gt = pd.DataFrame(pers.agg({'inntekt':['mean', 'sum']}).reset_index())
gt = (pd.DataFrame(gt
                   .agg({'inntekt':['mean', 'sum']})
                   .T)
              .T)
gt                  

gt = pd.DataFrame(pers['inntekt'].agg(['mean', 'sum'])).reset_index()
gt['x','z'] = 1
gt = gt.pivot(index=[('x','z')], columns='index', values= ['inntekt']).reset_index().drop(columns=[('x','z')])
gt

gt = pd.DataFrame(pers.agg({'inntekt':['mean', 'sum'], 'formue': ['sum', 'std']})).reset_index()
display(gt)
gt['x','z'] = 1
gt2 = gt.pivot(index=[('x','z')], columns='index').reset_index().drop(columns=[('x','z')])
gt2

gt.columns.get_level_values(0).unique()


