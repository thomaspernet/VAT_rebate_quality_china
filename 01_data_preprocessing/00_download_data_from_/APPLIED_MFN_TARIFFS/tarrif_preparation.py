import pandas as pd
import numpy as np
import glob
import os

concordance = (pd.read_csv('Product_correspondance.txt',
                           sep='\t', lineterminator='\r',
                           dtype={
                               'HS_used': 'str',
                               'TL_used': 'str',
                               'TL_selected': 'str'
                           })
               .assign(
    HS_used=lambda x: x['HS_used'].str.replace('\n|\"', '')
)
    .rename(columns={'TL_used': 'HS07', 'TL_selected': 'HS02'})
    #.drop(columns=['HS_used', 'HS_selected'])
    #.drop_duplicates(subset=['HS07'], keep='first')
    #.drop_duplicates(subset=['HS02'], keep='first')
)

hs_constant = concordance.loc[lambda x: x['HS07'] == x['HS02']]['HS07'].unique()

concordance_unique = (
concordance.loc[lambda x: ~x['HS07'].isin(hs_constant)]
.drop_duplicates(subset=['HS07'], keep='first')
.drop_duplicates(subset=['HS02'], keep='first')
)

concordance.loc[lambda x: x.duplicated(subset=['HS07'])].sort_values(by='HS07')
concordance.loc[lambda x: x.duplicated(subset=['HS02'])].sort_values(by='HS02')

concordance_unique.loc[lambda x: x['HS02'].isin(['903039'])]


df_tarrif = (pd.concat(map(
    lambda file:
    pd.read_csv(file,
                sep='\t', lineterminator='\r',
                dtype={
                    'Year': 'str',
                    'HS code level': 'str',
                    'Product': 'str'
                }
                ),
    glob.glob(os.path.join('', "*_Tariffs.txt")),
)
)
    .reset_index()
    .assign(
    Reporter=lambda x: x['Reporter'].str.replace('\n|\"', ''),
    index_tarrif=lambda x: x.index
)
    .dropna(subset=['Year'])
    .rename(columns={'Product': 'temp_HS'})
    .drop(columns=['List of unit values'])
)

df_tarrif.shape

# Merge HS concordance
reindex = ['index_tarrif', 'Reporter', 'Year', 'AVDuty average', 'HS02']

df_tarrif_hs2 = (
    df_tarrif.loc[lambda x: x['Year'].isin(['2007', '2008', '2009', '2010'])]
    .merge(concordance_unique, how='inner', left_on='temp_HS', right_on='HS07')
    .drop(columns=['temp_HS', 'Share_TL_used', 'HS07'])
    .reindex(columns=reindex)
)

df_tarrif_hs2.loc[lambda x: x['HS02'].isin(['903039'])]
df_tarrif_hs2.shape

#(df_tarrif.loc[lambda x: ~x['index_tarrif'].isin(df_tarrif_hs2['index_tarrif'])]
# .rename(columns={'temp_HS': 'HS02'})
# ).loc[lambda x: x['HS02'].isin(['903039'])]

tarrif_HS2 = (pd.concat([

    (df_tarrif.loc[lambda x: ~x['index_tarrif'].isin(df_tarrif_hs2['index_tarrif'])]
     .rename(columns={'temp_HS': 'HS02'})
     .reindex(columns=reindex)
     ),
    df_tarrif_hs2
], axis=0)
.sort_values(by=['Year','HS02','AVDuty average'])
.drop_duplicates(subset=['Year','HS02'], keep='last')
)

tarrif_HS2.shape

test = (tarrif_HS2
.rename(columns= {'AVDuty average': 'import_taxe', 'Year': 'year'})
.drop(columns=['index_tarrif'])
.sort_values(by = ['HS02', 'year'])
.to_csv('Applied_MFN_Tariffs_hs02_china_2002_2010.csv', index = False)
)


#tarrif_HS2.loc[lambda x: x['HS02'].isin(['121190'])]
test.shape
test.loc[lambda x: x.duplicated(subset=['year','HS02'])].sort_values(by=['year','HS02'])
test.loc[lambda x: x['HS02'].isin(['903039'])]
