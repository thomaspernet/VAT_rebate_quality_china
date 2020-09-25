import pandas as pd 
import numpy as np
from pathlib import Path
import os, re,  requests, json 
from GoogleDrivePy.google_authorization import authorization_service
from GoogleDrivePy.google_platform import connect_cloud_platform


path = os.getcwd()
parent_path = str(Path(path).parent)
project = 'valid-pagoda-132423'


auth = authorization_service.get_authorization(
    path_credential_gcp = "{}/creds/service.json".format(parent_path),
    verbose = False#
)

gcp_auth = auth.authorization_gcp()
gcp = connect_cloud_platform.connect_console(project = project, 
                                             service_account = gcp_auth) 


query = """
WITH merge_data AS (
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value,
    CASE WHEN Trade_type = '进料加工贸易' 
    OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime 
  FROM 
    `China.tradedata_*` 
  WHERE 
    (
      _TABLE_SUFFIX BETWEEN '2002' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND (
      Trade_type = '进料加工贸易' 
      OR Trade_type = '一般贸易' 
      OR Trade_type = '来料加工装配贸易'
    ) 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '国有' 
      OR Business_type = '私营' 
      OR Business_type = '集体'
      
    ) 
  UNION ALL 
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value,
    CASE WHEN Trade_type = '进料加工贸易' 
    OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime 
  FROM 
    `China.tradedata_*` 
  WHERE 
    NOT(
      _TABLE_SUFFIX BETWEEN '2000' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业'
    )
) 
SELECT 
      * 
    FROM 
      (
        WITH aggregate AS (
          SELECT  
            city_prod, 
            year, 
            regime,
            HS6, 
            destination, 
            SUM(Quantity) as Quantity, 
            SUM(value) as value 
          FROM 
            merge_data 
          GROUP BY 
            year, 
            regime, 
            HS6, 
            city_prod, 
            destination
        ) 
        SELECT
          cityen,
          geocode4_corr, 
          aggregate.year, 
          regime, 
          aggregate.HS6, 
          Country_en, 
          ISO_alpha, 
          Quantity, 
          value, 
          SAFE_DIVIDE(value, Quantity) AS unit_price ,
          lag_tax_rebate, 
          ln(1 + lag_tax_rebate) as ln_lag_tax_rebate, 
          lag_import_tax, 
          ln(1 + lag_import_tax) AS ln_lag_import_tax
        FROM 
          aggregate 
        INNER JOIN (SELECT DISTINCT(citycn) as citycn, cityen,geocode4_corr FROM China.city_cn_en )AS city_cn_en
        ON city_cn_en.citycn = aggregate.city_prod   
        LEFT JOIN China.country_cn_en ON country_cn_en.Country_cn = aggregate.destination
        INNER JOIN (
            SELECT 
              year, 
              HS02, 
              LAG(import_tax, 1) OVER (
                PARTITION BY HS02 
                ORDER BY 
                  HS02, 
                  year
              ) AS lag_import_tax 
            FROM 
              China.applied_MFN_Tariffs_hs02_china_2002_2010 
            WHERE 
              import_tax IS NOT NULL
          ) as import_tarrif ON import_tarrif.year = aggregate.year 
          AND import_tarrif.HS02 = aggregate.HS6
          LEFT JOIN (
            SELECT 
              HS6, 
              year, 
              tax_rebate, 
              vat_m, 
              vat_reb_m, 
              LAG(vat_m, 1) OVER (
                PARTITION BY HS6 
                ORDER BY 
                  HS6, 
                  year
              ) AS lag_vat_m, 
              LAG(vat_reb_m, 1) OVER (
                PARTITION BY HS6 
                ORDER BY 
                  HS6, 
                  year
              ) AS lag_vat_reb_m, 
              LAG(tax_rebate, 1) OVER (
                PARTITION BY HS6 
                ORDER BY 
                  HS6, 
                  year
              ) AS lag_tax_rebate, 
            FROM 
              China.base_hs6_VAT_2002_2012 
            WHERE 
              vat_m IS NOT NULL
          ) as vat 
          ON aggregate.year = vat.year 
          AND aggregate.HS6 = vat.HS6
          WHERE lag_tax_rebate IS NOT NULL AND lag_import_tax IS NOT NULL
          ORDER BY geocode4_corr, HS6, year, regime
        )
-- 7 587 818
-- 6 455 025
-- 6 455 025
-- 6 241 309
-- 5 848 017
"""


query = (
          "SELECT * "
            "FROM China.VAT_export_2002_2010 "

        )
df_final = gcp.upload_data_from_bigquery(query = query, location = 'US')
df_final.head()


df_final.shape


import sidetable
df_final.stb.freq(['year'])


df_final['year'].unique()


labels = []
date_var = 'year'


regex = r"(.*)/(.*)"
path = os.getcwd()
parent_path = Path(path).parent
test_str = str(parent_path)
matches = re.search(regex, test_str)
github_repo = matches.group(2)

path_credential = '/Users/Thomas/Google Drive/Projects/Data_science/Google_code_n_Oauth/Client_Oauth/Google_auth/'

dic_ = {
    
          'project_name' : github_repo,
          'input_datasets' : 'PROJECTNAME',
          'sheetnames' : '',
          'bigquery_dataset' : 'China',
          'destination_engine' : 'GCP',
          'path_destination_studio' : os.path.join(test_str,
                                       'Notebooks_Ready_to_use_studio'),
          'project' : 'valid-pagoda-132423',
          'username' : "thomas",
          'pathtoken' : path_credential,
          'connector' : 'GBQ', ## change to GS if spreadsheet
          'labels' : labels,
          'date_var' : date_var
}
#create_studio = studio.connector_notebook(dic_)
#create_studio.generate_notebook_studio()


Storage = 'GBQ'
Theme = 'Trade' 
Database = 'China'
Description = "The table is related to the paper that studies the effect of industrial policy in China, the VAT export tax, on the quality upgrading. We use Chinese transaction data for 2002-2006 to isolate the causal impact of the exogenous variation of VAT refund tax and within firm-product change in HS6 exported quality products."
Filename = 'VAT_export_2002_2010'
Status = 'Active'


regex = r"(.*)/(.*)"
path = os.getcwd()
parent_path = Path(path).parent
test_str = str(parent_path)
matches = re.search(regex, test_str)
github_repo = matches.group(2)
Source_data = ['tradedata_*', 'base_hs6_VAT_2002_2012', 'city_cn_en']

Profiling = True
if Profiling:
    Profiling_URL = 'http://htmlpreview.github.io/?https://github.com/' \
    'thomaspernet/{}/blob/master/Data_catalogue/table_profiling/{}.html'.format(github_repo,
                                                                               Filename)
else:
    Profiling_URL = ''
JupyterStudio = False
if JupyterStudio:
    JupyterStudio_URL = '"https://mybinder.org/v2/gh/thomaspernet/{0}/' \
    'master?filepath=Notebooks_Ready_to_use_studioget_ipython().run_line_magic("2F{1}_studio.ipynb'.format(github_repo,", " Filename)")
else:
    JupyterStudio_URL = ''
### BigQuery only 
path_url = 'https://console.cloud.google.com/bigquery?project=valid-pagoda-132423' \
'&p=valid-pagoda-132423&d=China&t={}&page=table'.format(Filename)

Link_methodology = 'https://nbviewer.jupyter.org/github/thomaspernet/' \
    '{0}/blob/master/Data_preprocessing/' \
    '{1}.ipynb'.format(github_repo,
    Filename)

Dataset_documentation = 'https://github.com/thomaspernet/{}'.format(github_repo)

to_add = {
    'Storage': Storage,
    'Theme': Theme,
    'Database': Database,
    'Path_url': path_url,
    'Filename': Filename,
    'Description': Description,
    'Source_data': Source_data,
    'Link_methodology': Link_methodology,
    'Dataset_documentation': Dataset_documentation,
    'Status': Status,
    'Profiling_URL': Profiling_URL,
    'JupyterStudio_launcher': JupyterStudio_URL

}
cols= []
for key, value in to_add.items():
    coda = {
    'column': key,
    'value':value
    }
    cols.append(coda)
    
###load token coda
with open('token_coda.json') as json_file:
    data = json.load(json_file)
    
token = data[0]['token'] 

uri = f'https://coda.io/apis/v1beta1/docs/vfMWDBnHh8/tables/grid-HgpAnIEhpP/rows'
headers = {'Authorization': 'Bearer {}'.format(token)}
payload = {
  'rows': [
    {
      'cells': cols,
    },
  ],
}
req = requests.post(uri, headers=headers, json=payload)
req.raise_for_status() # Throw if there was an error.
res = req.json()


req.raise_for_status() 
