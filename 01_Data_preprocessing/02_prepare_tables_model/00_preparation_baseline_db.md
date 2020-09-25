---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.2
  kernel_info:
    name: python3
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Baseline dataset paper Quality data preprocessing

# Objective(s)

*  Create a dataset with year 2002 to 2010, which join the export, tax and tariff tables. 
* Add the query to the parameter_ETL.json 
* Please, update the Source URL by clicking on the button after the information have been pasted
  * US 02 create baseline tables Modify rows
  * Delete tables and Github related to the US: Delete rows
  
# Metadata

* Epic: Epic 1
* US: US 2
* Date Begin: 9/25/2020
* Duration Task: 1
* Description: Join export, tax and tariff tables
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 02 create baseline tables
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 8
* Task tag: #athena,#sql,#data-preparation
* Toggl Tag: #data-preparation

# Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

## Table/file

* Origin: 
* Athena
* Name: 
* applied_mfn_tariffs_hs02_china_2002_2010
* base_hs6_VAT_2002_2012
* import_export
* Github: * 01_tables_trade_tariffs_taxes

# Destination Output/Delivery

## Table/file

* Origin: 
* Athena
* Name:
* VAT_export_2002_2010
* GitHub:
  *  00_preparation_baseline_db
<!-- #region heading_collapsed=true -->
## Destination Output/Delivery

* Jupyter Notebook (Github Link):
  * md : [00_preparation_baseline_db.md](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/Data_preprocessing/00_preparation_baseline_db.md)
  * ipynb: [00_preparation_baseline_db.ipynb](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/Data_preprocessing/00_preparation_baseline_db.ipynb)
  
* GCP:
  * Project: valid-pagoda-132423 
  * Database: China 
  * Table: VAT_export_2002_2010 
<!-- #endregion -->

# Load Dataset


```python inputHidden=false outputHidden=false jupyter={"outputs_hidden": false}
import pandas as pd 
import numpy as np
from pathlib import Path
import os, re,  requests, json 
from GoogleDrivePy.google_authorization import authorization_service
from GoogleDrivePy.google_platform import connect_cloud_platform
```

```python inputHidden=false outputHidden=false jupyter={"outputs_hidden": false}
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
```

<!-- #region heading_collapsed=true -->
## Things to know (Steps, Attention points or new flow of information)

### Steps

1. Filter export, trade type, business type and interemediaries  
2. Append data 2003 to 2009
3. Create regime
4. Merge with VAT rebate -> Use lag
5. Keep pair year, city, HS when both regime available
6. Merge geocode
7. Remove NULL vat rebate


### Consideration’s point for the developers/analyst

* Here is the list of trade type needed: Trade_type 
  * Processing with imported materials => 进料加工贸易 : eligible to vat refund
  * Ordinary trade 一般贸易: eligible to vat refund
  * Processing with supplied materials => 来料加工装配贸易 : no eligible to vat refund
* Keep the following business type Business_type :
  * 国有企业
  * 私营企业
  * 集体企业
* Remove intermediate: intermediate 
  * keep No 
* Make sure there is at least one firm per: City-HS6-Destination
* Computation VAT rebate:
  * rebate: (vat_m-vat_reb_m)
  * ln_vat_tax = log(1+(vat_m-vat_reb_m))
<!-- #endregion -->

```python inputHidden=false outputHidden=false jupyter={"outputs_hidden": false}
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
```

```python
query = (
          "SELECT * "
            "FROM China.VAT_export_2002_2010 "

        )
df_final = gcp.upload_data_from_bigquery(query = query, location = 'US')
df_final.head()
```

```python
df_final.shape
```

```python
import sidetable
df_final.stb.freq(['year'])
```

```python
df_final['year'].unique()
```

# Upload to cloud

The dataset is ready to be shared with your colleagues. 



<!-- #region nteract={"transient": {"deleting": false}} -->
# Generate Studio

To generate a notebook ready to use in the studio, please fill in the variables below:

- 'project_name' : Name of the repository
- 'input_datasets' : name of the table
- 'sheetnames' : Name of the sheet, if table saved in Google Spreadsheet
- 'bigquery_dataset' : Dataset name
- 'destination_engine' : 'GCP' or 'GS,
- 'path_destination_studio' : path to `Notebooks_Ready_to_use_studio`
- 'project' : 'valid-pagoda-132423',
- 'username' : "thomas",
- 'pathtoken' : Path to GCP token,
- 'connector' : 'GBQ', ## change to GS if spreadsheet
- 'labels' : Add any labels to the variables,
- 'date_var' : Date variable
<!-- #endregion -->

```python jupyter={"outputs_hidden": false, "source_hidden": false} nteract={"transient": {"deleting": false}}
labels = []
date_var = 'year'
```

```python jupyter={"outputs_hidden": false, "source_hidden": false} nteract={"transient": {"deleting": false}}
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
```

<!-- #region nteract={"transient": {"deleting": false}} -->
# Add data to catalogue

Now that the dataset is ready, you need to add the underlying information to the data catalogue. The data catalogue is stored in [Coda](https://coda.io/d/MasterFile-Database_dvfMWDBnHh8/MetaDatabase_suYFO#_ludIZ), more precisely, in the table named `DataSource`. 

The cells below helps you to push the information directly to the table using Coda API.

The columns are as follow:

- `Storage`: Define the location of the table
    - GBQ, GS, MongoDB
- `Theme`: Define a theme attached to the table
    - Accountancy, Complexity, Correspondance, Customer_prediction, Distance, Environment, Finance, Macro, Production, Productivity, Survey, Trade
- `Database`: Name of the dataset. Use only for GBQ or MongoDB (collection)
    - Business, China, Steamforged, Trade
- `Path`:A URL with the path of the location of the dataset
- `Filename`: Name of the table
- `Description`: Description of the table. Be very specific. 
- `Source_data`: A list of the data sources used to construct the table.
- `Link_methodology`: URL linked to the notebook
- `Dataset_documentation`: Github repository attached to the table
- `Status`: Status of the table. 
    - `Closed` if the table won't be altered in the future
    - `Active` if the table will be altered in the future
- `Profiling`: Specify if the user created a Pandas profiling
    - `True` if the profiling has been created
    - `False` otherwise
- `Profiling_URL`: Profiling URL (link to Github). Always located in `Data_catalogue/table_profiling`
- `JupyterStudio`: Specify if the user created a notebook to open the studio
    - `True` if the notebook has been created
    - `False` otherwise
- `JupyterStudio_launcher`: Notebook URL (link to Github). Always located in `Notebooks_Ready_to_use_studio`
- `Nb_projects`: Number of projects using this dataset. A Coda formula. Do not update this row
- `Created on`: Date of creation. A Coda formula. Do not update this row

Remember to commit in GitHub to activate the URL link for the profiling and Studio
<!-- #endregion -->

```python jupyter={"outputs_hidden": false, "source_hidden": false} nteract={"transient": {"deleting": false}}
Storage = 'GBQ'
Theme = 'Trade' 
Database = 'China'
Description = "The table is related to the paper that studies the effect of industrial policy in China, the VAT export tax, on the quality upgrading. We use Chinese transaction data for 2002-2006 to isolate the causal impact of the exogenous variation of VAT refund tax and within firm-product change in HS6 exported quality products."
Filename = 'VAT_export_2002_2010'
Status = 'Active'
```

<!-- #region nteract={"transient": {"deleting": false}} -->
The next cell pushes the information to [Coda](https://coda.io/d/MasterFile-Database_dvfMWDBnHh8/Test-API_suDBp#API_tuDK4)
<!-- #endregion -->

```python jupyter={"outputs_hidden": false, "source_hidden": false} nteract={"transient": {"deleting": false}}
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
    'master?filepath=Notebooks_Ready_to_use_studio%2F{1}_studio.ipynb'.format(github_repo, Filename)
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
```

```python jupyter={"outputs_hidden": false, "source_hidden": false} nteract={"transient": {"deleting": false}}
req.raise_for_status() 
```
