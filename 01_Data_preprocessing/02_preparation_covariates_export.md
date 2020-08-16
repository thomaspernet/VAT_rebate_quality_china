---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.5.0
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Add Export covariates

This notebook has been generated on 08/16/2020

## Objective(s)

*   Add covariates related to the export profil of a given city. Six covariates can be added to the model:
  * Lag Export growth by city, product, destination
  * Lag Export growth by city, product
  * Lag foreign export share by city, product, destination, regime
  * Lag foreign export share by city, product, regime
  * Lag SOE export share by city, product, destination, regime
  * Lag SOE export share by city, product, regime
* Include DataStudio (tick if yes): false

## Metadata

* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  5
* Task tag
  *  #covariates
* Toggl Tag
  * #variable-computation

## Input Cloud Storage [AWS/GCP]

1. BigQuery 
  * Table: quality_vat_export_2003_2010 
    * Notebook construction file (data lineage) 
      * md : [00_preparation_baseline_db.md](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_Data_preprocessing/00_preparation_baseline_db.md)
      * md : [01_preparation_quality.md](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_Data_preprocessing/01_preparation_quality.md)
  * Table: tradedata_* 
    * Notebook construction file (data lineage) 
      * md : 
      * github: https://github.com/thomaspernet/Chinese-Trade-Data
  * Table: city_cn_en 
    * Notebook construction file (data lineage) 
      * md : 
  * Table: country_cn_en 
    * Origin:
      * https://docs.google.com/spreadsheets/d/1QYWcLI-ybZ6l7JeWvHPNhhIurNxSM3-sDyrrLB-l2VQ/edit#gid=909268406
    * Notebook construction file (data lineage) 
      * md : 

## Destination Output/Delivery

* BigQuery: 
  * Project: valid-pagoda-132423 
  * Database:China 
  *  Table: quality_vat_export_2003_2010    

## Things to know (Steps, Attention points or new flow of information)

1. Paper 1: Trade policy repercussions: the role of local product space-Evidence from China
  * Paperpile: https://paperpile.com/shared/WCeap6
  * Paper: https://docs.google.com/file/d/1n8ociwqDVBaUKRTOmFX1mJepwIYqXlbd/edit
  * Authors: Gourdon, Julien; Hering, Laura; Monjon, Stéphanie; Poncet, Sandra 


# Load Dataset

## inputs

- Filename: VAT_export_2003_2010
- Link: https://console.cloud.google.com/bigquery?project=valid-pagoda-132423&p=valid-pagoda-132423&d=China&t=VAT_export_2003_2010&page=table
- Type: Table

Save locally because too slow to load

```python
import pandas as pd 
import numpy as np
from pathlib import Path
import os, re,  requests, json 
from GoogleDrivePy.google_authorization import authorization_service
from GoogleDrivePy.google_platform import connect_cloud_platform
```

```python
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

```python
query = (
          "SELECT * "
            "FROM China.VAT_export_2003_2010 "

        )
df_vat = gcp.upload_data_from_bigquery(query = query, location = 'US')
df_vat.head()
```

```python
#df_vat.to_csv('../00_Data_catalogue/temporary_local_data/VAT_export_2003_2010.csv', index = False)
```

# Steps

1.

<!-- #region -->
## Consideration’s point for the developers/analyst

From [Trade policy repercussions: the role of local product space-Evidence from China](https://docs.google.com/file/d/1n8ociwqDVBaUKRTOmFX1mJepwIYqXlbd/edit)

2 Need to keep this variable Business_type  to compute the share, and the business types we are interested in are
  * SOE: 国有企业 
  * Foreign: 外商独资企业 
  


### B-1 Construction and data sources of control variables

Since it is still possible that local export dynamics for a given product vary by trade
regime or city, we add a vector of control variables $X_{c,k,j, t-1}^{R}$, with coefficient vector $\lambda$. Therefore, we include the share of exports by foreign firms ($\text{Foreign export share}_{c,k,j, t-1}^{R}$) and the share of state-owned firms ($\text{State export share}_{c,k,j, t-1}^{R}$) defined at the city-product-regime level. 

These two controls are crucial to account for the time-varying ability of different localities to export
different products (under different regimes) as export performance in China varies greatly by
firm ownership (Amiti and Freund, 2010). We further include the change in city-product
export values from t-2 to t-1 ($\text{Export growth}_{c,k,j, t-1}$) to account for export dynamics at the
city and HS6 product level.

The Customs trade data is used to obtain several of our control variables: $\text{Export growth}_{k,j,t-1}$,
$\text{Export growth}_{c,k,j, t-1}$, $\text{Foreign export share}_{c,k,j, t-1}^{R}$
and $\text{State export share}_{c,k,j, t-1}^{R}$.

$\text{Export growth}_{k,j,t-1}$ and $\text{Export growth}_{c,k,j, t-1}$ are yearly export growth at the product-level
and at the city-product level respectively. These proxies of export dynamics are computed
using the mid-point growth rate formula using export values from t-2 and t-1. $\text{Foreign export share}_{c,k,j, t-1}^{R}$
and $\text{State export share}_{c,k,j, t-1}^{R}$ measure respectively the share of export quantities by foreign and state-owned firms for each product-city-regime combination.

<!-- #endregion -->

### Step 

```python
df_quality.shape
```

# Upload to cloud

The dataset is ready to be shared with your colleagues. 

## Output 

- Filename: quality_vat_export_2003_2010
- Link: https://console.cloud.google.com/bigquery?project=valid-pagoda-132423&p=valid-pagoda-132423&d=China&t=quality_vat_export_2003_2010&page=table
- Cloud Storage: 
- Type:  Table


```python
df_quality.to_csv('quality_vat_export_2003_2010.csv', index = False)
```

```python
bucket_name = 'chinese_data'
destination_blob_name = 'paper_project/Processed'
source_file_name = 'quality_vat_export_2003_2010.csv'
```

```python
gcp.delete_blob(bucket_name, 'paper_project/Processed/quality_vat_export_2003_2010.csv')
```

```python
gcp.upload_blob(bucket_name, destination_blob_name, source_file_name)
```

```python
gcp.delete_table(dataset_name = 'China', name_table = 'quality_vat_export_2003_2010')
```

```python
bucket_gcs ='chinese_data/paper_project/Processed/quality_vat_export_2003_2010.csv'
gcp.move_to_bq_autodetect(dataset_name= 'China',
							 name_table= 'quality_vat_export_2003_2010',
							 bucket_gcs=bucket_gcs)
```

```python
import shutil
shutil.move('quality_vat_export_2003_2010.csv',
            '../00_Data_catalogue/temporary_local_data/quality_vat_export_2003_2010.csv')
```

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

```python
Storage = 'GBQ'
Theme = 'Trade' 
Database = 'China'
Description = "The table is related to the paper that studies the effect of industrial policy in China, the VAT export tax, on the quality upgrading. We use Chinese transaction data for 2002-2006 to isolate the causal impact of the exogenous variation of VAT refund tax and within firm-product change in HS6 exported quality products."
Filename = 'quality_vat_export_2003_2010'
Status = 'Active'
```

The next cell pushes the information to [Coda](https://coda.io/d/MasterFile-Database_dvfMWDBnHh8/Test-API_suDBp#API_tuDK4)

```python
regex = r"(.*)/(.*)"
path = os.getcwd()
parent_path = Path(path).parent
test_str = str(parent_path)
matches = re.search(regex, test_str)
github_repo = matches.group(2)
Source_data = ['VAT_export_2002_2010', 'Sigmas_3digit_China', 'city_cn_en']

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

```python
req
```
