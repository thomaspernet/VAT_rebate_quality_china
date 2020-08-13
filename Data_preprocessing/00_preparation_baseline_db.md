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

This notebook has been generated on 08/13/2020

The objective of this notebook is:

*  Create a dataset with year 2002 to 2010, including the following variables:

  * Date:
    * Date 
  * Firm id:
    * ID 
  * Export price:
    * value 
  * Quantity:
    * Quantity 
  * VAT:
    *  
  * Province
    * province 
  * City:
    *  city_prod 
  * Destination:
    * Origin_or_destination 
  * HS6:
    * HS 
  * HS4:
    * 
  * Trade status:
    * Trade_type 
  * Business type
    * Business_type 

## Global steps 

The global steps to construct the dataset are the following:

- Lorem ipsum dolor sit amet
- Lorem ipsum dolor sit amet
- Lorem ipsum dolor sit amet

## Input Cloud Storage [AWS/GCP]: If link from the internet, save it to the cloud first

The data source to construct the dataset are the following:

* BigQuery 
  *  Table: 
    * tradedata_* 
    * Notebook construction file (data lineage) 
      * md :
      * github: https://github.com/thomaspernet/Chinese-Trade-Data
  *  Table: 
    * base_hs6_VAT_2002_2012 
    * Notebook construction file (data lineage) 
      * md :
      * github:
    * Spreadsheet: 
        * base_hs6_VAT_2002_2012 
          * https://docs.google.com/spreadsheets/d/1Sl_8jh1gMKHWBRCjQDRBHyB26IQU2tIAioHdxaqdJqM/edit#gid=620969611
        * Source: 
          * Google Drive: base_hs6_VAT_2002_2012.dta
        * Notebook construction file (data lineage) 
          * md :




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
from Fast_connectCloud import connector
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_platform import connect_cloud_platform
from app_creation import studio
import pandas as pd 
import numpy as np
import pandas_profiling
from pathlib import Path
import os, re,  requests, json 
```

```python inputHidden=false outputHidden=false jupyter={"outputs_hidden": false}
gs = connector.open_connection(online_connection = False, 
	path_credential = '/PATH_TOKEN/')

service_gd = gs.connect_remote(engine = 'GS')
service_gcp = gs.connect_remote(engine = 'GCP')

gdr = connect_drive.connect_drive(service_gd['GoogleDrive'])

project = 'PROJECTNAME'
gcp = connect_cloud_platform.connect_console(project = project,
											 service_account = service_gcp['GoogleCloudP'])
```

<!-- #region heading_collapsed=true -->
## Things to know (Steps, Attention points or new flow of information)

### Steps
1.  
2. 

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

```

# Profiling

In order to get a quick summary statistic of the data, we generate an HTML file with the profiling of the dataset we've just created. 

The profiling will be available at this URL after you commit a push to GitHub. 

**You need to rename the final dataframe `df_final` in the previous section to generate the profiling.**

```python
#### make sure the final dataframe is stored as df_final
### Overide the default value: 
#https://github.com/pandas-profiling/pandas-profiling/blob/master/pandas_profiling/config_default.yaml

profile = pandas_profiling.ProfileReport(df_final,
                                        check_correlation_pearson = False)
name_html = "NAME.html"
profile.to_file(output_file=name_html)
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
date_var = ''
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
create_studio = studio.connector_notebook(dic_)
create_studio.generate_notebook_studio()
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
Description = "The table is related to"
Filename = 'PROJECTNAME'
Status = 'Active'
```

```python jupyter={"outputs_hidden": false, "source_hidden": false} nteract={"transient": {"deleting": false}}

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

```
