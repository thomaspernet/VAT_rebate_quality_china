---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.2
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Computation quality from Khandelwal methodology

This notebook has been generated on 08/14/2020

## Objective(s)

*  Compute Kandhelwal quality for the table created in the US 1, US 1 Prepare baseline dataset. The quality is computed at the firm product-destination-level for each year in our sample. 

* Add the following Fixed effect variables:
  * Name
  * firm-product-eligibility
  * HS4-year-eligibility
  * city-year 
  * destination-year
  * Product-year

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
  *  #computation,#linear-regression,#quality
* Toggl Tag
  * #variable-computation

## Input Cloud Storage [AWS/GCP]

* BigQuery 
  * Table: VAT_export_2003_2010 
    * Notebook construction file (data lineage) 
      * md : [00_preparation_baseline_db.md](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_Data_preprocessing/00_preparation_baseline_db.md)
      
* Spreadsheet
  * Name: [Sigmas_3digit_China](https://docs.google.com/spreadsheets/d/1YLr4n2xLWKIxYftf8ODSMw6tsoiukLMxs1L5mopTDfk/edit?usp=sharing)
  * Sheet:Sigmas 
  * ID:1YLr4n2xLWKIxYftf8ODSMw6tsoiukLMxs1L5mopTDfk 
  * Notebook construction file (data lineage) 
    * From [sigma - Google Drive](https://drive.google.com/drive/folders/1KLkMm-p3_rjrHDbQcaIRlRmQwsGymoSB?usp=sharing)

## Destination Output/Delivery

* BigQuery: 
  * Project: valid-pagoda-132423 
  * Database:China 
  *  Table: quality_vat_export_2003_2010    

## Things to know (Steps, Attention points or new flow of information)

* Dynalist:
  * A cheat sheet about quality computed is available [here](https://dynalist.io/d/hnOnutKtJdI6IPjlF_IrsBQi#z=WDnMEWuP5UGr49qFyXifoEtX)
* Previous Stata [code](https://drive.google.com/file/d/1MYf5L_5D99BY9bq0fpo4_7K3jfMGllvq/view?usp=sharing)
* paper:
  * The genuine paper to compute the quality can be found [here](https://paperpile.com/shared/T5Njl6) 
  


# Load Dataset

## inputs

- Filename: Sigmas_3digit_China
- Link: https://docs.google.com/spreadsheets/d/1YLr4n2xLWKIxYftf8ODSMw6tsoiukLMxs1L5mopTDfk/edit?usp=sharing
- Type: Spreadsheet

```python
import pandas as pd 
import numpy as np
from pathlib import Path
import os, re,  requests, json 
from GoogleDrivePy.google_authorization import authorization_service
from GoogleDrivePy.google_platform import connect_cloud_platform
from GoogleDrivePy.google_drive import connect_drive
```

```python
path = os.getcwd()
parent_path = str(Path(path).parent)
project = 'valid-pagoda-132423'


auth = authorization_service.get_authorization(
    path_credential_gcp = "{}/creds/service.json".format(parent_path),
    path_credential_drive = "{}/creds".format(parent_path),
    verbose = False#
)

gcp_auth = auth.authorization_gcp()
gd_auth = auth.authorization_drive()
gcp = connect_cloud_platform.connect_console(project = project, 
                                             service_account = gcp_auth) 
drive = connect_drive.drive_operations(gd_auth)
```

```python
sigmas = drive.upload_data_from_spreadsheet(
    sheetID = '1YLr4n2xLWKIxYftf8ODSMw6tsoiukLMxs1L5mopTDfk',
    sheetName = 'Sigmas',
	 to_dataframe = True)
```

```python
sigmas.dtypes
```

- Filename: VAT_export_2003_2010
- Link: https://console.cloud.google.com/bigquery?project=valid-pagoda-132423&p=valid-pagoda-132423&d=China&t=VAT_export_2003_2010&page=table
- Type: Table

Save locally because too slow to load

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

1. Merge Sigma
2. Create additional variables:
    - sigma_price = sigma * log(unit price)
    - y = log quantity + sigma_price
    - FE_ct = country year fixed effect
3. Compute the residual
4. Compute quality:
    - Adjusted: log(unit price) - residual
    - Kandhelwal : residual /(sigma - 1)
5. Add Fixed effect
    * Name
    * firm-product-eligibility
    * HS4-year-eligibility
    * city-year 
    * destination-year
    * Product-year

<!-- #region -->
## Consideration’s point for the developers/analyst

From [Fan et al. - Trade Liberalization, Quality, and Export Prices](https://paperpile.com/app/p/98954695-6715-0f43-ac54-55de0ba1cf20)

the majority of the trade literature in defining “quality” as unobserved attributes of a variety that make consumers willing to purchase relatively large quantities of the variety despite relatively high prices charged for the variety
we estimate the “effective quality” (quality as it enters consumer’s utility) of exported product $h$ shipped to destination country $c$ by firm $f$ in year $t$,$\left( q _ { f h c t } \right) ^ { \eta }$ via the empirical demand equation:

$$x _ { f h c t } = q _ { f h c t } ^ { \eta } p _ { f h c t } ^ { - \sigma } P _ { c t } ^ { \sigma - 1 } Y _ { c t }$$


Where $x _ { f h c t }$ denotes the demand for a particular firm $f$’s product

We take logs of the empirical demand equation, and then use the residual from the following OLS regression to infer quality: 

$$\ln \left( x _ { f h c t } \right) + \sigma \ln \left( p _ { f h c t } \right) = \varphi _ { h } + \varphi _ { c t } + \epsilon _ { f h c t }$$

where the country-year fixed effect $\varphi _ { c t }$ collects both the destination price index $P_{ct}$ and income $Y_{ct}$. The product fixed effect $\varphi _ { h }$ captures the difference in prices and qualitites across product categories due to the inherent characteristics of products.

Then estimated quality is $\ln \left( \hat { q } _ { f h c t } \right) = \hat { \epsilon } _ { f h c t }$

Consequently, quality-adjusted prices are the observed log prices less estimated effective quality:

$$\ln \left(\widetilde{p}_{f h c t}\right) = \ln \left( p _ { f h c t } \right) - \ln \left( \hat { q } _ { f h c t } \right)$$ 

From Khandewal 

$$\hat{\lambda}_{f c d t} \equiv \hat{\epsilon}_{f c h t} /(\sigma-1)$$

<!-- #endregion -->

### Step 1/2 Merge and add new variables

In the first step, we merge sigma with the dataframe. There are three industries that do no match:

- 910
- 970
- 911

|    | _merge    |   Count |    Percent |   Cumulative Count |   Cumulative Percent |
|---:|:----------|--------:|-----------:|-------------------:|---------------------:|
|  0 | both      | 2406111 | 0.994417   |            2406111 |             0.994417 |
|  1 | left_only |   13509 | 0.00558311 |            2419620 |             1        |

We also compute the following variables:

- $ \text{sigma_price} = \sigma \ln \left( \text{unit_price} \right)$ 
- $y = \ln Quantity + \text{sigma_price}$
- $\text{FE_ct} = \varphi _ { c t }$

```python
df_quality = (
    df_vat.assign(
    HS3 = lambda x: x['HS6'].str[:3],
    HS4 = lambda x: x['HS6'].str[:4],
        
)
    .merge(sigmas, how = 'inner')
    .assign(
        sigma_price = lambda x: x['sigma'].astype('float') * np.log(x['unit_price']),
        y = lambda x : np.log(x['Quantity']) + x['sigma_price']
    )
)
```

```python
df_quality["FE_ct"] = pd.factorize(df_quality["year"] + 
                                   df_quality["Country_en"])[0]
```

### Step 3: compute the residual and quality

The formula is:

$$\ln \left( y _ { f h c t } \right)  = \varphi _ { h } + \varphi _ { c t } + \epsilon _ { f h c t }$$

There are two quality:

1. Price adjusted: $\ln \left( p _ { f h c t } \right) - \ln \left( \hat { q } _ { f h c t } \right)$
2. Khandelwal: $\hat{\epsilon}_{f c h t} /(\sigma-1)$

```python
#import statsmodels.api as sm
#import statsmodels.formula.api as smf
from sklearn.pipeline import make_pipeline
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import make_column_transformer
```

```python
cat_proc = make_pipeline(
    OneHotEncoder()
)
preprocessor = make_column_transformer(
    (cat_proc, tuple(['HS6', 'FE_ct']))
)
clf = make_pipeline(preprocessor,
                    LinearRegression(fit_intercept=True, normalize=False))
```

It takes about 50s to compute the weights

```python
%%time
MODEL = clf.fit(df_quality[['HS6', 'FE_ct']], df_quality['y']) 
```

```python
#pred_class = MODEL.predict(df_quality[['HS6', 'FE_ct']])
```

```python
df_quality = df_quality.assign(
    prediction = lambda x: MODEL.predict(x[['HS6', 'FE_ct']]),
    residual = lambda x: x['y'] - x['prediction'],
    price_adjusted_quality = lambda x: np.log(x['unit_price']) - x['residual'],
    kandhelwal_quality = lambda x: x['residual'] / (x['sigma'].astype('float') -1)
)    
```

Create the following fixed effect for the baseline regression:

* firm-product-regime
* HS4-year-regime
* city-year 
* destination-year
* Product-year

```python
df_quality.columns
```

```python
df_quality["FE_fpr"] = pd.factorize(df_quality["ID"].astype('str') + 
                                    df_quality["HS6"].astype('str') +
                                    df_quality["regime"].astype('str')
                                   )[0]
df_quality["FE_str"] = pd.factorize(df_quality["HS4"].astype('str') + 
                                    df_quality["year"].astype('str') +
                                    df_quality["regime"].astype('str')
                                   )[0]
df_quality["FE_ct"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
                                    df_quality["year"].astype('str')
                                   )[0]
df_quality["FE_dt"] = pd.factorize(df_quality["destination"].astype('str') + 
                                    df_quality["year"].astype('str')
                                   )[0]
df_quality["FE_pt"] = pd.factorize(df_quality["HS6"].astype('str') + 
                                    df_quality["year"].astype('str')
                                   )[0]
```

```python
pd.set_option('display.max_columns', None)
```

```python
df_quality.head()
```

```python
reindex = [
    'ID','year','regime',
    'Trade_type', 'Business_type',
    'HS6', 'HS4','HS3',
    'citycn', 'geocode4_corr', 'cityen',
    'destination', 'Country_en','ISO_alpha',
    'Quantity', 'value',  'unit_price', 
    'sigma', 'sigma_price',
    'lag_vat_m', 'lag_vat_reb_m',
    'lag_tax_rebate', 'ln_lag_tax_rebate',
    'lag_import_tax', 'ln_lag_import_tax',
    'y', 'prediction','residual',
    'price_adjusted_quality', 'kandhelwal_quality',
    'FE_ct', 'FE_fpr', 'FE_str','FE_dt', 'FE_pt']

df_quality = df_quality.reindex(columns = reindex)
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

### Dashboad Data studio

- Name: [Quality_Export_2003_2010](https://datastudio.google.com/u/0/explorer/4eb1426d-9744-4112-963a-a4bc1e52b356?config=%7B%22projectId%22:%22valid-pagoda-132423%22,%22tableId%22:%22quality_vat_export_2003_2010%22,%22datasetId%22:%22China%22,%22billingProjectId%22:%22valid-pagoda-132423%22,%22connectorType%22:%22BIG_QUERY%22,%22sqlType%22:%22STANDARD_SQL%22%7D)

![](https://drive.google.com/uc?export=view&id=1Ujj4meX_S2kZdI0WM3AU1RsHrVr8qRiW)


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
