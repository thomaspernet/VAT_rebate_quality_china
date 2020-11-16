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

*  Compute Kandhelwal quality for the table created in the Epic 1, US 2. The quality is computed at the city product-destination-level for each year in our sample.
* The notebook was already prepared. Originally, we used the data from BigQuery. 
* Please, update the Source URL by clicking on the button after the information have been pasted
  * US 02 create baseline tables Modify rows
  * Delete tables and Github related to the US: Delete rows
  
# Metadata

* Epic: Epic 1
* US: US 2
* Date Begin: 9/25/2020
* Duration Task: 1
* Description: Compute the quality of exported products using the export trade table
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: US 02 create baseline tables
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 10
* Task tag: #computation,#s3,#machine-learning
* Toggl Tag: #data-preparation

# Input Cloud Storage [AWS/GCP]

If link from the internet, save it to the cloud first

## Table/file

* Origin: 
* Athena
* Name: 
* VAT_export_2003_2010
* sigma_industry
* Github: 
    * [00_preparation_baseline_db](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/00_preparation_baseline_db.md)
    * [01_tables_trade_tariffs_taxes](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/01_prepare_tables/01_tables_trade_tariffs_taxes.md)

# Destination Output/Delivery

## Table/file

* Origin: 
* S3
* Name:
* TRADE_DATA/TRANSFORMED
* GitHub:
  *  [01_preparation_quality](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/01_preparation_quality.md)
* URL: 
* vat-rebate-quality/DATA/TRANSFORMED


# Load Dataset

## inputs

- Filename: Sigmas_3digit_China
- Link: https://docs.google.com/spreadsheets/d/1YLr4n2xLWKIxYftf8ODSMw6tsoiukLMxs1L5mopTDfk/edit?usp=sharing
- Type: Spreadsheet

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil, json

path = os.getcwd()
parent_path = str(Path(path).parent.parent)


name_credential = 'thomas_vat_credentials.csv'
region = 'eu-west-3'
bucket = 'vat-rebate-quality'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True) 
glue = service_glue.connect_glue(client = client) 
```

```python
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

# Load tables

Since we load the data as a Pandas DataFrame, we want to pass the `dtypes`. We load the schema from Glue to guess the types

## Load Sigma

```python
db = 'chinese_trade'
table = 'sigma_industry'
```

```python
dtypes = {}
schema = (glue.get_table_information(database = db,
                           table = table)
          ['Table']['StorageDescriptor']['Columns']
         )
for key, value in enumerate(schema):
    if value['Type'] in ['varchar(12)']:
        format_ = 'string'
    elif value['Type'] in ['decimal(21,5)', 'double', 'bigint']:
        format_ = 'float'
    else:
        format_ = value['Type'] 
    dtypes.update(
        {value['Name']:format_}
    )
dtypes
```

```python
query = """
SELECT * 
FROM chinese_trade.sigma_industry
"""
sigma = (s3.run_query(
    query=query,
    database=db,
    s3_output='SQL_OUTPUT_ATHENA',
    filename='sigma',  # Add filename to print dataframe
    destination_key=None,  # Add destination key if need to copy output
    dtype = dtypes
)
        )
    #.assign(
    #hs3_string=lambda x: np.where(
    #    x['hs3'].astype('string').str.len() < 3,
    #    "0" + x['hs3'].astype('string'),
    #    x['hs3'].astype('string')
    #)
#)
#)
```

```python
sigma.dtypes
```

## Load trade data

```python
table = 'VAT_export_2003_2010'
dtypes = {}
schema = (glue.get_table_information(database = db,
                           table = table)
          ['Table']['StorageDescriptor']['Columns']
         )
for key, value in enumerate(schema):
    if value['Type'] in ['varchar(12)']:
        format_ = 'string'
    elif value['Type'] in ['decimal(21,5)', 'double', 'bigint']:
        format_ = 'float'
    else:
        format_ = value['Type'] 
    dtypes.update(
        {value['Name']:format_}
    )
dtypes
```

```python
query = """
SELECT * 
FROM chinese_trade.VAT_export_2003_2010 
            """
df_vat = s3.run_query(
    query=query,
    database="chinese_trade",
    s3_output="SQL_OUTPUT_ATHENA",
    filename="trade_vat",  # Add filename to print dataframe
    destination_key=None,  # Add destination key if need to copy output
    dtype=dtypes,
)
# .assign(
# hs6_string=lambda x: np.where(
#    x['hs6'].astype('string').str.len() < 6,
#    "0" + x['hs6'].astype('string'),
#    x['hs6'].astype('string')
# )
# )
# )
```

```python
df_vat.dtypes
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
|  0 | both      | 5836151 | 0.99461    |            5836151 |              0.99461 |
|  1 | left_only |   31629 | 0.00539028 |            5867780 |              1       |

```
temp=  (
    df_vat.assign(
    hs3 = lambda x: x['hs6'].str[:3],
    hs4 = lambda x: x['hs6'].str[:4],
        
)
    .merge(sigma, how = 'left', indicator= True)
     
)
import sidetable
print((temp.stb.freq(['_merge']).to_markdown()))
```

We also compute the following variables:

- $ \text{sigma_price} = \sigma \ln \left( \text{unit_price} \right)$ 
- $y = \ln Quantity + \text{sigma_price}$
- $\text{FE_ct} = \varphi _ { c t }$

```python
df_quality = (
    df_vat.assign(
    hs3 = lambda x: x['hs6'].str[:3],
    hs4 = lambda x: x['hs6'].str[:4],
        
)
    .merge(sigma, how = 'inner')
    .assign(
        sigma_price = lambda x: x['sigma'].astype('float') * np.log(x['unit_price']),
        y = lambda x : np.log(x['quantity']) + x['sigma_price']
    )
)
```

Compute the Fixed Effect city-year, `FE_ct`

```python
df_quality["FE_ct"] = pd.factorize(df_quality["year"].astype('string') + 
                                   df_quality["country_en"])[0]
```

### Step 3: compute the residual and quality

The formula is:

$$\ln \left( y _ { f h c t } \right)  = \varphi _ { h } + \varphi _ { c t } + \epsilon _ { f h c t }$$

There are two `quality`:

1. Price adjusted: $\ln \left( p _ { f h c t } \right) - \ln \left( \hat { q } _ { f h c t } \right)$
2. Khandelwal: $\hat{\epsilon}_{f c h t} /(\sigma-1)$

```python
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
    (cat_proc, tuple(['hs6', 'FE_ct']))
)
clf = make_pipeline(preprocessor,
                    LinearRegression(fit_intercept=True, normalize=False))
```

It takes about 5/6m to compute the weights

```python
%%time
MODEL = clf.fit(df_quality[['hs6', 'FE_ct']], df_quality['y']) 
```

Save the model in S3, [vat-rebate-quality/ALGORITHM](https://s3.console.aws.amazon.com/s3/buckets/vat-rebate-quality/ALGORITHM/?region=eu-west-3&tab=overview)

```python
s3 = service_s3.connect_S3(client = client,
                      bucket = 'vat-rebate-quality', verbose = True) 
```

```python
from datetime import date
import joblib
today = date.today().strftime('%Y%m%d')
model_ols_output_path = 'OLS_MODEL.sav'

joblib.dump(MODEL, model_ols_output_path)

## save S3
destination_model_ols = "ALGORITHM/{0}/OLS/MODELS/{1}".format(today, model_ols_output_path)

s3.upload_file(
    model_ols_output_path,
    destination_model_ols
)
```

```python
df_quality = df_quality.assign(
    prediction = lambda x: MODEL.predict(x[['hs6', 'FE_ct']]),
    residual = lambda x: x['y'] - x['prediction'],
    price_adjusted_quality = lambda x: np.log(x['unit_price']) - x['residual'],
    kandhelwal_quality = lambda x: x['residual'] / (x['sigma'].astype('float') -1)
)    
```

Create the following fixed effect for the baseline regression:

**index**

* city: `c`
* product: `k`
* sector: `s`
* year: `t`
* Destination: `j`
* regime: `r`

**FE**

* city-product: `FE_ck`
* City-sector-year: `FE_cst`
* City-product-regime: `FE_ckr`
* City-sector-regime-year: `FE_csrt`
* Product-year: `FE_kt`
* Product-destination: `FE_pj`
* Destination-year: `FE_jt`

```python
### city-product
df_quality["FE_ck"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
                                    df_quality["hs6"].astype('str')
                                   )[0]

### City-sector-year
df_quality["FE_cst"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
                                    df_quality["hs4"].astype('str') +
                                    df_quality["year"].astype('str')
                                   )[0]

### City-product-regime
df_quality["FE_ckr"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
                                    df_quality["hs6"].astype('str') +
                                    df_quality["regime"].astype('str')
                                   )[0]

### City-sector-regime-year
df_quality["FE_csrt"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
                                    df_quality["hs4"].astype('str') +
                                    df_quality["regime"].astype('str') +
                                    df_quality["year"].astype('str')
                                   )[0]

## Product-year
df_quality["FE_kt"] = pd.factorize(df_quality["hs6"].astype('str') + 
                                    df_quality["year"].astype('str')
                                   )[0]

## Product-destination
df_quality["FE_kj"] = pd.factorize(df_quality["hs6"].astype('str') + 
                                    df_quality["country_en"].astype('str')
                                   )[0]

## Destination-year
df_quality["FE_jt"] = pd.factorize(df_quality["country_en"].astype('str') + 
                                    df_quality["year"].astype('str')
                                   )[0]

## city-product-destination
df_quality["FE_ckj"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
                                    df_quality["hs6"].astype('str') + 
                                    df_quality["country_en"].astype('str')
                                   )[0]

#df_quality["FE_ct"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
#                                    df_quality["year"].astype('str')
#                                   )[0]


```

```python
list(df_quality.columns)
```

```python
reindex =[
    "cityen",
    "geocode4_corr",
    "year",
    "regime",
    "hs6",
    "hs4",
    "hs3",
    "country_en",
    "iso_alpha",
    "quantity",
    "value",
    "unit_price",
    "price_adjusted_quality",
    "kandhelwal_quality",
    'lag_vat_m',
    'lag_vat_reb_m',
    'lag_tax_rebate',
    'ln_lag_vat_m',
    'ln_lag_vat_reb_m',
    'ln_lag_tax_rebate',
    'lag_import_tax',
    'ln_lag_import_tax',
    "sigma",
    "sigma_price",
    "y",
    "prediction",
    "residual",
    "FE_ct",
    "FE_ck",
    "FE_cst",
    "FE_ckr",
    "FE_csrt",
    "FE_kt",
    "FE_kj",
    "FE_jt",
    "FE_ckj"
]

df_quality = df_quality.reindex(columns=reindex)
```

```python
df_quality.head()
```

```python
df_quality.shape
```

Plot the average quality by year

![](https://drive.google.com/uc?export=view&id=1Q9KHBwEyx6tTuCU8hsy3_P46uIOm_dB9)

```python
import matplotlib.pyplot as plt
```

```python
fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(13,8))
(
    df_quality
    .groupby(['year', 'regime'])['kandhelwal_quality']
    .mean()
    .unstack(-1)
    .plot
    .line(ax=axes[0], title = 'kandhelwal_quality')
)
(
    df_quality
    .groupby(['year', 'regime'])['price_adjusted_quality']
    .mean()
    .unstack(-1)
    .plot
    .line(ax=axes[1], title = 'price_adjusted_quality')
)
```

# Upload to S3

## Output 

- Filename: quality_vat_export_2003_2010
- Link: https://s3.console.aws.amazon.com/s3/buckets/vat-rebate-quality/DATA/TRANSFORMED/?region=eu-west-3#


```python
df_quality.to_csv('quality_vat_export_2003_2010.csv', index = False)
```

```python
s3 = service_s3.connect_S3(client = client,
                      bucket = 'datalake-datascience', verbose = True) 
```

```python
s3.upload_file(
'quality_vat_export_2003_2010.csv',
    'DATA/ECON/TRADE_DATA/CHINA/TRANSFORMED/QUALITY_EXPORT_TARIFF_TAX'
)
```

```python
import shutil
os.remove('quality_vat_export_2003_2010.csv')
os.remove('OLS_MODEL.sav')
```

# Generation report

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python
def create_report(extension = "html", keep_code = False):
    """
    Create a report from the current notebook and save it in the 
    Report folder (Parent-> child directory)
    
    1. Exctract the current notbook name
    2. Convert the Notebook 
    3. Move the newly created report
    
    Args:
    extension: string. Can be "html", "pdf", "md"
    
    
    """
    
    ### Get notebook name
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split('-', 1)[0].split('.')[0]

    for srv in notebookapp.list_running_servers():
        try:
            if srv['token']=='' and not srv['password']:  
                req = urllib.request.urlopen(srv['url']+'api/sessions')
            else:
                req = urllib.request.urlopen(srv['url']+ \
                                             'api/sessions?token=' + \
                                             srv['token'])
            sessions = json.load(req)
            notebookname = sessions[0]['name']
        except:
            pass  
    
    sep = '.'
    path = os.getcwd()
    #parent_path = str(Path(path).parent)
    
    ### Path report
    #path_report = "{}/Reports".format(parent_path)
    #path_report = "{}/Reports".format(path)
    
    ### Path destination
    name_no_extension = notebookname.split(sep, 1)[0]
    source_to_move = name_no_extension +'.{}'.format(extension)
    dest = os.path.join(path,'Reports', source_to_move)
    
    ### Generate notebook
    if keep_code:
        os.system('jupyter nbconvert --to {} {}'.format(
    extension,notebookname))
    else:
        os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))
```

```python
create_report(extension = "html", keep_code= True)
```
