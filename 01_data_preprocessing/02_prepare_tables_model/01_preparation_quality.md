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
bucket = 'chinese-data'
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
|  0 | both      | 5058872 | 0.995119   |            5058872 |             0.995119 |
|  1 | left_only |   24811 | 0.00488052 |            5083683 |             1        |

```
temp=  (
    df_vat.assign(
    hs3_string = lambda x: x['hs6_string'].str[:3],
    hs4_string = lambda x: x['hs6_string'].str[:4],
        
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
    hs3_string = lambda x: x['hs6_string'].str[:3],
    hs4_string = lambda x: x['hs6_string'].str[:4],
        
)
    .merge(sigma, how = 'inner')
    .assign(
        sigma_price = lambda x: x['sigma'].astype('float') * np.log(x['unit_price']),
        y = lambda x : np.log(x['quantity']) + x['sigma_price']
    )
)
```

```python
df_quality["FE_ct"] = pd.factorize(df_quality["year"].astype('string') + 
                                   df_quality["country_en"])[0]
```

### Step 3: compute the residual and quality

The formula is:

$$\ln \left( y _ { f h c t } \right)  = \varphi _ { h } + \varphi _ { c t } + \epsilon _ { f h c t }$$

There are two quality:

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
    (cat_proc, tuple(['hs6_string', 'FE_ct']))
)
clf = make_pipeline(preprocessor,
                    LinearRegression(fit_intercept=True, normalize=False))
```

It takes about 6m to compute the weights

```python
%%time
MODEL = clf.fit(df_quality[['hs6_string', 'FE_ct']], df_quality['y']) 
```

```python
#pred_class = MODEL.predict(df_quality[['HS6', 'FE_ct']])
```

```python
df_quality = df_quality.assign(
    prediction = lambda x: MODEL.predict(x[['hs6_string', 'FE_ct']]),
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
### city-product
df_quality["FE_ck"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
                                    df_quality["hs6_string"].astype('str')
                                   )[0]

### City-sector-year
df_quality["FE_cst"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
                                    df_quality["hs4_string"].astype('str') +
                                    df_quality["year"].astype('str')
                                   )[0]

### City-product-regime
df_quality["FE_ckr"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
                                    df_quality["hs6_string"].astype('str') +
                                    df_quality["regime"].astype('str')
                                   )[0]

### City-sector-regime-year
df_quality["FE_csrt"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
                                    df_quality["hs4_string"].astype('str') +
                                    df_quality["regime"].astype('str') +
                                    df_quality["year"].astype('str')
                                   )[0]

## Product-year
df_quality["FE_kt"] = pd.factorize(df_quality["hs6_string"].astype('str') + 
                                    df_quality["year"].astype('str')
                                   )[0]

## Product-destination
df_quality["FE_pj"] = pd.factorize(df_quality["hs6_string"].astype('str') + 
                                    df_quality["country_en"].astype('str')
                                   )[0]

## Destination-year
df_quality["FE_jt"] = pd.factorize(df_quality["country_en"].astype('str') + 
                                    df_quality["year"].astype('str')
                                   )[0]

#df_quality["FE_ct"] = pd.factorize(df_quality["geocode4_corr"].astype('str') + 
#                                    df_quality["year"].astype('str')
#                                   )[0]


```

```python
pd.set_option('display.max_columns', None)
```

```python
df_quality.head()
```

```python
df_quality['kandhelwal_quality'].isna().sum()
```

```python
reindex = [
    'cityen', 'geocode4_corr', 'year', 'regime',
    'hs6_string','hs4_string','hs3_string',
    'Country_en','ISO_alpha',
    'Quantity', 'value', 'unit_price', 
    'kandhelwal_quality','price_adjusted_quality',
    'lag_tax_rebate', 'ln_lag_tax_rebate', 'lag_import_tax', 'ln_lag_import_tax', 
    'sigma', 'sigma_price', 'y', 'prediction', 'residual', 
    'FE_ck','FE_cst','FE_ckr', 'FE_csrt', 'FE_kt', 'FE_pj', 'FE_jt', 'FE_ct',
    #'FE_ct', 'FE_fpr', 'FE_str','FE_dt', 'FE_pt'
]

df_quality = df_quality.reindex(columns = reindex).rename(columns  = {
    'hs6_string' : 'HS6',
    'hs4_string' : 'HS4',
    'hs3_string' : 'HS3',
})
```

```python
df_quality.shape
```

# Upload to cloud

The dataset is ready to be shared with your colleagues. 

## Output 

- Filename: quality_vat_export_2003_2010
- Link: https://s3.console.aws.amazon.com/s3/buckets/vat-rebate-quality/DATA/TRANSFORMED/?region=eu-west-3#


```python
df_quality.to_csv('quality_vat_export_2003_2010.csv', index = False)
```

```python
s3 = service_s3.connect_S3(client = client,
                      bucket = 'vat-rebate-quality', verbose = True) 
```

```python
s3.upload_file(
'quality_vat_export_2003_2010.csv',
    'DATA/TRANSFORMED'
)
```

```python
import shutil
os.remove('quality_vat_export_2003_2010.csv')
```

### Dashboad Data studio

- Name: [Quality_Export_2003_2010](https://datastudio.google.com/u/0/explorer/4721292b-b490-49db-bcdb-2306a2b43aae?config=%7B%22projectId%22:%22valid-pagoda-132423%22,%22tableId%22:%22quality_vat_export_2003_2010%22,%22datasetId%22:%22China%22,%22billingProjectId%22:%22valid-pagoda-132423%22,%22connectorType%22:%22BIG_QUERY%22,%22sqlType%22:%22STANDARD_SQL%22%7D)

![](https://drive.google.com/uc?export=view&id=1j7J2vnv1FZaB0iCIVBUKwrWXwo0bm34T)

```python
(
    df_quality
    .groupby(['year', 'regime'])['kandhelwal_quality']
    .mean()
    #.unstack(-1)
    #.plot
    #.line()
)
```
