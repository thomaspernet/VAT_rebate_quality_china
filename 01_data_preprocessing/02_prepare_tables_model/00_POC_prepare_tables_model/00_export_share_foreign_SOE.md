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

# Poc compute export share covariates by firms ownsership

# Objective(s)

- In the context of a poc,  add covariates related to the export profil of a given city. four covariates can be added to the model:
    * Lag foreign export share by city, product, destination, regime
    * Lag foreign export share by city, product, regime 
    * Lag SOE export share by city, product, destination, regime
    * Lag SOE export share by city, product, regime

# Metadata

* Epic: Epic 1
* US: US 3
* Date Begin: 10/3/2020
* Duration Task: 0
* Description: Add foreign and SOE export shares variables 
* Step type:  
* Status: Active
* Source URL: US 03 Export share
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 8
* Task tag: #data-preparation,#athena,#sql
* Toggl Tag: #data-preparation

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* Name: 
* import_export
* Github: 
  * https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/01_prepare_tables/01_tables_trade_tariffs_taxes.md
  
# Destination Output/Delivery  
  
## Table/file
* Origin: 
* Athena
* Name:
* lag_foreign_export_ckjr
* lag_foreign_export_ckr
* lag_soe_export_ckjr
* lag_soe_export_ckr
* GitHub:
* https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/00_POC_prepare_tables_model/00_export_share_foreign_SOE.md

# Knowledge

## List of candidates

* Trade policy repercussions: the role of local product space-Evidence from China

```python inputHidden=false outputHidden=false jupyter={"outputs_hidden": false}
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil, json

path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)


name_credential = 'thomas_vat_credentials.csv'
region = 'eu-west-3'
bucket = 'chinese-data'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```python inputHidden=false outputHidden=false jupyter={"outputs_hidden": false}
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

<!-- #region -->
# Prepare query POC

This notebook is in a POC stage, which means, you will write your queries and tests if it works. Once you are satisfied by the jobs, move the queries to the ETL. Prepare a pseudo JSON file to spare time during the US linked to the ETL. Use the following format to validate the queries:

## Templare prepare table

- To create a new table using existing table (i.e Athena tables), copy the template below and paste it inside the list `TABLES.PREPARATION.ALL_SCHEMA`
    - The list `ALL_SCHEMA` accepts one or more steps. Each steps, `STEPS_X` can be a sequence of queries execution. 

```
"PREPARATION":{
   "ALL_SCHEMA":[
      {
         "STEPS_0":{
            "name":"",
            "execution":[
               {
                  "database":"",
                  "name":"",
                  "output_id":"",
                  "query":{
                     "top":"",
                     "middle":"",
                     "bottom":""
                  }
               }
            ],
            "schema":[
               {
                  "Name":"",
                  "Type":"",
                  "Comment":""
               }
            ]
         }
      }
   ],
   "template":{
      "top":"CREATE TABLE {}.{} WITH (format = 'PARQUET') AS "
   }
}
``` 

To add a step, use this template inside `TABLES.PREPARATION.ALL_SCHEMA`

```
{
   "STEPS_X":{
      "name":"",
      "execution":[
         {
            "database":"",
            "name":"",
            "output_id":"",
            "query":{
               "top":"",
               "middle":"",
               "bottom":""
            }
         }
      ],
      "schema":[
               {
                  "Name":"",
                  "Type":"",
                  "Comment":""
               }
            ]
   }
}
```

To add a query execution with a within, use the following template inside the list `STEPS_X.execution`

```
{
   "database":"",
   "name":"",
   "output_id":"",
   "query":{
      "top":"",
      "middle":"",
      "bottom":""
   }
}
``` 



Each step name should follow this format `STEPS_0`, `STEPS_1`, `STEPS_2`, etc

## Templare add comments to Glue

The AWS Glue Data Catalog contains references to data that is used as sources and targets of your extract, transform, and load (ETL) jobs in AWS Glue. To create your data warehouse or data lake, you must catalog this data. The AWS Glue Data Catalog is an index to the location, schema, and runtime metrics of your data. You use the information in the Data Catalog to create and monitor your ETL jobs. Information in the Data Catalog is stored as metadata tables, where each table specifies a single data store.

We make use of the `boto3` API to add comments in the metastore. 

- To alter the metadata (only comments), copy the template below and paste inside the list `PREPARATION.STEPS_X.schema`. 

```
[
   {
      "Name":"",
      "Type":"",
      "Comment":""
   }
]
```

The schema is related to a table, and will be modified by Glue API. **Only** variables inside the list will be modified, the remaining variables will keep default's value.

<!-- #endregion -->

# Steps

- Lag Export growth by city, product, destination
- Lag Export growth by city, product
- Lag foreign export share by city, product, destination, regime
- Lag foreign export share by city, product, regime
- Lag SOE export share by city, product, destination, regime
- Lag SOE export share by city, product, regime
- Merge all tables

From [Trade policy repercussions: the role of local product space-Evidence from China](https://docs.google.com/file/d/1n8ociwqDVBaUKRTOmFX1mJepwIYqXlbd/edit)

2 Need to keep this variable Business_type  to compute the share, and the business types we are interested in are
  * SOE: 国有企业 
  * Foreign: 外商独资企业 
  
## Construction and data sources of control variables

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


## Query Methodolology

For each query below, we store them in a dataset named `temporary` in Athena, then we will merge them with `quality_vat_export_2003_2010`.

We keep the exact same number of observations (5,836,151 in `quality_vat_export_2003_2010`, and replace `NULL`with 0.

The new tables are the following:

* `lag_foreign_export_ckjr`
* `lag_foreign_export_ckr`
* `lag_soe_export_ckjr`
* `lag_soe_export_ckr`

```python
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
db = parameters['GLOBAL']['DATABASE']
s3_output = parameters['GLOBAL']['QUERIES_OUTPUT']
```

```python
from datetime import date
today = date.today().strftime('%Y%M%d')
```

# Lag foreign export share by city, product, destination, regime

- Table name: `lag_foreign_export_ckjr`

Some countries like SER have more than one chinese name so we filter only one country amonf the duplicates. Easiest way

```python
query = """
SELECT * 
FROM chinese_lookup.country_cn_en 
WHERE iso_alpha =  'SER'
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename="duplicates", 
                )
```

```python
query = """
SELECT *
FROM "chinese_lookup"."country_cn_en" 
WHERE iso_alpha = 'FRA' OR
iso_alpha = 	'UAE'  OR 
iso_alpha = 	'ANT' OR 
iso_alpha = 	'BMU' OR 
iso_alpha = 	'MTQ' OR
iso_alpha = 	'DMA' OR
iso_alpha = 	'ABW' OR
iso_alpha = 	'GLP'
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename="duplicates", 
                )
```

```python
query = """
DROP TABLE lag_foreign_export_ckjr
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                )
```

```python
query = """
CREATE TABLE chinese_trade.lag_foreign_export_ckjr
WITH (
  format='PARQUET'
) AS
WITH filter_data AS (
  SELECT 
    date as year, 
    id, 
    trade_type, 
    business_type, 
    CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, 
    city_prod, 
    CASE 
    WHEN origin_or_destination = '阿鲁巴' OR origin_or_destination = '阿鲁巴岛' THEN '阿鲁巴岛' 
    WHEN origin_or_destination = '荷属安地列斯群岛' OR origin_or_destination = '荷属安的列斯' THEN '荷属安的列斯' 
    WHEN origin_or_destination = '百慕大' OR origin_or_destination = '百慕大群岛' THEN '百慕大群岛' 
    WHEN origin_or_destination = '多米尼克' OR origin_or_destination = '多米尼加' THEN '多米尼加' 
    WHEN origin_or_destination = '法国' OR origin_or_destination = '马约特' OR origin_or_destination = '马约特岛' THEN '法国' 
    WHEN origin_or_destination = '瓜德罗普' OR origin_or_destination = '瓜德罗普岛' THEN '瓜德罗普岛' 
    WHEN origin_or_destination = '马提尼克' OR origin_or_destination = '马提尼克岛' THEN '马提尼克岛' 
    WHEN origin_or_destination = '阿拉伯联合酋长国' OR origin_or_destination = '阿联酋' THEN '阿拉伯联合酋长国' 
    WHEN origin_or_destination = '巴哈马' OR origin_or_destination = '巴林' THEN '巴林' 
    WHEN origin_or_destination = '中国' OR origin_or_destination = '中华人民共和国' OR origin_or_destination = '台湾省' THEN '中国' 
    WHEN origin_or_destination = '南斯拉夫联盟共和国' OR origin_or_destination = '前南马其顿' OR origin_or_destination = '前南斯拉夫马其顿共和国'  THEN '南斯拉夫联盟共和国' 
    WHEN origin_or_destination = '吉尔吉斯' OR origin_or_destination = '吉尔吉斯斯坦' THEN '吉尔吉斯斯坦' 
    WHEN origin_or_destination = '黑山' OR origin_or_destination = '塞尔维亚' THEN '塞尔维亚' 
    ELSE origin_or_destination END AS destination, 
    quantities, 
    value, 
    CASE WHEN trade_type = '进料加工贸易' 
    OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime, 
    CASE WHEN business_type = '外商独资企业' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership 
  FROM 
    chinese_trade.import_export 
  WHERE 
    date in (
      '2002', '2003', '2004', '2005', '2006', 
      '2007', '2008', '2009', '2010'
    ) 
    AND imp_exp = '出口' 
    AND (
      trade_type = '进料加工贸易' 
      OR trade_type = '一般贸易' 
      OR trade_type = '来料加工装配贸易' 
      OR trade_type = '加工贸易'
    ) 
    AND intermediate = 'No' 
    AND quantities > 0 
    AND value > 0
) 
SELECT 
  * 
FROM 
  (
    WITH aggregate as (
      SELECT 
        year, 
        CAST(
          CAST(year AS INTEGER) -1 AS VARCHAR
        ) as year_lag, 
        regime, 
        foreign_ownership, 
        city_prod, 
        HS6, 
        destination, 
        SUM(quantities) as quantities 
      FROM 
        filter_data 
      GROUP BY 
        year, 
        regime, 
        foreign_ownership, 
        city_prod, 
        HS6, 
        destination
    ) 
    SELECT 
      aggregate.year, 
      aggregate.year_lag, 
      aggregate.regime, 
      aggregate.foreign_ownership, 
      geocode4_corr,
      iso_alpha,
      aggregate.HS6, 
      quantities, 
      CASE WHEN quantities_lag IS NULL THEN 0 ELSE quantities_lag END AS quantities_lag, 
      CASE WHEN total_quantities_lag IS NULL THEN 0 ELSE total_quantities_lag END AS total_quantities_lag, 
      CASE WHEN quantities_lag IS NULL 
      OR total_quantities_lag IS NULL THEN 0 ELSE CAST(
        quantities_lag AS DECIMAL(16, 5)
      )/ CAST(
        total_quantities_lag AS DECIMAL(16, 5)
      ) END AS lag_foreign_export_share_ckjr 
    FROM 
      aggregate 
      LEFT JOIN (
        SELECT 
          year, 
          regime, 
          foreign_ownership, 
          city_prod, 
          destination,
          HS6, 
          quantities as quantities_lag 
        FROM 
          aggregate
      ) as lag_quantities ON aggregate.year_lag = lag_quantities.year 
      AND aggregate.regime = lag_quantities.regime 
      AND aggregate.foreign_ownership = lag_quantities.foreign_ownership 
      AND aggregate.city_prod = lag_quantities.city_prod 
      AND aggregate.HS6 = lag_quantities.HS6 
      AND aggregate.destination = lag_quantities.destination 
      LEFT JOIN (
        SELECT 
          year, 
          regime, 
          HS6, 
          city_prod, 
          destination, 
          SUM(quantities) as total_quantities_lag 
        FROM 
          aggregate 
        GROUP BY 
          year, 
          regime, 
          HS6, 
          city_prod, 
          destination
      ) as group_lag ON aggregate.year_lag = group_lag.year 
      AND aggregate.regime = group_lag.regime 
      AND aggregate.city_prod = group_lag.city_prod 
      AND aggregate.HS6 = group_lag.HS6 
      AND aggregate.destination = group_lag.destination 
      INNER JOIN (
        SELECT 
          DISTINCT(citycn) as citycn, 
          cityen, 
          geocode4_corr 
        FROM 
          chinese_lookup.city_cn_en
      ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod 
      LEFT JOIN chinese_lookup.country_cn_en
      ON country_cn_en.Country_cn = aggregate.destination 
      WHERE aggregate.foreign_ownership = 'FOREIGN' AND iso_alpha IS NOT NULL
  )

"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                )
```

Check for duplicates

```python
query = """
SELECT CNT, COUNT(*) AS CNT_DUPLICATE
FROM (
SELECT year_lag, regime, geocode4_corr, iso_alpha, hs6, COUNT(*) as CNT
FROM "chinese_trade"."lag_foreign_export_ckjr" 
GROUP BY 
year_lag, regime, geocode4_corr, iso_alpha, hs6
  )
  GROUP BY CNT
  ORDER BY CNT_DUPLICATE
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename="duplicates", 
                )
```

# Lag foreign export share by city, product, regime

Table name: `lag_foreign_export_ckr`


```python
query = """
DROP TABLE lag_foreign_export_ckr
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                )
```

```python
query = """
CREATE TABLE chinese_trade.lag_foreign_export_ckr
WITH (
  format='PARQUET'
) AS
WITH filter_data AS (
  SELECT 
    date as year, 
    id, 
    trade_type, 
    business_type, 
    CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, 
    city_prod, 
    quantities, 
    value, 
    CASE WHEN trade_type = '进料加工贸易' 
    OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime, 
    CASE WHEN business_type = '外商独资企业' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership 
  FROM 
    chinese_trade.import_export 
  WHERE 
    date in (
      '2002', '2003', '2004', '2005', '2006', 
      '2007', '2008', '2009', '2010'
    ) 
    AND imp_exp = '出口' 
    AND (
      trade_type = '进料加工贸易' 
      OR trade_type = '一般贸易' 
      OR trade_type = '来料加工装配贸易' 
      OR trade_type = '加工贸易'
    ) 
    AND intermediate = 'No' 
    AND quantities > 0 
    AND value > 0
) 
SELECT 
  * 
FROM 
  (
    WITH aggregate as (
      SELECT 
        year, 
        CAST(
          CAST(year AS INTEGER) -1 AS VARCHAR
        ) as year_lag, 
        regime, 
        foreign_ownership, 
        city_prod, 
        HS6, 
        SUM(quantities) as quantities 
      FROM 
        filter_data 
      GROUP BY 
        year, 
        regime, 
        foreign_ownership, 
        city_prod, 
        HS6
    ) 
    SELECT 
      aggregate.year, 
      aggregate.year_lag, 
      aggregate.regime, 
      aggregate.foreign_ownership, 
      geocode4_corr,
      aggregate.HS6, 
      quantities, 
      CASE WHEN quantities_lag IS NULL THEN 0 ELSE quantities_lag END AS quantities_lag, 
      CASE WHEN total_quantities_lag IS NULL THEN 0 ELSE total_quantities_lag END AS total_quantities_lag, 
      CASE WHEN quantities_lag IS NULL 
      OR total_quantities_lag IS NULL THEN 0 ELSE CAST(
        quantities_lag AS DECIMAL(16, 5)
      )/ CAST(
        total_quantities_lag AS DECIMAL(16, 5)
      ) END AS lag_foreign_export_share_ckr 
    FROM 
      aggregate 
      LEFT JOIN (
        SELECT 
          year, 
          regime, 
          foreign_ownership, 
          city_prod, 
          HS6, 
          quantities as quantities_lag 
        FROM 
          aggregate
      ) as lag_quantities ON aggregate.year_lag = lag_quantities.year 
      AND aggregate.regime = lag_quantities.regime 
      AND aggregate.foreign_ownership = lag_quantities.foreign_ownership 
      AND aggregate.city_prod = lag_quantities.city_prod 
      AND aggregate.HS6 = lag_quantities.HS6 
      LEFT JOIN (
        SELECT 
          year, 
          regime, 
          HS6, 
          city_prod, 
          SUM(quantities) as total_quantities_lag 
        FROM 
          aggregate 
        GROUP BY 
          year, 
          regime, 
          HS6, 
          city_prod 
      ) as group_lag ON aggregate.year_lag = group_lag.year 
      AND aggregate.regime = group_lag.regime 
      AND aggregate.city_prod = group_lag.city_prod 
      AND aggregate.HS6 = group_lag.HS6
      INNER JOIN (
        SELECT 
          DISTINCT(citycn) as citycn, 
          cityen, 
          geocode4_corr 
        FROM 
          chinese_lookup.city_cn_en
      ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod 
      WHERE aggregate.foreign_ownership = 'FOREIGN' 
  )

"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                )
```

```python
query = """
SELECT CNT, COUNT(*) AS CNT_DUPLICATE
FROM (
SELECT year_lag, regime, geocode4_corr, hs6, COUNT(*) as CNT
FROM "chinese_trade"."lag_foreign_export_ckr" 
GROUP BY 
year_lag, regime, geocode4_corr, hs6
  )
  GROUP BY CNT
  ORDER BY CNT_DUPLICATE
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename="duplicates", 
                )
```

# Lag SOE export share by city, product, destination, regime

- Table name: `lag_soe_export_ckjr`

```python
query = """
DROP TABLE lag_soe_export_ckjr
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                )
```

```python
query = """
CREATE TABLE chinese_trade.lag_soe_export_ckjr
WITH (
  format='PARQUET'
) AS
WITH filter_data AS (
  SELECT 
    date as year, 
    id, 
    trade_type, 
    business_type, 
    CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, 
    city_prod, 
    CASE 
    WHEN origin_or_destination = '阿鲁巴' OR origin_or_destination = '阿鲁巴岛' THEN '阿鲁巴岛' 
    WHEN origin_or_destination = '荷属安地列斯群岛' OR origin_or_destination = '荷属安的列斯' THEN '荷属安的列斯' 
    WHEN origin_or_destination = '百慕大' OR origin_or_destination = '百慕大群岛' THEN '百慕大群岛' 
    WHEN origin_or_destination = '多米尼克' OR origin_or_destination = '多米尼加' THEN '多米尼加' 
    WHEN origin_or_destination = '法国' OR origin_or_destination = '马约特' OR origin_or_destination = '马约特岛' THEN '法国' 
    WHEN origin_or_destination = '瓜德罗普' OR origin_or_destination = '瓜德罗普岛' THEN '瓜德罗普岛' 
    WHEN origin_or_destination = '马提尼克' OR origin_or_destination = '马提尼克岛' THEN '马提尼克岛' 
    WHEN origin_or_destination = '阿拉伯联合酋长国' OR origin_or_destination = '阿联酋' THEN '阿拉伯联合酋长国' 
    WHEN origin_or_destination = '巴哈马' OR origin_or_destination = '巴林' THEN '巴林' 
    WHEN origin_or_destination = '中国' OR origin_or_destination = '中华人民共和国' OR origin_or_destination = '台湾省' THEN '中国' 
    WHEN origin_or_destination = '南斯拉夫联盟共和国' OR origin_or_destination = '前南马其顿' OR origin_or_destination = '前南斯拉夫马其顿共和国'  THEN '南斯拉夫联盟共和国' 
    WHEN origin_or_destination = '吉尔吉斯' OR origin_or_destination = '吉尔吉斯斯坦' THEN '吉尔吉斯斯坦' 
    WHEN origin_or_destination = '黑山' OR origin_or_destination = '塞尔维亚' THEN '塞尔维亚' 
    ELSE origin_or_destination END AS destination,
    quantities, 
    value, 
    CASE WHEN trade_type = '进料加工贸易' 
    OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime, 
    CASE WHEN Business_type = '国有企业' OR Business_type = '国有' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership
  FROM 
    chinese_trade.import_export 
  WHERE 
    date in (
      '2002', '2003', '2004', '2005', '2006', 
      '2007', '2008', '2009', '2010'
    ) 
    AND imp_exp = '出口' 
    AND (
      trade_type = '进料加工贸易' 
      OR trade_type = '一般贸易' 
      OR trade_type = '来料加工装配贸易' 
      OR trade_type = '加工贸易'
    ) 
    AND intermediate = 'No' 
    AND quantities > 0 
    AND value > 0
) 
SELECT 
  * 
FROM 
  (
    WITH aggregate as (
      SELECT 
        year, 
        CAST(
          CAST(year AS INTEGER) -1 AS VARCHAR
        ) as year_lag, 
        regime, 
        SOE_ownership, 
        city_prod, 
        HS6, 
        destination, 
        SUM(quantities) as quantities 
      FROM 
        filter_data 
      GROUP BY 
        year, 
        regime, 
        SOE_ownership, 
        city_prod, 
        HS6, 
        destination
    ) 
    SELECT 
      aggregate.year, 
      aggregate.year_lag, 
      aggregate.regime, 
      aggregate.SOE_ownership, 
      geocode4_corr,
      iso_alpha,
      aggregate.HS6, 
      quantities, 
      CASE WHEN quantities_lag IS NULL THEN 0 ELSE quantities_lag END AS quantities_lag, 
      CASE WHEN total_quantities_lag IS NULL THEN 0 ELSE total_quantities_lag END AS total_quantities_lag, 
      CASE WHEN quantities_lag IS NULL 
      OR total_quantities_lag IS NULL THEN 0 ELSE CAST(
        quantities_lag AS DECIMAL(16, 5)
      )/ CAST(
        total_quantities_lag AS DECIMAL(16, 5)
      ) END AS lag_soe_export_share_ckjr 
    FROM 
      aggregate 
      LEFT JOIN (
        SELECT 
          year, 
          regime, 
          SOE_ownership, 
          city_prod, 
          destination,
          HS6, 
          quantities as quantities_lag 
        FROM 
          aggregate
      ) as lag_quantities ON aggregate.year_lag = lag_quantities.year 
      AND aggregate.regime = lag_quantities.regime 
      AND aggregate.SOE_ownership = lag_quantities.SOE_ownership 
      AND aggregate.city_prod = lag_quantities.city_prod 
      AND aggregate.HS6 = lag_quantities.HS6 
      AND aggregate.destination = lag_quantities.destination 
      LEFT JOIN (
        SELECT 
          year, 
          regime, 
          HS6, 
          city_prod, 
          destination, 
          SUM(quantities) as total_quantities_lag 
        FROM 
          aggregate 
        GROUP BY 
          year, 
          regime, 
          HS6, 
          city_prod, 
          destination
      ) as group_lag ON aggregate.year_lag = group_lag.year 
      AND aggregate.regime = group_lag.regime 
      AND aggregate.city_prod = group_lag.city_prod 
      AND aggregate.HS6 = group_lag.HS6 
      AND aggregate.destination = group_lag.destination 
      INNER JOIN (
        SELECT 
          DISTINCT(citycn) as citycn, 
          cityen, 
          geocode4_corr 
        FROM 
          chinese_lookup.city_cn_en
      ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod 
      LEFT JOIN chinese_lookup.country_cn_en ON country_cn_en.Country_cn = aggregate.destination 
      WHERE aggregate.SOE_ownership = 'SOE' AND iso_alpha IS NOT NULL
  )

"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                )
```

```python
query = """
SELECT CNT, COUNT(*) AS CNT_DUPLICATE
FROM (
SELECT year_lag, regime, geocode4_corr, iso_alpha, hs6, COUNT(*) as CNT
FROM "chinese_trade"."lag_soe_export_ckjr" 
GROUP BY 
year_lag, regime, geocode4_corr, iso_alpha, hs6
  )
  GROUP BY CNT
  ORDER BY CNT_DUPLICATE
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename="duplicates", 
                )
```

# Lag SOE export share by city, product, regime

- Table name: `lag_soe_export_ckr`

```python
query = """
DROP TABLE lag_soe_export_ckr
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                )
```

```python
query = """
CREATE TABLE chinese_trade.lag_soe_export_ckr
WITH (
  format='PARQUET'
) AS
WITH filter_data AS (
  SELECT 
    date as year, 
    id, 
    trade_type, 
    business_type, 
    CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, 
    city_prod,  
    quantities, 
    value, 
    CASE WHEN trade_type = '进料加工贸易' 
    OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime, 
    CASE WHEN Business_type = '国有企业' OR Business_type = '国有' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership
  FROM 
    chinese_trade.import_export 
  WHERE 
    date in (
      '2002', '2003', '2004', '2005', '2006', 
      '2007', '2008', '2009', '2010'
    ) 
    AND imp_exp = '出口' 
    AND (
      trade_type = '进料加工贸易' 
      OR trade_type = '一般贸易' 
      OR trade_type = '来料加工装配贸易' 
      OR trade_type = '加工贸易'
    ) 
    AND intermediate = 'No' 
    AND quantities > 0 
    AND value > 0
) 
SELECT 
  * 
FROM 
  (
    WITH aggregate as (
      SELECT 
        year, 
        CAST(
          CAST(year AS INTEGER) -1 AS VARCHAR
        ) as year_lag, 
        regime, 
        SOE_ownership, 
        city_prod, 
        HS6, 
        SUM(quantities) as quantities 
      FROM 
        filter_data 
      GROUP BY 
        year, 
        regime, 
        SOE_ownership, 
        city_prod, 
        HS6
    ) 
    SELECT 
      aggregate.year, 
      aggregate.year_lag, 
      aggregate.regime, 
      aggregate.SOE_ownership, 
      geocode4_corr,
      aggregate.HS6, 
      quantities, 
      CASE WHEN quantities_lag IS NULL THEN 0 ELSE quantities_lag END AS quantities_lag, 
      CASE WHEN total_quantities_lag IS NULL THEN 0 ELSE total_quantities_lag END AS total_quantities_lag, 
      CASE WHEN quantities_lag IS NULL 
      OR total_quantities_lag IS NULL THEN 0 ELSE CAST(
        quantities_lag AS DECIMAL(16, 5)
      )/ CAST(
        total_quantities_lag AS DECIMAL(16, 5)
      ) END AS lag_soe_export_share_ckr
    FROM 
      aggregate 
      LEFT JOIN (
        SELECT 
          year, 
          regime, 
          SOE_ownership, 
          city_prod, 
          HS6, 
          quantities as quantities_lag 
        FROM 
          aggregate
      ) as lag_quantities ON aggregate.year_lag = lag_quantities.year 
      AND aggregate.regime = lag_quantities.regime 
      AND aggregate.SOE_ownership = lag_quantities.SOE_ownership 
      AND aggregate.city_prod = lag_quantities.city_prod 
      AND aggregate.HS6 = lag_quantities.HS6 
      LEFT JOIN (
        SELECT 
          year, 
          regime, 
          HS6, 
          city_prod, 
          SUM(quantities) as total_quantities_lag 
        FROM 
          aggregate 
        GROUP BY 
          year, 
          regime, 
          HS6, 
          city_prod 
      ) as group_lag ON aggregate.year_lag = group_lag.year 
      AND aggregate.regime = group_lag.regime 
      AND aggregate.city_prod = group_lag.city_prod 
      AND aggregate.HS6 = group_lag.HS6
      INNER JOIN (
        SELECT 
          DISTINCT(citycn) as citycn, 
          cityen, 
          geocode4_corr 
        FROM 
          chinese_lookup.city_cn_en
      ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod 
      WHERE aggregate.SOE_ownership = 'SOE'
  )

"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                )
```

```python
query = """
SELECT CNT, COUNT(*) AS CNT_DUPLICATE
FROM (
SELECT year_lag, regime, geocode4_corr, hs6, COUNT(*) as CNT
FROM "chinese_trade"."lag_soe_export_ckr" 
GROUP BY 
year_lag, regime, geocode4_corr, hs6
  )
  GROUP BY CNT
  ORDER BY CNT_DUPLICATE
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename="duplicates", 
                )
```

# Analysis

In this part, we are providing basic summary statistic. Since we have created the tables, we can parse the schema in Glue and use our json file to automatically generates the analysis.

The cells below execute the job in the key `ANALYSIS`. You need to change the `primary_key` and `secondary_key` 


## `lag_foreign_export_ckjr`

```python
table = 'lag_foreign_export_ckjr'
schema = glue.get_table_information(
    database = db,
    table = table
)['Table']
schema
```

## Count missing values

```python
table_top = parameters["ANALYSIS"]["COUNT_MISSING"]["top"]
table_middle = ""
table_bottom = parameters["ANALYSIS"]["COUNT_MISSING"]["bottom"].format(
    db, table
)

for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if key == len(schema["StorageDescriptor"]["Columns"]) - 1:

        table_middle += "{} ".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
    else:
        table_middle += "{} ,".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
query = table_top + table_middle + table_bottom
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_missing",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
display(
    output.T.rename(columns={0: "total_missing"})
    .assign(total_missing_pct=lambda x: x["total_missing"] / x.iloc[0, 0])
    .sort_values(by=["total_missing"], ascending=False)
    .style.format("{0:,.2%}", subset=["total_missing_pct"])
    .bar(subset="total_missing_pct", color=["#d65f5f"])
)
```

## Categorical Description

During the categorical analysis, we wil count the number of observations for a given group and for a pair.

### Count obs by group

- Index: primary group
- nb_obs: Number of observations per primary group value
- percentage: Percentage of observation per primary group value over the total number of observations

Returns the top 10 only

```python
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(10)","varchar(12)"]:

        print("Nb of obs for {}".format(field["Name"]))

        query = parameters["ANALYSIS"]["CATEGORICAL"]["PAIR"].format(
            db, table, field["Name"]
        )
        output = s3.run_query(
            query=query,
            database=db,
            s3_output=s3_output,
            filename="count_categorical_{}".format(
                field["Name"]
            ),  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )

        ### Print top 10

        display(
            (
                output.set_index([field["Name"]])
                .assign(percentage=lambda x: x["nb_obs"] / x["nb_obs"].sum())
                .sort_values("percentage", ascending=False)
                .head(10)
                .style.format("{0:.2%}", subset=["percentage"])
                .bar(subset=["percentage"], color="#d65f5f")
            )
        )
```

### Count obs by two pair

You need to pass the primary group in the cell below

- Index: primary group
- Columns: Secondary key -> All the categorical variables in the dataset
- nb_obs: Number of observations per primary group value
- Total: Total number of observations per primary group value (sum by row)
- percentage: Percentage of observations per primary group value over the total number of observations per primary group value (sum by row)

Returns the top 10 only

```python
primary_key = "year"
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(10)","varchar(12)"]:
        if field["Name"] != primary_key:
            print(
                "Nb of obs for the primary group {} and {}".format(
                    primary_key, field["Name"]
                )
            )
            query = parameters["ANALYSIS"]["CATEGORICAL"]["MULTI_PAIR"].format(
                db, table, primary_key, field["Name"]
            )

            output = s3.run_query(
                query=query,
                database=db,
                s3_output=s3_output,
                filename="count_categorical_{}_{}".format(
                    primary_key, field["Name"]
                ),  # Add filename to print dataframe
                destination_key=None,  # Add destination key if need to copy output
            )

            display(
                (
                    pd.concat(
                        [
                            (
                                output.loc[
                                    lambda x: x[field["Name"]].isin(
                                        (
                                            output.assign(
                                                total_secondary=lambda x: x["nb_obs"]
                                                .groupby([x[field["Name"]]])
                                                .transform("sum")
                                            )
                                            .drop_duplicates(
                                                subset="total_secondary", keep="last"
                                            )
                                            .sort_values(
                                                by=["total_secondary"], ascending=False
                                            )
                                            .iloc[:10, 1]
                                            .to_list()
                                        )
                                    )
                                ]
                                .set_index([primary_key, field["Name"]])
                                .unstack([0])
                                .fillna(0)
                                .assign(total=lambda x: x.sum(axis=1))
                                .sort_values(by=["total"])
                            ),
                            (
                                output.loc[
                                    lambda x: x[field["Name"]].isin(
                                        (
                                            output.assign(
                                                total_secondary=lambda x: x["nb_obs"]
                                                .groupby([x[field["Name"]]])
                                                .transform("sum")
                                            )
                                            .drop_duplicates(
                                                subset="total_secondary", keep="last"
                                            )
                                            .sort_values(
                                                by=["total_secondary"], ascending=False
                                            )
                                            .iloc[:10, 1]
                                            .to_list()
                                        )
                                    )
                                ]
                                .rename(columns={"nb_obs": "percentage"})
                                .set_index([primary_key, field["Name"]])
                                .unstack([0])
                                .fillna(0)
                                .apply(lambda x: x / x.sum(), axis=1)
                            ),
                        ],
                        axis=1,
                    )
                    .fillna(0)
                    # .sort_index(axis=1, level=1)
                    .style.format("{0:,.2f}", subset=["nb_obs", "total"])
                    .bar(subset=["total"], color="#d65f5f")
                    .format("{0:,.2%}", subset=("percentage"))
                    .background_gradient(
                        cmap=sns.light_palette("green", as_cmap=True), subset=("nb_obs")
                    )
                )
            )
```

<!-- #region -->
## Continuous description

There are three possibilities to show the ditribution of a continuous variables:

1. Display the percentile
2. Display the percentile, with one primary key
3. Display the percentile, with one primary key, and a secondary key


### 1. Display the percentile

- pct: Percentile [.25, .50, .75, .95, .90]
<!-- #endregion -->

```python
table_top = ""
table_top_var = ""
table_middle = ""
table_bottom = ""

var_index = 0
size_continuous = len([len(x) for x in schema["StorageDescriptor"]["Columns"] if 
                       x['Type'] in ["float", "double", "bigint", "decimal(21,5)"]])
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"][
                "bottom"
            ].format(db, table, value["Name"], key)
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["middle_1"],
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["bottom"].format(
                    db, table, value["Name"], key
                ),
            )
            temp_middle_2 = parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"][
                "middle_2"
            ].format(value["Name"])

            if cont == size_continuous:

                table_top_var += "{} {}".format(
                    value["Name"],
                    parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_3"],
                )
                table_bottom += "{} {})".format(temp_middle_1, temp_middle_2)
            else:
                table_top_var += "{} ,".format(value["Name"])
                table_bottom += "{} {}".format(temp_middle_1, temp_middle_2)
        var_index += 1

query = (
    parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_1"]
    + table_top
    + parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_2"]
    + table_top_var
    + table_bottom
)
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_distribution",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
(output.sort_values(by="pct").set_index(["pct"]).style.format("{0:.2f}"))
```

### 2. Display the percentile, with one primary key

The primary key will be passed to all the continuous variables

- index: 
    - Primary group
    - Percentile [.25, .50, .75, .95, .90] per primary group value
- Columns: Secondary group
- Heatmap is colored based on the row, ie darker blue indicates larger values for a given row

```python
primary_key = "year"
table_top = ""
table_top_var = ""
table_middle = ""
table_bottom = ""
var_index = 0
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                "bottom"
            ].format(
                db, table, value["Name"], key, primary_key
            )
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "middle_1"
                ],
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "bottom"
                ].format(
                    db, table, value["Name"], key, primary_key
                ),
            )
            temp_middle_2 = parameters["ANALYSIS"]["CONTINUOUS"][
                "ONE_PAIR_DISTRIBUTION"
            ]["middle_2"].format(value["Name"], primary_key)

            if cont == size_continuous:

                table_top_var += "{} {}".format(
                    value["Name"],
                    parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                        "top_3"
                    ],
                )
                table_bottom += "{} {})".format(temp_middle_1, temp_middle_2)
            else:
                table_top_var += "{} ,".format(value["Name"])
                table_bottom += "{} {}".format(temp_middle_1, temp_middle_2)
        var_index += 1

query = (
    parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"]["top_1"]
    + table_top
    + parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"]["top_2"].format(
        primary_key
    )
    + table_top_var
    + table_bottom
)
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_distribution_primary_key",  # Add filename to print dataframe
    destination_key=None,  # Add destination key if need to copy output
)
(
    output.set_index([primary_key, "pct"])
    .unstack(1)
    .T.style.format("{0:,.2f}")
    .background_gradient(cmap=sns.light_palette("blue", as_cmap=True), axis=1)
)
```

### 3. Display the percentile, with one primary key, and a secondary key

The primary and secondary key will be passed to all the continuous variables. The output might be too big so we print only the top 10 for the secondary key

- index:  Primary group
- Columns: 
    - Secondary group
    - Percentile [.25, .50, .75, .95, .90] per secondary group value
- Heatmap is colored based on the column, ie darker green indicates larger values for a given column

```python
primary_key = 'year'
secondary_key = 'regime'
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:

        query = parameters["ANALYSIS"]["CONTINUOUS"]["TWO_PAIRS_DISTRIBUTION"].format(
            db,
            table,
            primary_key,
            secondary_key,
            value["Name"],
        )

        output = s3.run_query(
            query=query,
            database=db,
            s3_output=s3_output,
            filename="count_distribution_{}_{}_{}".format(
                primary_key, secondary_key, value["Name"]
            ),  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )

        print(
            "Distribution of {}, by {} and {}".format(
                value["Name"], primary_key, secondary_key,
            )
        )

        display(
            (
                output.loc[
                    lambda x: x[secondary_key].isin(
                        (
                            output.assign(
                                total_secondary=lambda x: x[value["Name"]]
                                .groupby([x[secondary_key]])
                                .transform("sum")
                            )
                            .drop_duplicates(subset="total_secondary", keep="last")
                            .sort_values(by=["total_secondary"], ascending=False)
                            .iloc[:10, 1]
                        ).to_list()
                    )
                ]
                .set_index([primary_key, "pct", secondary_key])
                .unstack([0, 1])
                .fillna(0)
                .sort_index(axis=1, level=[1, 2])
                .style.format("{0:,.2f}")
                .background_gradient(cmap=sns.light_palette("green", as_cmap=True))
            )
        )
```

## `lag_foreign_export_ckr`

```python
table = 'lag_foreign_export_ckr'
schema = glue.get_table_information(
    database = db,
    table = table
)['Table']
schema
```

## Count missing values

```python
table_top = parameters["ANALYSIS"]["COUNT_MISSING"]["top"]
table_middle = ""
table_bottom = parameters["ANALYSIS"]["COUNT_MISSING"]["bottom"].format(
    db, table
)

for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if key == len(schema["StorageDescriptor"]["Columns"]) - 1:

        table_middle += "{} ".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
    else:
        table_middle += "{} ,".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
query = table_top + table_middle + table_bottom
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_missing",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
display(
    output.T.rename(columns={0: "total_missing"})
    .assign(total_missing_pct=lambda x: x["total_missing"] / x.iloc[0, 0])
    .sort_values(by=["total_missing"], ascending=False)
    .style.format("{0:,.2%}", subset=["total_missing_pct"])
    .bar(subset="total_missing_pct", color=["#d65f5f"])
)
```

## Categorical Description

During the categorical analysis, we wil count the number of observations for a given group and for a pair.

### Count obs by group

- Index: primary group
- nb_obs: Number of observations per primary group value
- percentage: Percentage of observation per primary group value over the total number of observations

Returns the top 10 only

```python
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(10)","varchar(12)"]:

        print("Nb of obs for {}".format(field["Name"]))

        query = parameters["ANALYSIS"]["CATEGORICAL"]["PAIR"].format(
            db, table, field["Name"]
        )
        output = s3.run_query(
            query=query,
            database=db,
            s3_output=s3_output,
            filename="count_categorical_{}".format(
                field["Name"]
            ),  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )

        ### Print top 10

        display(
            (
                output.set_index([field["Name"]])
                .assign(percentage=lambda x: x["nb_obs"] / x["nb_obs"].sum())
                .sort_values("percentage", ascending=False)
                .head(10)
                .style.format("{0:.2%}", subset=["percentage"])
                .bar(subset=["percentage"], color="#d65f5f")
            )
        )
```

### Count obs by two pair

You need to pass the primary group in the cell below

- Index: primary group
- Columns: Secondary key -> All the categorical variables in the dataset
- nb_obs: Number of observations per primary group value
- Total: Total number of observations per primary group value (sum by row)
- percentage: Percentage of observations per primary group value over the total number of observations per primary group value (sum by row)

Returns the top 10 only

```python
primary_key = "year"
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(10)","varchar(12)"]:
        if field["Name"] != primary_key:
            print(
                "Nb of obs for the primary group {} and {}".format(
                    primary_key, field["Name"]
                )
            )
            query = parameters["ANALYSIS"]["CATEGORICAL"]["MULTI_PAIR"].format(
                db, table, primary_key, field["Name"]
            )

            output = s3.run_query(
                query=query,
                database=db,
                s3_output=s3_output,
                filename="count_categorical_{}_{}".format(
                    primary_key, field["Name"]
                ),  # Add filename to print dataframe
                destination_key=None,  # Add destination key if need to copy output
            )

            display(
                (
                    pd.concat(
                        [
                            (
                                output.loc[
                                    lambda x: x[field["Name"]].isin(
                                        (
                                            output.assign(
                                                total_secondary=lambda x: x["nb_obs"]
                                                .groupby([x[field["Name"]]])
                                                .transform("sum")
                                            )
                                            .drop_duplicates(
                                                subset="total_secondary", keep="last"
                                            )
                                            .sort_values(
                                                by=["total_secondary"], ascending=False
                                            )
                                            .iloc[:10, 1]
                                            .to_list()
                                        )
                                    )
                                ]
                                .set_index([primary_key, field["Name"]])
                                .unstack([0])
                                .fillna(0)
                                .assign(total=lambda x: x.sum(axis=1))
                                .sort_values(by=["total"])
                            ),
                            (
                                output.loc[
                                    lambda x: x[field["Name"]].isin(
                                        (
                                            output.assign(
                                                total_secondary=lambda x: x["nb_obs"]
                                                .groupby([x[field["Name"]]])
                                                .transform("sum")
                                            )
                                            .drop_duplicates(
                                                subset="total_secondary", keep="last"
                                            )
                                            .sort_values(
                                                by=["total_secondary"], ascending=False
                                            )
                                            .iloc[:10, 1]
                                            .to_list()
                                        )
                                    )
                                ]
                                .rename(columns={"nb_obs": "percentage"})
                                .set_index([primary_key, field["Name"]])
                                .unstack([0])
                                .fillna(0)
                                .apply(lambda x: x / x.sum(), axis=1)
                            ),
                        ],
                        axis=1,
                    )
                    .fillna(0)
                    # .sort_index(axis=1, level=1)
                    .style.format("{0:,.2f}", subset=["nb_obs", "total"])
                    .bar(subset=["total"], color="#d65f5f")
                    .format("{0:,.2%}", subset=("percentage"))
                    .background_gradient(
                        cmap=sns.light_palette("green", as_cmap=True), subset=("nb_obs")
                    )
                )
            )
```

<!-- #region -->
## Continuous description

There are three possibilities to show the ditribution of a continuous variables:

1. Display the percentile
2. Display the percentile, with one primary key
3. Display the percentile, with one primary key, and a secondary key


### 1. Display the percentile

- pct: Percentile [.25, .50, .75, .95, .90]
<!-- #endregion -->

```python
table_top = ""
table_top_var = ""
table_middle = ""
table_bottom = ""

var_index = 0
size_continuous = len([len(x) for x in schema["StorageDescriptor"]["Columns"] if 
                       x['Type'] in ["float", "double", "bigint", "decimal(21,5)"]])
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"][
                "bottom"
            ].format(db, table, value["Name"], key)
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["middle_1"],
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["bottom"].format(
                    db, table, value["Name"], key
                ),
            )
            temp_middle_2 = parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"][
                "middle_2"
            ].format(value["Name"])

            if cont == size_continuous:

                table_top_var += "{} {}".format(
                    value["Name"],
                    parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_3"],
                )
                table_bottom += "{} {})".format(temp_middle_1, temp_middle_2)
            else:
                table_top_var += "{} ,".format(value["Name"])
                table_bottom += "{} {}".format(temp_middle_1, temp_middle_2)
        var_index += 1

query = (
    parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_1"]
    + table_top
    + parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_2"]
    + table_top_var
    + table_bottom
)
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_distribution",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
(output.sort_values(by="pct").set_index(["pct"]).style.format("{0:.2f}"))
```

### 2. Display the percentile, with one primary key

The primary key will be passed to all the continuous variables

- index: 
    - Primary group
    - Percentile [.25, .50, .75, .95, .90] per primary group value
- Columns: Secondary group
- Heatmap is colored based on the row, ie darker blue indicates larger values for a given row

```python
primary_key = "year"
table_top = ""
table_top_var = ""
table_middle = ""
table_bottom = ""
var_index = 0
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                "bottom"
            ].format(
                db, table, value["Name"], key, primary_key
            )
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "middle_1"
                ],
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "bottom"
                ].format(
                    db, table, value["Name"], key, primary_key
                ),
            )
            temp_middle_2 = parameters["ANALYSIS"]["CONTINUOUS"][
                "ONE_PAIR_DISTRIBUTION"
            ]["middle_2"].format(value["Name"], primary_key)

            if cont == size_continuous:

                table_top_var += "{} {}".format(
                    value["Name"],
                    parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                        "top_3"
                    ],
                )
                table_bottom += "{} {})".format(temp_middle_1, temp_middle_2)
            else:
                table_top_var += "{} ,".format(value["Name"])
                table_bottom += "{} {}".format(temp_middle_1, temp_middle_2)
        var_index += 1

query = (
    parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"]["top_1"]
    + table_top
    + parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"]["top_2"].format(
        primary_key
    )
    + table_top_var
    + table_bottom
)
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_distribution_primary_key",  # Add filename to print dataframe
    destination_key=None,  # Add destination key if need to copy output
)
(
    output.set_index([primary_key, "pct"])
    .unstack(1)
    .T.style.format("{0:,.2f}")
    .background_gradient(cmap=sns.light_palette("blue", as_cmap=True), axis=1)
)
```

### 3. Display the percentile, with one primary key, and a secondary key

The primary and secondary key will be passed to all the continuous variables. The output might be too big so we print only the top 10 for the secondary key

- index:  Primary group
- Columns: 
    - Secondary group
    - Percentile [.25, .50, .75, .95, .90] per secondary group value
- Heatmap is colored based on the column, ie darker green indicates larger values for a given column

```python
primary_key = 'year'
secondary_key = 'regime'
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:

        query = parameters["ANALYSIS"]["CONTINUOUS"]["TWO_PAIRS_DISTRIBUTION"].format(
            db,
            table,
            primary_key,
            secondary_key,
            value["Name"],
        )

        output = s3.run_query(
            query=query,
            database=db,
            s3_output=s3_output,
            filename="count_distribution_{}_{}_{}".format(
                primary_key, secondary_key, value["Name"]
            ),  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )

        print(
            "Distribution of {}, by {} and {}".format(
                value["Name"], primary_key, secondary_key,
            )
        )

        display(
            (
                output.loc[
                    lambda x: x[secondary_key].isin(
                        (
                            output.assign(
                                total_secondary=lambda x: x[value["Name"]]
                                .groupby([x[secondary_key]])
                                .transform("sum")
                            )
                            .drop_duplicates(subset="total_secondary", keep="last")
                            .sort_values(by=["total_secondary"], ascending=False)
                            .iloc[:10, 1]
                        ).to_list()
                    )
                ]
                .set_index([primary_key, "pct", secondary_key])
                .unstack([0, 1])
                .fillna(0)
                .sort_index(axis=1, level=[1, 2])
                .style.format("{0:,.2f}")
                .background_gradient(cmap=sns.light_palette("green", as_cmap=True))
            )
        )
```

## `lag_soe_export_ckjr` 

```python
table = 'lag_soe_export_ckjr'
schema = glue.get_table_information(
    database = db,
    table = table
)['Table']
schema
```

## Count missing values

```python
table_top = parameters["ANALYSIS"]["COUNT_MISSING"]["top"]
table_middle = ""
table_bottom = parameters["ANALYSIS"]["COUNT_MISSING"]["bottom"].format(
    db, table
)

for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if key == len(schema["StorageDescriptor"]["Columns"]) - 1:

        table_middle += "{} ".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
    else:
        table_middle += "{} ,".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
query = table_top + table_middle + table_bottom
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_missing",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
display(
    output.T.rename(columns={0: "total_missing"})
    .assign(total_missing_pct=lambda x: x["total_missing"] / x.iloc[0, 0])
    .sort_values(by=["total_missing"], ascending=False)
    .style.format("{0:,.2%}", subset=["total_missing_pct"])
    .bar(subset="total_missing_pct", color=["#d65f5f"])
)
```

## Categorical Description

During the categorical analysis, we wil count the number of observations for a given group and for a pair.

### Count obs by group

- Index: primary group
- nb_obs: Number of observations per primary group value
- percentage: Percentage of observation per primary group value over the total number of observations

Returns the top 10 only

```python
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(10)","varchar(12)"]:

        print("Nb of obs for {}".format(field["Name"]))

        query = parameters["ANALYSIS"]["CATEGORICAL"]["PAIR"].format(
            db, table, field["Name"]
        )
        output = s3.run_query(
            query=query,
            database=db,
            s3_output=s3_output,
            filename="count_categorical_{}".format(
                field["Name"]
            ),  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )

        ### Print top 10

        display(
            (
                output.set_index([field["Name"]])
                .assign(percentage=lambda x: x["nb_obs"] / x["nb_obs"].sum())
                .sort_values("percentage", ascending=False)
                .head(10)
                .style.format("{0:.2%}", subset=["percentage"])
                .bar(subset=["percentage"], color="#d65f5f")
            )
        )
```

### Count obs by two pair

You need to pass the primary group in the cell below

- Index: primary group
- Columns: Secondary key -> All the categorical variables in the dataset
- nb_obs: Number of observations per primary group value
- Total: Total number of observations per primary group value (sum by row)
- percentage: Percentage of observations per primary group value over the total number of observations per primary group value (sum by row)

Returns the top 10 only

```python
primary_key = "year"
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(10)","varchar(12)"]:
        if field["Name"] != primary_key:
            print(
                "Nb of obs for the primary group {} and {}".format(
                    primary_key, field["Name"]
                )
            )
            query = parameters["ANALYSIS"]["CATEGORICAL"]["MULTI_PAIR"].format(
                db, table, primary_key, field["Name"]
            )

            output = s3.run_query(
                query=query,
                database=db,
                s3_output=s3_output,
                filename="count_categorical_{}_{}".format(
                    primary_key, field["Name"]
                ),  # Add filename to print dataframe
                destination_key=None,  # Add destination key if need to copy output
            )

            display(
                (
                    pd.concat(
                        [
                            (
                                output.loc[
                                    lambda x: x[field["Name"]].isin(
                                        (
                                            output.assign(
                                                total_secondary=lambda x: x["nb_obs"]
                                                .groupby([x[field["Name"]]])
                                                .transform("sum")
                                            )
                                            .drop_duplicates(
                                                subset="total_secondary", keep="last"
                                            )
                                            .sort_values(
                                                by=["total_secondary"], ascending=False
                                            )
                                            .iloc[:10, 1]
                                            .to_list()
                                        )
                                    )
                                ]
                                .set_index([primary_key, field["Name"]])
                                .unstack([0])
                                .fillna(0)
                                .assign(total=lambda x: x.sum(axis=1))
                                .sort_values(by=["total"])
                            ),
                            (
                                output.loc[
                                    lambda x: x[field["Name"]].isin(
                                        (
                                            output.assign(
                                                total_secondary=lambda x: x["nb_obs"]
                                                .groupby([x[field["Name"]]])
                                                .transform("sum")
                                            )
                                            .drop_duplicates(
                                                subset="total_secondary", keep="last"
                                            )
                                            .sort_values(
                                                by=["total_secondary"], ascending=False
                                            )
                                            .iloc[:10, 1]
                                            .to_list()
                                        )
                                    )
                                ]
                                .rename(columns={"nb_obs": "percentage"})
                                .set_index([primary_key, field["Name"]])
                                .unstack([0])
                                .fillna(0)
                                .apply(lambda x: x / x.sum(), axis=1)
                            ),
                        ],
                        axis=1,
                    )
                    .fillna(0)
                    # .sort_index(axis=1, level=1)
                    .style.format("{0:,.2f}", subset=["nb_obs", "total"])
                    .bar(subset=["total"], color="#d65f5f")
                    .format("{0:,.2%}", subset=("percentage"))
                    .background_gradient(
                        cmap=sns.light_palette("green", as_cmap=True), subset=("nb_obs")
                    )
                )
            )
```

<!-- #region -->
## Continuous description

There are three possibilities to show the ditribution of a continuous variables:

1. Display the percentile
2. Display the percentile, with one primary key
3. Display the percentile, with one primary key, and a secondary key


### 1. Display the percentile

- pct: Percentile [.25, .50, .75, .95, .90]
<!-- #endregion -->

```python
table_top = ""
table_top_var = ""
table_middle = ""
table_bottom = ""

var_index = 0
size_continuous = len([len(x) for x in schema["StorageDescriptor"]["Columns"] if 
                       x['Type'] in ["float", "double", "bigint", "decimal(21,5)"]])
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"][
                "bottom"
            ].format(db, table, value["Name"], key)
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["middle_1"],
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["bottom"].format(
                    db, table, value["Name"], key
                ),
            )
            temp_middle_2 = parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"][
                "middle_2"
            ].format(value["Name"])

            if cont == size_continuous:

                table_top_var += "{} {}".format(
                    value["Name"],
                    parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_3"],
                )
                table_bottom += "{} {})".format(temp_middle_1, temp_middle_2)
            else:
                table_top_var += "{} ,".format(value["Name"])
                table_bottom += "{} {}".format(temp_middle_1, temp_middle_2)
        var_index += 1

query = (
    parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_1"]
    + table_top
    + parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_2"]
    + table_top_var
    + table_bottom
)
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_distribution",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
(output.sort_values(by="pct").set_index(["pct"]).style.format("{0:.2f}"))
```

### 2. Display the percentile, with one primary key

The primary key will be passed to all the continuous variables

- index: 
    - Primary group
    - Percentile [.25, .50, .75, .95, .90] per primary group value
- Columns: Secondary group
- Heatmap is colored based on the row, ie darker blue indicates larger values for a given row

```python
primary_key = "year"
table_top = ""
table_top_var = ""
table_middle = ""
table_bottom = ""
var_index = 0
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                "bottom"
            ].format(
                db, table, value["Name"], key, primary_key
            )
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "middle_1"
                ],
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "bottom"
                ].format(
                    db, table, value["Name"], key, primary_key
                ),
            )
            temp_middle_2 = parameters["ANALYSIS"]["CONTINUOUS"][
                "ONE_PAIR_DISTRIBUTION"
            ]["middle_2"].format(value["Name"], primary_key)

            if cont == size_continuous:

                table_top_var += "{} {}".format(
                    value["Name"],
                    parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                        "top_3"
                    ],
                )
                table_bottom += "{} {})".format(temp_middle_1, temp_middle_2)
            else:
                table_top_var += "{} ,".format(value["Name"])
                table_bottom += "{} {}".format(temp_middle_1, temp_middle_2)
        var_index += 1

query = (
    parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"]["top_1"]
    + table_top
    + parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"]["top_2"].format(
        primary_key
    )
    + table_top_var
    + table_bottom
)
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_distribution_primary_key",  # Add filename to print dataframe
    destination_key=None,  # Add destination key if need to copy output
)
(
    output.set_index([primary_key, "pct"])
    .unstack(1)
    .T.style.format("{0:,.2f}")
    .background_gradient(cmap=sns.light_palette("blue", as_cmap=True), axis=1)
)
```

### 3. Display the percentile, with one primary key, and a secondary key

The primary and secondary key will be passed to all the continuous variables. The output might be too big so we print only the top 10 for the secondary key

- index:  Primary group
- Columns: 
    - Secondary group
    - Percentile [.25, .50, .75, .95, .90] per secondary group value
- Heatmap is colored based on the column, ie darker green indicates larger values for a given column

```python
primary_key = 'year'
secondary_key = 'regime'
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:

        query = parameters["ANALYSIS"]["CONTINUOUS"]["TWO_PAIRS_DISTRIBUTION"].format(
            db,
            table,
            primary_key,
            secondary_key,
            value["Name"],
        )

        output = s3.run_query(
            query=query,
            database=db,
            s3_output=s3_output,
            filename="count_distribution_{}_{}_{}".format(
                primary_key, secondary_key, value["Name"]
            ),  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )

        print(
            "Distribution of {}, by {} and {}".format(
                value["Name"], primary_key, secondary_key,
            )
        )

        display(
            (
                output.loc[
                    lambda x: x[secondary_key].isin(
                        (
                            output.assign(
                                total_secondary=lambda x: x[value["Name"]]
                                .groupby([x[secondary_key]])
                                .transform("sum")
                            )
                            .drop_duplicates(subset="total_secondary", keep="last")
                            .sort_values(by=["total_secondary"], ascending=False)
                            .iloc[:10, 1]
                        ).to_list()
                    )
                ]
                .set_index([primary_key, "pct", secondary_key])
                .unstack([0, 1])
                .fillna(0)
                .sort_index(axis=1, level=[1, 2])
                .style.format("{0:,.2f}")
                .background_gradient(cmap=sns.light_palette("green", as_cmap=True))
            )
        )
```

## `lag_soe_export_ckr`

```python
table = 'lag_soe_export_ckr'
schema = glue.get_table_information(
    database = db,
    table = table
)['Table']
schema
```

## Count missing values

```python
table_top = parameters["ANALYSIS"]["COUNT_MISSING"]["top"]
table_middle = ""
table_bottom = parameters["ANALYSIS"]["COUNT_MISSING"]["bottom"].format(
    db, table
)

for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if key == len(schema["StorageDescriptor"]["Columns"]) - 1:

        table_middle += "{} ".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
    else:
        table_middle += "{} ,".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
query = table_top + table_middle + table_bottom
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_missing",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
display(
    output.T.rename(columns={0: "total_missing"})
    .assign(total_missing_pct=lambda x: x["total_missing"] / x.iloc[0, 0])
    .sort_values(by=["total_missing"], ascending=False)
    .style.format("{0:,.2%}", subset=["total_missing_pct"])
    .bar(subset="total_missing_pct", color=["#d65f5f"])
)
```

## Categorical Description

During the categorical analysis, we wil count the number of observations for a given group and for a pair.

### Count obs by group

- Index: primary group
- nb_obs: Number of observations per primary group value
- percentage: Percentage of observation per primary group value over the total number of observations

Returns the top 10 only

```python
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(10)","varchar(12)"]:

        print("Nb of obs for {}".format(field["Name"]))

        query = parameters["ANALYSIS"]["CATEGORICAL"]["PAIR"].format(
            db, table, field["Name"]
        )
        output = s3.run_query(
            query=query,
            database=db,
            s3_output=s3_output,
            filename="count_categorical_{}".format(
                field["Name"]
            ),  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )

        ### Print top 10

        display(
            (
                output.set_index([field["Name"]])
                .assign(percentage=lambda x: x["nb_obs"] / x["nb_obs"].sum())
                .sort_values("percentage", ascending=False)
                .head(10)
                .style.format("{0:.2%}", subset=["percentage"])
                .bar(subset=["percentage"], color="#d65f5f")
            )
        )
```

### Count obs by two pair

You need to pass the primary group in the cell below

- Index: primary group
- Columns: Secondary key -> All the categorical variables in the dataset
- nb_obs: Number of observations per primary group value
- Total: Total number of observations per primary group value (sum by row)
- percentage: Percentage of observations per primary group value over the total number of observations per primary group value (sum by row)

Returns the top 10 only

```python
primary_key = "year"
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(10)","varchar(12)"]:
        if field["Name"] != primary_key:
            print(
                "Nb of obs for the primary group {} and {}".format(
                    primary_key, field["Name"]
                )
            )
            query = parameters["ANALYSIS"]["CATEGORICAL"]["MULTI_PAIR"].format(
                db, table, primary_key, field["Name"]
            )

            output = s3.run_query(
                query=query,
                database=db,
                s3_output=s3_output,
                filename="count_categorical_{}_{}".format(
                    primary_key, field["Name"]
                ),  # Add filename to print dataframe
                destination_key=None,  # Add destination key if need to copy output
            )

            display(
                (
                    pd.concat(
                        [
                            (
                                output.loc[
                                    lambda x: x[field["Name"]].isin(
                                        (
                                            output.assign(
                                                total_secondary=lambda x: x["nb_obs"]
                                                .groupby([x[field["Name"]]])
                                                .transform("sum")
                                            )
                                            .drop_duplicates(
                                                subset="total_secondary", keep="last"
                                            )
                                            .sort_values(
                                                by=["total_secondary"], ascending=False
                                            )
                                            .iloc[:10, 1]
                                            .to_list()
                                        )
                                    )
                                ]
                                .set_index([primary_key, field["Name"]])
                                .unstack([0])
                                .fillna(0)
                                .assign(total=lambda x: x.sum(axis=1))
                                .sort_values(by=["total"])
                            ),
                            (
                                output.loc[
                                    lambda x: x[field["Name"]].isin(
                                        (
                                            output.assign(
                                                total_secondary=lambda x: x["nb_obs"]
                                                .groupby([x[field["Name"]]])
                                                .transform("sum")
                                            )
                                            .drop_duplicates(
                                                subset="total_secondary", keep="last"
                                            )
                                            .sort_values(
                                                by=["total_secondary"], ascending=False
                                            )
                                            .iloc[:10, 1]
                                            .to_list()
                                        )
                                    )
                                ]
                                .rename(columns={"nb_obs": "percentage"})
                                .set_index([primary_key, field["Name"]])
                                .unstack([0])
                                .fillna(0)
                                .apply(lambda x: x / x.sum(), axis=1)
                            ),
                        ],
                        axis=1,
                    )
                    .fillna(0)
                    # .sort_index(axis=1, level=1)
                    .style.format("{0:,.2f}", subset=["nb_obs", "total"])
                    .bar(subset=["total"], color="#d65f5f")
                    .format("{0:,.2%}", subset=("percentage"))
                    .background_gradient(
                        cmap=sns.light_palette("green", as_cmap=True), subset=("nb_obs")
                    )
                )
            )
```

<!-- #region -->
## Continuous description

There are three possibilities to show the ditribution of a continuous variables:

1. Display the percentile
2. Display the percentile, with one primary key
3. Display the percentile, with one primary key, and a secondary key


### 1. Display the percentile

- pct: Percentile [.25, .50, .75, .95, .90]
<!-- #endregion -->

```python
table_top = ""
table_top_var = ""
table_middle = ""
table_bottom = ""

var_index = 0
size_continuous = len([len(x) for x in schema["StorageDescriptor"]["Columns"] if 
                       x['Type'] in ["float", "double", "bigint", "decimal(21,5)"]])
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"][
                "bottom"
            ].format(db, table, value["Name"], key)
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["middle_1"],
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["bottom"].format(
                    db, table, value["Name"], key
                ),
            )
            temp_middle_2 = parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"][
                "middle_2"
            ].format(value["Name"])

            if cont == size_continuous:

                table_top_var += "{} {}".format(
                    value["Name"],
                    parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_3"],
                )
                table_bottom += "{} {})".format(temp_middle_1, temp_middle_2)
            else:
                table_top_var += "{} ,".format(value["Name"])
                table_bottom += "{} {}".format(temp_middle_1, temp_middle_2)
        var_index += 1

query = (
    parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_1"]
    + table_top
    + parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["top_2"]
    + table_top_var
    + table_bottom
)
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_distribution",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
(output.sort_values(by="pct").set_index(["pct"]).style.format("{0:.2f}"))
```

### 2. Display the percentile, with one primary key

The primary key will be passed to all the continuous variables

- index: 
    - Primary group
    - Percentile [.25, .50, .75, .95, .90] per primary group value
- Columns: Secondary group
- Heatmap is colored based on the row, ie darker blue indicates larger values for a given row

```python
primary_key = "year"
table_top = ""
table_top_var = ""
table_middle = ""
table_bottom = ""
var_index = 0
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                "bottom"
            ].format(
                db, table, value["Name"], key, primary_key
            )
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "middle_1"
                ],
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "bottom"
                ].format(
                    db, table, value["Name"], key, primary_key
                ),
            )
            temp_middle_2 = parameters["ANALYSIS"]["CONTINUOUS"][
                "ONE_PAIR_DISTRIBUTION"
            ]["middle_2"].format(value["Name"], primary_key)

            if cont == size_continuous:

                table_top_var += "{} {}".format(
                    value["Name"],
                    parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                        "top_3"
                    ],
                )
                table_bottom += "{} {})".format(temp_middle_1, temp_middle_2)
            else:
                table_top_var += "{} ,".format(value["Name"])
                table_bottom += "{} {}".format(temp_middle_1, temp_middle_2)
        var_index += 1

query = (
    parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"]["top_1"]
    + table_top
    + parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"]["top_2"].format(
        primary_key
    )
    + table_top_var
    + table_bottom
)
output = s3.run_query(
    query=query,
    database=db,
    s3_output=s3_output,
    filename="count_distribution_primary_key",  # Add filename to print dataframe
    destination_key=None,  # Add destination key if need to copy output
)
(
    output.set_index([primary_key, "pct"])
    .unstack(1)
    .T.style.format("{0:,.2f}")
    .background_gradient(cmap=sns.light_palette("blue", as_cmap=True), axis=1)
)
```

### 3. Display the percentile, with one primary key, and a secondary key

The primary and secondary key will be passed to all the continuous variables. The output might be too big so we print only the top 10 for the secondary key

- index:  Primary group
- Columns: 
    - Secondary group
    - Percentile [.25, .50, .75, .95, .90] per secondary group value
- Heatmap is colored based on the column, ie darker green indicates larger values for a given column

```python
primary_key = 'year'
secondary_key = 'regime'
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint", "decimal(21,5)"]:

        query = parameters["ANALYSIS"]["CONTINUOUS"]["TWO_PAIRS_DISTRIBUTION"].format(
            db,
            table,
            primary_key,
            secondary_key,
            value["Name"],
        )

        output = s3.run_query(
            query=query,
            database=db,
            s3_output=s3_output,
            filename="count_distribution_{}_{}_{}".format(
                primary_key, secondary_key, value["Name"]
            ),  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )

        print(
            "Distribution of {}, by {} and {}".format(
                value["Name"], primary_key, secondary_key,
            )
        )

        display(
            (
                output.loc[
                    lambda x: x[secondary_key].isin(
                        (
                            output.assign(
                                total_secondary=lambda x: x[value["Name"]]
                                .groupby([x[secondary_key]])
                                .transform("sum")
                            )
                            .drop_duplicates(subset="total_secondary", keep="last")
                            .sort_values(by=["total_secondary"], ascending=False)
                            .iloc[:10, 1]
                        ).to_list()
                    )
                ]
                .set_index([primary_key, "pct", secondary_key])
                .unstack([0, 1])
                .fillna(0)
                .sort_index(axis=1, level=[1, 2])
                .style.format("{0:,.2f}")
                .background_gradient(cmap=sns.light_palette("green", as_cmap=True))
            )
        )
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
create_report(extension = "html")
```
