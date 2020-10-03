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

# Add step 2 data transformation create baseline table with covariates in ETL

# Objective(s)

The two recent POC done during the US POC merge ownership export values with quality_vat_export_2003_2010 and Poc compute export share covariates by firms ownership have proven to be a success. The queries should be move to the ETL. The new step is the second steps in the data transformation. 

# Metadata

* Epic: Epic 1
* US: US 2
* Date Begin: 10/3/2020
* Duration Task: 0
* Description: POC about covariates are validated. baseline table with covariates can be added to the ETL 
* Step type: Transform table
* Status: Active
* Source URL: US 02 create baseline tables
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #athena,#lookup-table,#sql,#data-preparation
* Toggl Tag: #data-preparation
* Meetings:  
* Email Information:  
  * thread: Number of threads: 0(Default 0, to avoid display email)
  *  

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* Name: 
* lag_foreign_export_ckjr
* lag_foreign_export_ckr
* lag_soe_export_ckjr
* lag_soe_export_ckr
* quality_vat_export_2003_2010
* Github: 
  * https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/00_POC_prepare_tables_model/00_export_share_foreign_SOE.md
  * https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/01_prepare_tables/02_create_quality_tariff_tax_table.md

# Destination Output/Delivery

## Table/file
* Origin: 
 * Athena
* Name:
* quality_vat_export_covariate_2003_2010
* GitHub:
* https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/01_prepare_tables/02_transform_table_baseline_covariates.md
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
parent_path = str(Path(path).parent.parent)


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
# Creation tables

The data creation, and transformation are done through a JSON file. The JSON file is available in the S3, and version in Github.

- [DATA/ETS]

The table schema is automatically modifidied based on the parameter logs

## How to use

1. Load the json file from the S3, and start populate it
2. the template skips the header. If the files does not have a header, remove `'skip.header.line.count'='1'`


### Template create table

- To create a new table using raw data (i.e files in a given folder in the S3), copy the template below and paste it inside the list `TABLES.CREATION.ALL_SCHEMA`

```
{
   "database":"",
   "name":"",
   "output_id":"",
   "separator":",",
   "s3URI":"",
   "schema":[
   {
      "Name":"",
      "Type":"",
      "Comment":""
   }
]
}
```

Each variable has to pass written inside the schema:

- `Name`: Variable name
- `Type`: Type of variable. Refer to [athena/latest/ug/data-types](https://docs.aws.amazon.com/athena/latest/ug/data-types.html) for the accepted data type
- `Comment`: Provide a comment to the variable

You can add other fields if needed, they will be pushed to Glue.

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

## Analytical part

The json file already contains queries to analyse the dataset. It contains queries to count the number of observations for a given variables, for a group and a pair of group. It also has queries to provide the distribution for a single column, for a group and a pair of group. The queries are available in the key `ANALYSIS`
<!-- #endregion -->

# Prepare parameters file

There are three steps to prepara the parameter file:

1. Prepare `GLOBAL` parameters
2. Prepare `TABLES.CREATION`:
    - Usually a notebook in the folder `01_prepare_tables` 
3. Prepare `TABLES.PREPARATION`
    - Usually a notebook in the folder `02_prepare_tables_model` 
    
The parameter file is named `parameters_ETL.json` and will be moved each time in the root folder `01_data_preprocessing` for versioning. When the parameter file is finished, we will use it in the deployment process to run the entire process
    
## Prepare `GLOBAL` parameters
    
To begin with, you need to add the global parameters in the key `GLOBAL`:

- `DATABASE`
- `QUERIES_OUTPUT`. By default, `SQL_OUTPUT_ATHENA`

## 2. Prepare `TABLES.CREATION` 

This part should already been done with the notebooks in the folder `01_prepare_tables`. At this stage, the key `TABLES.CREATION`  in parameter file should have content, except if the project needs tables already created from a different project

## 3. Prepare `TABLES.PREPARATION`

In this stage of the ETL, we are processing the data from existing tables in Athena. This stage is meant to use one or more table to create temporary, intermediate or final tables to use in the analysis. The notebook template is named `XX_template_table_preprocessing_AWS` and should be saved in the child folder `02_prepare_tables_model`

```python
### If chinese characters, set  ensure_ascii=False
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
print(json.dumps(parameters, indent=4, sort_keys=True, ensure_ascii=False))
```

We are now at the second steps of the transformation. The second steps consists in creating four tables:

- lag_foreign_export_ckjr
- lag_foreign_export_ckr
- lag_soe_export_ckjr
- lag_soe_export_ckr

and merge if with `quality_vat_export_2003_2010`

```python
step_1 = [{
   "STEPS_1":{
      "name":"Create lag foreign export at the city, product, destination level",
      "execution":[
         {
            "database":"chinese_trade",
            "name":"lag_foreign_export_ckjr",
            "output_id":"",
            "query":{
               "top":"WITH filter_data AS ( SELECT date as year, id, trade_type, business_type, CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, city_prod, CASE WHEN origin_or_destination = '阿鲁巴' OR origin_or_destination = '阿鲁巴岛' THEN '阿鲁巴岛' WHEN origin_or_destination = '荷属安地列斯群岛' OR origin_or_destination = '荷属安的列斯' THEN '荷属安的列斯' WHEN origin_or_destination = '百慕大' OR origin_or_destination = '百慕大群岛' THEN '百慕大群岛' WHEN origin_or_destination = '多米尼克' OR origin_or_destination = '多米尼加' THEN '多米尼加' WHEN origin_or_destination = '法国' OR origin_or_destination = '马约特' OR origin_or_destination = '马约特岛' THEN '法国' WHEN origin_or_destination = '瓜德罗普' OR origin_or_destination = '瓜德罗普岛' THEN '瓜德罗普岛' WHEN origin_or_destination = '马提尼克' OR origin_or_destination = '马提尼克岛' THEN '马提尼克岛' WHEN origin_or_destination = '阿拉伯联合酋长国' OR origin_or_destination = '阿联酋' THEN '阿拉伯联合酋长国' WHEN origin_or_destination = '巴哈马' OR origin_or_destination = '巴林' THEN '巴林' WHEN origin_or_destination = '中国' OR origin_or_destination = '中华人民共和国' OR origin_or_destination = '台湾省' THEN '中国' WHEN origin_or_destination = '南斯拉夫联盟共和国' OR origin_or_destination = '前南马其顿' OR origin_or_destination = '前南斯拉夫马其顿共和国' THEN '南斯拉夫联盟共和国' WHEN origin_or_destination = '吉尔吉斯' OR origin_or_destination = '吉尔吉斯斯坦' THEN '吉尔吉斯斯坦' WHEN origin_or_destination = '黑山' OR origin_or_destination = '塞尔维亚' THEN '塞尔维亚' ELSE origin_or_destination END AS destination, quantities, value, CASE WHEN trade_type = '进料加工贸易' OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime, CASE WHEN business_type = '外商独资企业' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership FROM chinese_trade.import_export WHERE date in ( '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010' ) AND imp_exp = '出口' AND ( trade_type = '进料加工贸易' OR trade_type = '一般贸易' OR trade_type = '来料加工装配贸易' OR trade_type = '加工贸易' ) AND intermediate = 'No' AND quantities > 0 AND value > 0 )",
               "middle":"SELECT * FROM ( WITH aggregate as ( SELECT year, CAST( CAST(year AS INTEGER) -1 AS VARCHAR ) as year_lag, regime, foreign_ownership, city_prod, HS6, destination, SUM(quantities) as quantities FROM filter_data GROUP BY year, regime, foreign_ownership, city_prod, HS6, destination )",
               "bottom":"SELECT aggregate.year, aggregate.year_lag, aggregate.regime, aggregate.foreign_ownership, geocode4_corr, iso_alpha, aggregate.HS6, quantities, CASE WHEN quantities_lag IS NULL THEN 0 ELSE quantities_lag END AS quantities_lag, CASE WHEN total_quantities_lag IS NULL THEN 0 ELSE total_quantities_lag END AS total_quantities_lag, CASE WHEN quantities_lag IS NULL OR total_quantities_lag IS NULL THEN 0 ELSE CAST( quantities_lag AS DECIMAL(16, 5) )/ CAST( total_quantities_lag AS DECIMAL(16, 5) ) END AS lag_foreign_export_share_ckjr FROM aggregate LEFT JOIN ( SELECT year, regime, foreign_ownership, city_prod, destination, HS6, quantities as quantities_lag FROM aggregate ) as lag_quantities ON aggregate.year_lag = lag_quantities.year AND aggregate.regime = lag_quantities.regime AND aggregate.foreign_ownership = lag_quantities.foreign_ownership AND aggregate.city_prod = lag_quantities.city_prod AND aggregate.HS6 = lag_quantities.HS6 AND aggregate.destination = lag_quantities.destination LEFT JOIN ( SELECT year, regime, HS6, city_prod, destination, SUM(quantities) as total_quantities_lag FROM aggregate GROUP BY year, regime, HS6, city_prod, destination ) as group_lag ON aggregate.year_lag = group_lag.year AND aggregate.regime = group_lag.regime AND aggregate.city_prod = group_lag.city_prod AND aggregate.HS6 = group_lag.HS6 AND aggregate.destination = group_lag.destination INNER JOIN ( SELECT DISTINCT(citycn) as citycn, cityen, geocode4_corr FROM chinese_lookup.city_cn_en ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod LEFT JOIN chinese_lookup.country_cn_en ON country_cn_en.Country_cn = aggregate.destination WHERE aggregate.foreign_ownership = 'FOREIGN' AND iso_alpha IS NOT NULL )"
            }
         }
      ],
       "schema":[
               {
                  "Name":"foreign_ownership",
                  "Type":"varchar(10)",
                  "Comment":"Only FOREIGN"
               }
            ]
   }
},
    {
   "STEPS_2":{
      "name":"Create lag foreign export at the city, product level",
      "execution":[
         {
            "database":"chinese_trade",
            "name":"lag_foreign_export_ckr",
            "output_id":"",
            "query":{
               "top":"WITH filter_data AS ( SELECT date as year, id, trade_type, business_type, CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, city_prod, quantities, value, CASE WHEN trade_type = '进料加工贸易' OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime, CASE WHEN business_type = '外商独资企业' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership FROM chinese_trade.import_export WHERE date in ( '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010' ) AND imp_exp = '出口' AND ( trade_type = '进料加工贸易' OR trade_type = '一般贸易' OR trade_type = '来料加工装配贸易' OR trade_type = '加工贸易' ) AND intermediate = 'No' AND quantities > 0 AND value > 0 )",
               "middle":"SELECT * FROM ( WITH aggregate as ( SELECT year, CAST( CAST(year AS INTEGER) -1 AS VARCHAR ) as year_lag, regime, foreign_ownership, city_prod, HS6, SUM(quantities) as quantities FROM filter_data GROUP BY year, regime, foreign_ownership, city_prod, HS6 )",
               "bottom":"SELECT aggregate.year, aggregate.year_lag, aggregate.regime, aggregate.foreign_ownership, geocode4_corr, aggregate.HS6, quantities, CASE WHEN quantities_lag IS NULL THEN 0 ELSE quantities_lag END AS quantities_lag, CASE WHEN total_quantities_lag IS NULL THEN 0 ELSE total_quantities_lag END AS total_quantities_lag, CASE WHEN quantities_lag IS NULL OR total_quantities_lag IS NULL THEN 0 ELSE CAST( quantities_lag AS DECIMAL(16, 5) )/ CAST( total_quantities_lag AS DECIMAL(16, 5) ) END AS lag_foreign_export_share_ckr FROM aggregate LEFT JOIN ( SELECT year, regime, foreign_ownership, city_prod, HS6, quantities as quantities_lag FROM aggregate ) as lag_quantities ON aggregate.year_lag = lag_quantities.year AND aggregate.regime = lag_quantities.regime AND aggregate.foreign_ownership = lag_quantities.foreign_ownership AND aggregate.city_prod = lag_quantities.city_prod AND aggregate.HS6 = lag_quantities.HS6 LEFT JOIN ( SELECT year, regime, HS6, city_prod, SUM(quantities) as total_quantities_lag FROM aggregate GROUP BY year, regime, HS6, city_prod ) as group_lag ON aggregate.year_lag = group_lag.year AND aggregate.regime = group_lag.regime AND aggregate.city_prod = group_lag.city_prod AND aggregate.HS6 = group_lag.HS6 INNER JOIN ( SELECT DISTINCT(citycn) as citycn, cityen, geocode4_corr FROM chinese_lookup.city_cn_en ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod WHERE aggregate.foreign_ownership = 'FOREIGN' )"
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
},
    {
   "STEPS_3":{
      "name":"Create lag SOE export at the city, product, destination level",
      "execution":[
         {
            "database":"chinese_trade",
            "name":"lag_soe_export_ckjr",
            "output_id":"",
            "query":{
               "top":"WITH filter_data AS ( SELECT date as year, id, trade_type, business_type, CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, city_prod, CASE WHEN origin_or_destination = '阿鲁巴' OR origin_or_destination = '阿鲁巴岛' THEN '阿鲁巴岛' WHEN origin_or_destination = '荷属安地列斯群岛' OR origin_or_destination = '荷属安的列斯' THEN '荷属安的列斯' WHEN origin_or_destination = '百慕大' OR origin_or_destination = '百慕大群岛' THEN '百慕大群岛' WHEN origin_or_destination = '多米尼克' OR origin_or_destination = '多米尼加' THEN '多米尼加' WHEN origin_or_destination = '法国' OR origin_or_destination = '马约特' OR origin_or_destination = '马约特岛' THEN '法国' WHEN origin_or_destination = '瓜德罗普' OR origin_or_destination = '瓜德罗普岛' THEN '瓜德罗普岛' WHEN origin_or_destination = '马提尼克' OR origin_or_destination = '马提尼克岛' THEN '马提尼克岛' WHEN origin_or_destination = '阿拉伯联合酋长国' OR origin_or_destination = '阿联酋' THEN '阿拉伯联合酋长国' WHEN origin_or_destination = '巴哈马' OR origin_or_destination = '巴林' THEN '巴林' WHEN origin_or_destination = '中国' OR origin_or_destination = '中华人民共和国' OR origin_or_destination = '台湾省' THEN '中国' WHEN origin_or_destination = '南斯拉夫联盟共和国' OR origin_or_destination = '前南马其顿' OR origin_or_destination = '前南斯拉夫马其顿共和国' THEN '南斯拉夫联盟共和国' WHEN origin_or_destination = '吉尔吉斯' OR origin_or_destination = '吉尔吉斯斯坦' THEN '吉尔吉斯斯坦' WHEN origin_or_destination = '黑山' OR origin_or_destination = '塞尔维亚' THEN '塞尔维亚' ELSE origin_or_destination END AS destination, quantities, value, CASE WHEN trade_type = '进料加工贸易' OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime, CASE WHEN Business_type = '国有企业' OR Business_type = '国有' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership FROM chinese_trade.import_export WHERE date in ( '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010' ) AND imp_exp = '出口' AND ( trade_type = '进料加工贸易' OR trade_type = '一般贸易' OR trade_type = '来料加工装配贸易' OR trade_type = '加工贸易' ) AND intermediate = 'No' AND quantities > 0 AND value > 0 )",
               "middle":"SELECT * FROM ( WITH aggregate as ( SELECT year, CAST( CAST(year AS INTEGER) -1 AS VARCHAR ) as year_lag, regime, SOE_ownership, city_prod, HS6, destination, SUM(quantities) as quantities FROM filter_data GROUP BY year, regime, SOE_ownership, city_prod, HS6, destination )",
               "bottom":"SELECT aggregate.year, aggregate.year_lag, aggregate.regime, aggregate.SOE_ownership, geocode4_corr, iso_alpha, aggregate.HS6, quantities, CASE WHEN quantities_lag IS NULL THEN 0 ELSE quantities_lag END AS quantities_lag, CASE WHEN total_quantities_lag IS NULL THEN 0 ELSE total_quantities_lag END AS total_quantities_lag, CASE WHEN quantities_lag IS NULL OR total_quantities_lag IS NULL THEN 0 ELSE CAST( quantities_lag AS DECIMAL(16, 5) )/ CAST( total_quantities_lag AS DECIMAL(16, 5) ) END AS lag_soe_export_share_ckjr FROM aggregate LEFT JOIN ( SELECT year, regime, SOE_ownership, city_prod, destination, HS6, quantities as quantities_lag FROM aggregate ) as lag_quantities ON aggregate.year_lag = lag_quantities.year AND aggregate.regime = lag_quantities.regime AND aggregate.SOE_ownership = lag_quantities.SOE_ownership AND aggregate.city_prod = lag_quantities.city_prod AND aggregate.HS6 = lag_quantities.HS6 AND aggregate.destination = lag_quantities.destination LEFT JOIN ( SELECT year, regime, HS6, city_prod, destination, SUM(quantities) as total_quantities_lag FROM aggregate GROUP BY year, regime, HS6, city_prod, destination ) as group_lag ON aggregate.year_lag = group_lag.year AND aggregate.regime = group_lag.regime AND aggregate.city_prod = group_lag.city_prod AND aggregate.HS6 = group_lag.HS6 AND aggregate.destination = group_lag.destination INNER JOIN ( SELECT DISTINCT(citycn) as citycn, cityen, geocode4_corr FROM chinese_lookup.city_cn_en ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod LEFT JOIN chinese_lookup.country_cn_en ON country_cn_en.Country_cn = aggregate.destination WHERE aggregate.SOE_ownership = 'SOE' AND iso_alpha IS NOT NULL )"
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
},
    {
   "STEPS_4":{
      "name":"Create lag SOE export at the city, product level",
      "execution":[
         {
            "database":"chinese_trade",
            "name":"lag_soe_export_ckr",
            "output_id":"",
            "query":{
               "top":"WITH filter_data AS ( SELECT date as year, id, trade_type, business_type, CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, city_prod, quantities, value, CASE WHEN trade_type = '进料加工贸易' OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime, CASE WHEN Business_type = '国有企业' OR Business_type = '国有' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership FROM chinese_trade.import_export WHERE date in ( '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010' ) AND imp_exp = '出口' AND ( trade_type = '进料加工贸易' OR trade_type = '一般贸易' OR trade_type = '来料加工装配贸易' OR trade_type = '加工贸易' ) AND intermediate = 'No' AND quantities > 0 AND value > 0 )",
               "middle":"SELECT * FROM ( WITH aggregate as ( SELECT year, CAST( CAST(year AS INTEGER) -1 AS VARCHAR ) as year_lag, regime, SOE_ownership, city_prod, HS6, SUM(quantities) as quantities FROM filter_data GROUP BY year, regime, SOE_ownership, city_prod, HS6 )",
               "bottom":"SELECT aggregate.year, aggregate.year_lag, aggregate.regime, aggregate.SOE_ownership, geocode4_corr, aggregate.HS6, quantities, CASE WHEN quantities_lag IS NULL THEN 0 ELSE quantities_lag END AS quantities_lag, CASE WHEN total_quantities_lag IS NULL THEN 0 ELSE total_quantities_lag END AS total_quantities_lag, CASE WHEN quantities_lag IS NULL OR total_quantities_lag IS NULL THEN 0 ELSE CAST( quantities_lag AS DECIMAL(16, 5) )/ CAST( total_quantities_lag AS DECIMAL(16, 5) ) END AS lag_soe_export_share_ckr FROM aggregate LEFT JOIN ( SELECT year, regime, SOE_ownership, city_prod, HS6, quantities as quantities_lag FROM aggregate ) as lag_quantities ON aggregate.year_lag = lag_quantities.year AND aggregate.regime = lag_quantities.regime AND aggregate.SOE_ownership = lag_quantities.SOE_ownership AND aggregate.city_prod = lag_quantities.city_prod AND aggregate.HS6 = lag_quantities.HS6 LEFT JOIN ( SELECT year, regime, HS6, city_prod, SUM(quantities) as total_quantities_lag FROM aggregate GROUP BY year, regime, HS6, city_prod ) as group_lag ON aggregate.year_lag = group_lag.year AND aggregate.regime = group_lag.regime AND aggregate.city_prod = group_lag.city_prod AND aggregate.HS6 = group_lag.HS6 INNER JOIN ( SELECT DISTINCT(citycn) as citycn, cityen, geocode4_corr FROM chinese_lookup.city_cn_en ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod WHERE aggregate.SOE_ownership = 'SOE' )"
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
},
    {
   "STEPS_5":{
      "name":"Merge export values with quality baseline table",
      "execution":[
         {
            "database":"chinese_trade",
            "name":"quality_vat_export_covariate_2003_2010",
            "output_id":"",
            "query":{
               "top":" WITH merge_cov AS ( SELECT quality_vat_export_2003_2010.cityen, quality_vat_export_2003_2010.geocode4_corr, quality_vat_export_2003_2010.year, quality_vat_export_2003_2010.regime, quality_vat_export_2003_2010.hs6, hs4, hs3, quality_vat_export_2003_2010.country_en, quality_vat_export_2003_2010.iso_alpha, quantity, value, unit_price, kandhelwal_quality, price_adjusted_quality, lag_tax_rebate, ln_lag_tax_rebate, lag_import_tax, ln_lag_import_tax, sigma, sigma_price, y, prediction, residual, FE_ck, FE_cst, FE_ckr, FE_csrt, FE_kt, FE_pj, FE_jt, FE_ct, CASE WHEN lag_foreign_export_share_ckr IS NULL THEN 0 ELSE lag_foreign_export_share_ckr END AS lag_foreign_export_share_ckr, CASE WHEN lag_soe_export_share_ckr IS NULL THEN 0 ELSE lag_soe_export_share_ckr END AS lag_soe_export_share_ckr, CASE WHEN lag_foreign_export_share_ckjr IS NULL THEN 0 ELSE lag_foreign_export_share_ckjr END AS lag_foreign_export_share_ckjr, CASE WHEN lag_soe_export_share_ckjr IS NULL THEN 0 ELSE lag_soe_export_share_ckjr END AS lag_soe_export_share_ckjr FROM quality_vat_export_2003_2010 ",
                "middle":" LEFT JOIN chinese_trade.lag_foreign_export_ckr ON quality_vat_export_2003_2010.geocode4_corr = lag_foreign_export_ckr.geocode4_corr AND quality_vat_export_2003_2010.year = lag_foreign_export_ckr.year AND quality_vat_export_2003_2010.hs6 = lag_foreign_export_ckr.hs6 AND quality_vat_export_2003_2010.regime = lag_foreign_export_ckr.regime LEFT JOIN chinese_trade.lag_soe_export_ckr ON quality_vat_export_2003_2010.geocode4_corr = lag_soe_export_ckr.geocode4_corr AND quality_vat_export_2003_2010.year = lag_soe_export_ckr.year AND quality_vat_export_2003_2010.hs6 = lag_soe_export_ckr.hs6 AND quality_vat_export_2003_2010.regime = lag_soe_export_ckr.regime LEFT JOIN chinese_trade.lag_foreign_export_ckjr ON quality_vat_export_2003_2010.geocode4_corr = lag_foreign_export_ckjr.geocode4_corr AND quality_vat_export_2003_2010.year = lag_foreign_export_ckjr.year AND quality_vat_export_2003_2010.hs6 = lag_foreign_export_ckjr.hs6 AND quality_vat_export_2003_2010.regime = lag_foreign_export_ckjr.regime AND quality_vat_export_2003_2010.iso_alpha = lag_foreign_export_ckjr.iso_alpha LEFT JOIN chinese_trade.lag_soe_export_ckjr ON quality_vat_export_2003_2010.geocode4_corr = lag_soe_export_ckjr.geocode4_corr AND quality_vat_export_2003_2010.year = lag_soe_export_ckjr.year AND quality_vat_export_2003_2010.hs6 = lag_soe_export_ckjr.hs6 AND quality_vat_export_2003_2010.regime = lag_soe_export_ckjr.regime AND quality_vat_export_2003_2010.iso_alpha = lag_soe_export_ckjr.iso_alpha WHERE quantity IS NOT NULL ) ",
                "bottom":" SELECT merge_cov.cityen, merge_cov.geocode4_corr, merge_cov.year, merge_cov.regime, merge_cov.hs6, hs4, hs3, country_en, merge_cov.iso_alpha, quantity, value, unit_price, kandhelwal_quality, price_adjusted_quality, lag_tax_rebate, ln_lag_tax_rebate, lag_import_tax, ln_lag_import_tax, lag_soe_export_share_ckr, lag_foreign_export_share_ckr lag_soe_export_share_ckjr, lag_foreign_export_share_ckjr, sigma, sigma_price, y, prediction, residual, FE_ck, FE_cst, FE_ckr, FE_csrt, FE_kt, FE_pj, FE_jt, FE_ct FROM merge_cov INNER JOIN ( SELECT year, regime, geocode4_corr, iso_alpha, hs6 FROM merge_cov GROUP BY year, regime, geocode4_corr, iso_alpha, hs6 HAVING COUNT(*) = 1 ) as no_duplicate ON merge_cov.year = no_duplicate.year AND merge_cov.regime = no_duplicate.regime AND merge_cov.geocode4_corr = no_duplicate.geocode4_corr AND merge_cov.iso_alpha = no_duplicate.iso_alpha AND merge_cov.hs6 = no_duplicate.hs6 "
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
]
```

To remove an item from the list, use `pop` with the index to remove. Exemple `parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].pop(6)` will remove the 5th item

```python
to_remove = True
if to_remove:
    parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].pop(-1)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].extend(step_1)
```

```python
parameters['TABLES']['PREPARATION']['ALL_SCHEMA'][-1]
```

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

```python
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

Move `parameters_ETL.json` to the parent folder `01_prepare_tables`

```python
import shutil
shutil.move('parameters_ETL.json', '../parameters_ETL.json')
```

<!-- #region heading_collapsed=true -->
# Execute jobs

The cell below will execute the queries in the key `TABLES.PREPARATION` for all the steps in `ALL_SCHEMA` 

## Steps

1.
<!-- #endregion -->

```python
s3_output = parameters['GLOBAL']['QUERIES_OUTPUT']
db = parameters['GLOBAL']['DATABASE']
```

```python
for key, value in parameters["TABLES"]["PREPARATION"].items():
    if key == "ALL_SCHEMA":
        ### LOOP STEPS
        for i, steps in enumerate(value):
            step_name = "STEPS_{}".format(i)
            if step_name in [
               'STEPS_1', 'STEPS_2', 'STEPS_3', 'STEPS_4', 'STEPS_5']:

                ### LOOP EXECUTION WITHIN STEP
                for j, step_n in enumerate(steps[step_name]["execution"]):

                    ### DROP IF EXIST
                    s3.run_query(
                        query="DROP TABLE {}.{}".format(step_n["database"], step_n["name"]),
                        database=db,
                        s3_output=s3_output,
                    )

                    ### CREATE TOP
                    table_top = parameters["TABLES"]["PREPARATION"]["template"][
                        "top"
                    ].format(step_n["database"], step_n["name"],)

                    ### COMPILE QUERY
                    query = (
                        table_top
                        + step_n["query"]["top"]
                        + step_n["query"]["middle"]
                        + step_n["query"]["bottom"]
                    )
                    output = s3.run_query(
                        query=query,
                        database=db,
                        s3_output=s3_output,
                        filename=None,  ## Add filename to print dataframe
                        destination_key=None,  ### Add destination key if need to copy output
                    )

                    ## SAVE QUERY ID
                    step_n["output_id"] = output["QueryID"]

                    ### UPDATE CATALOG
                    glue.update_schema_table(
                        database=step_n["database"],
                        table=step_n["name"],
                        schema=steps[step_name]["schema"],
                    )

                    print(output)
```

Get the schema of the lattest job

```python
schema = glue.get_table_information(
    database = step_n['database'],
    table = step_n['name'])['Table']
schema
```

# Analytics

The cells below execute the job in the key `ANALYSIS`. You need to change the `primary_key` and `secondary_key` 


## Count missing values

```python
from datetime import date
today = date.today().strftime('%Y%M%d')
```

```python
table_top = parameters["ANALYSIS"]["COUNT_MISSING"]["top"]
table_middle = ""
table_bottom = parameters["ANALYSIS"]["COUNT_MISSING"]["bottom"].format(
    step_n["database"], step_n["name"]
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

# Brief description table

In this part, we provide a brief summary statistic from the lattest jobs. For the continuous analysis with a primary/secondary key, please add the relevant variables you want to know the count and distribution


## Categorical Description

During the categorical analysis, we wil count the number of observations for a given group and for a pair.

### Count obs by group

- Index: primary group
- nb_obs: Number of observations per primary group value
- percentage: Percentage of observation per primary group value over the total number of observations

Returns the top 10 only

```python
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(12)"]:

        print("Nb of obs for {}".format(field["Name"]))

        query = parameters["ANALYSIS"]["CATEGORICAL"]["PAIR"].format(
            step_n["database"], step_n["name"], field["Name"]
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
```

```python
for field in schema["StorageDescriptor"]["Columns"]:
    if field["Type"] in ["string", "object", "varchar(12)"]:
        if field["Name"] != primary_key:
            print(
                "Nb of obs for the primary group {} and {}".format(
                    primary_key, field["Name"]
                )
            )
            query = parameters["ANALYSIS"]["CATEGORICAL"]["MULTI_PAIR"].format(
                step_n["database"], step_n["name"], primary_key, field["Name"]
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

## Continuous description

There are three possibilities to show the ditribution of a continuous variables:

1. Display the percentile
2. Display the percentile, with one primary key
3. Display the percentile, with one primary key, and a secondary key


### 1. Display the percentile

- pct: Percentile [.25, .50, .75, .95, .90]

```python
table_top = ""
table_top_var = ""
table_middle = ""
table_bottom = ""

var_index = 0
size_continuous = len([len(x) for x in schema["StorageDescriptor"]["Columns"] if 
                       x['Type'] in ["float", "double", "bigint", "int"]])
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if value["Type"] in ["float", "double", "bigint", "int" ]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"][
                "bottom"
            ].format(step_n["database"], step_n["name"], value["Name"], key)
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["middle_1"],
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["bottom"].format(
                    step_n["database"], step_n["name"], value["Name"], key
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

    if value["Type"] in ["float", "double", "bigint", "int"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                "bottom"
            ].format(
                step_n["database"], step_n["name"], value["Name"], key, primary_key
            )
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "middle_1"
                ],
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "bottom"
                ].format(
                    step_n["database"], step_n["name"], value["Name"], key, primary_key
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
```

```python
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint", "int"]:

        query = parameters["ANALYSIS"]["CONTINUOUS"]["TWO_PAIRS_DISTRIBUTION"].format(
            step_n["database"],
            step_n["name"],
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
