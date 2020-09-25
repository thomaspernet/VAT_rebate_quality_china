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

<!-- #region -->
# Create Athena tables for the project trade, tariffs and taxes


Select the US you just created →Create Athena tables for the project trade, tariffs and taxes
* The ID is yoa18cktx45832m
* Add notebook Epic Epic 1 US US 1 Create Athena tables for the project trade, tariffs and taxes

# Objective(s)

*   All trade data are available in the S3 in gz format. In this US, we are going to parse the folder and create a table in Athena
* Make sure to add the information in Glue using boto
  * Create a wrapper in AwsPy if needed
* Please, update the Source URL by clicking on the button after the information have been pasted
  * US 01 create tables Athena Modify rows
  * Delete tables and Github related to the US: Delete rows
  
# Metadata

* Epic: Epic 1
* US: US 1
* Date Begin: 9/24/2020
* Duration Task: 0
* Description: Create the Chinese import/export data from 2000 to 2010 along with vat rebate and tariffs
* Status: Active
  * Change Status task: Active
  * Update table: Modify rows
* Source URL: [US 01 create tables Athena](https://coda.io/d/_di6Ik05Tjwm/US-01-create-table-Athena_suIeP)
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 8
* Task tag: #data-preparation,#sql,#athena,#s3
* Toggl Tag: #data-preparation

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
    * S3
* Name: 
    * TRADE_DATA/TRANSFORMED
    * TAX_DATA/TRANSFORMED/VAT_REBATE
    * TAX_DATA/TRANSFORMED/APPLIED_MFN_TARIFFS
    * ADDITIONAL_DATA/SIGMAS_HS3
    * LOOKUP_DATA/COUNTRY_NAME
    * LOOKUP_DATA/CITY_NAME
* Github: 
* https://github.com/thomaspernet/Chinese-Trade-Data
- https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_Data_preprocessing/00_download_data_from_/APPLIED_MFN_TARIFFS
- https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/00_download_data_from_/Sigma/sigma.py

# Destination Output/Delivery

## Table/file

* Origin: 
    * S3
    * Athena
* Name:
    * import_export
    * base_hs6_VAT_2002_2012
    * applied_mfn_tariffs_hs02_china_2002_2010
    * country_cn_en
    * city_cn_en
* GitHub:
  *  [01_tables_trade_tariffs_taxes](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/grid-NpDE6BBM0M/i-TZi-tP5WlR/01_tables_trade_tariffs_taxes.md)
<!-- #endregion -->

## Connexion serveur

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
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
```

```python
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

# Creation tables

The data creation, and transformation are done through a JSON file. The JSON file is available in the S3, and version in Github.

- [DATA/ETS](https://s3.console.aws.amazon.com/s3/buckets/chinese-data/DATA/ETL/?region=eu-west-3)

Note that the template skips the header. If the files does not have a header, remove `'skip.header.line.count'='1'`

If the file is empty, add information and send it to the S3.

## Template create table

```
{
               "database":"",
               "name":"",
               "output_id":"",
               "separator":",",
               "s3URI":"",
               "schema":{
                  "variables":[

                     
                  ],
                  "format":[

                     
                  ],
                  "comments":[
                     
                  ]
               }
          }
```

## Templare prepare table

To prepare a table from existing table, you can use the following schema. The list `ALL_SCHAME` accepts one or more steps. Each steps, `STEPS_X` can be a sequence of queries execution. 

```
"PREPARATION": {
        "ALL_SCHEMA": [
                {"STEPS_00": {
                    'name':'Join export, tariff and tax',
                    'execution':
                    [{
                        "database": "chinese_trade",
                        "name": "VAT_export_2003_2010",
                        "output_id": "",
                        "query": {
                            "top": "",
                            "middle": "",
                            "bottom": ""
                    }
                    }
                    ]
                }
                }],
                "template":{
                    "top": "CREATE TABLE {}.{} WITH (format = 'PARQUET') AS "
                }
            }
``` 

To add a query execution with a within, use the following template:

```
{
                        "database": "chinese_trade",
                        "name": "VAT_export_2003_2010",
                        "output_id": "",
                        "query": {
                            "top": "",
                            "middle": "",
                            "bottom": ""
                    }
                    }
``` 

To add a step, use this template:

```
{"STEPS_X": {
                    'name':'Join export, tariff and tax',
                    'execution':
                    [{
                        "database": "chinese_trade",
                        "name": "VAT_export_2003_2010",
                        "output_id": "",
                        "query": {
                            "top": "",
                            "middle": "",
                            "bottom": ""
                    }
                    }
                    ]
                }
                }
```

Each step name should follow this format `STEPS_0`, `STEPS_1`, `STEPS_2`, etc

## Steps

1. Create Chinese trade data from 2000 to 2010
2. Create Chinese VAT rebate tax
3. Create Chinese tariff
4. Create Chinese sigma

```python
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
print(json.dumps(parameters, indent=4, sort_keys=True))
```

To add or alter the file, copy and paste it below

```python
parameters = {
    "GLOBAL": {
        "DATABASE": "chinese_trade",
        "QUERIES_OUTPUT": "SQL_OUTPUT_ATHENA"
    },
    "TABLES": {
        "CREATION": {
            "ALL_SCHEMA": [
                {
                    "database": "chinese_trade",
                    "name": "import_export",
                    "output_id": "",
                    "s3URI": "s3://chinese-data/TRADE_DATA/TRANSFORMED/",
                    "schema": {
                        "comments": [],
                        "format": [
                            "string",
                            "string",
                            "string",
                            "string",
                            "string",
                            "string",
                            "string",
                            "string",
                            "string",
                            "string",
                            "string",
                            "int",
                            "int"
                        ],
                        "variables": [
                            "date",
                            "ID",
                            "business_type",
                            "intermediate",
                            "trade_type",
                            "province",
                            "city_prod",
                            "matching_city",
                            "imp_exp",
                            "hs",
                            "origin_or_destination",
                            "values",
                            "quantities"
                        ]
                    },
                    "separator": ","
                },
                {
                    "database": "chinese_trade",
                    "name": "base_hs6_VAT_2002_2012",
                    "output_id": "",
                    "s3URI": "s3://chinese-data/TAX_DATA/TRANSFORMED/VAT_REBATE/",
                    "schema": {
                        "comments": [],
                        "format": [
                            "string",
                            "string",
                            "float",
                            "float",
                            "float",
                            "float"
                        ],
                        "variables": [
                            "hs6",
                            "year",
                            "tax_rebate",
                            "ln_vat_rebate",
                            "vat_m",
                            "vat_reb_m"
                        ]
                    },
                    "separator": ","
                },
                {
                    "database": "chinese_trade",
                    "name": "applied_mfn_tariffs_hs02_china_2002_2010",
                    "output_id": "",
                    "s3URI": "s3://chinese-data/TAX_DATA/TRANSFORMED/APPLIED_MFN_TARIFFS/",
                    "schema": {
                        "comments": [],
                        "format": [
                            "string",
                            "string",
                            "float",
                            "string"
                        ],
                        "variables": [
                            "reporter",
                            "year",
                            "import_tax",
                            "HS02"
                        ]
                    },
                    "separator": ","
                },
                {
                    "database": "chinese_trade",
                    "name": "sigma_industry",
                    "output_id": "",
                    "s3URI": "s3://chinese-data/ADDITIONAL_DATA/SIGMAS_HS3/",
                    "schema": {
                        "comments": [
                            "Country code",
                            "countr name",
                            "sigma",
                            "industry code"
                        ],
                        "format": [
                            "string",
                            "string",
                            "float",
                            "string"
                        ],
                        "variables": [
                            "ccode",
                            "cname",
                            "sigma",
                            "HS3"
                        ]
                    },
                    "separator": ","
                },
                {
               "database":"chinese_lookup",
               "name":"country_cn_en",
               "output_id":"",
               "separator":",",
               "s3URI":"s3://chinese-data/LOOKUP_DATA/COUNTRY_NAME/",
               "schema":{
                  "variables":[
                      "country_cn","country_en","iso_alpha","code_2" 
                  ],
                  "format":[
                      "string","string","string","string"
                  ],
                  "comments":[
                      "Country name in Chinese","Country name in English",
                      "Country code","Country code WB"
                     
                  ]
               }
          },
                {
               "database":"chinese_lookup",
               "name":"city_cn_en",
               "output_id":"",
               "separator":",",
               "s3URI":"s3://chinese-data/LOOKUP_DATA/CITY_NAME/",
               "schema":{
                  "variables":[
                      "extra_code","geocode4_corr","citycn","cityen","province_cn","province_en"
                  ],
                  "format":[
                      "string","string","string","string","string","string"
                  ],
                  "comments":[
                     "Correspondence code","Official code","City name in Chinese","City name in English",
                      "Province name in Chinese","Province name in English"
                  ]
               }
          } 
            ],
            "template": {
                "bottom": "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'serialization.format' = ',', 'field.delim' = '{0}') LOCATION '{1}' TBLPROPERTIES ('has_encrypted_data'='false', 'skip.header.line.count'='1')",
                "middle": "{0} {1} {2}",
                "top": "CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} ("
            }
        },
        "PREPARATION": [
            {
                "STEPS_XX": {
                    "query": {
                        "bottom": {},
                        "middle": {},
                        "top": {}
                    }
                },
                "output_id": []
            }
        ]
    }
}
```

```python
json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')
```

<!-- #region -->
# Run Parameters files

1. Create a database

**Chinese trade**


```
CREATE DATABASE chinese_trade
```

**Chinese look up**

```
CREATE DATABASE chinese_lookup
```
<!-- #endregion -->

```python
s3_output = parameters['GLOBAL']['QUERIES_OUTPUT']
bd = parameters['GLOBAL']['DATABASE']
```

```python
for key, value in parameters["TABLES"]["CREATION"].items():
    if key == "ALL_SCHEMA":
        for table_info in value:

            ## CREATE QUERY

            ### Create top/bottom query
            table_top = parameters["TABLES"]["CREATION"]["template"]["top"].format(
                table_info["database"], table_info["name"]
            )
            table_bottom = parameters["TABLES"]["CREATION"]["template"][
                "bottom"
            ].format(table_info["separator"], table_info["s3URI"])

            ### Create middle
            table_middle = ""
            for i, val in enumerate(table_info["schema"]["variables"]):
                if i == len(table_info["schema"]["variables"]) - 1:
                    table_middle += parameters["TABLES"]["CREATION"]["template"][
                        "middle"
                    ].format(val, table_info["schema"]["format"][i], ")")
                else:
                    table_middle += parameters["TABLES"]["CREATION"]["template"][
                        "middle"
                    ].format(val, table_info["schema"]["format"][i], ",")
            query = table_top + table_middle + table_bottom
            ## DROP IF EXIST
        
            s3.run_query(
                    query="DROP TABLE {}".format(table_info["name"]),
                    database=bd,
                    s3_output=s3_output
            )

            ## RUN QUERY
            output = s3.run_query(
                query=query,
                database=bd,
                s3_output=s3_output,
                filename=None,  ## Add filename to print dataframe
                destination_key=None,  ### Add destination key if need to copy output
            )
            ## SAVE QUERY ID
            table_info['output_id'] = output['QueryID']
            print(output)
```

## Show first rows

```python
query = """
SELECT *
FROM import_export
LIMIT 10
"""
s3.run_query(
            query=query,
            database=bd,
            s3_output=s3_output,
            filename='chinese_import_export',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )
```

Count number of lines

```python
query = """
SELECT COUNT(*) AS CNT
FROM import_export
"""
s3.run_query(
            query=query,
            database=bd,
            s3_output=s3_output,
            filename='count_chinese_import_export',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )
```

VAT Tax

```python
query = """
SELECT *
FROM base_hs6_VAT_2002_2012
LIMIT 10
"""
s3.run_query(
            query=query,
            database=bd,
            s3_output=s3_output,
            filename='chinese_base_hs6_VAT_2002_2012',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )
```

Tariff

```python
query = """
SELECT *
FROM applied_mfn_tariffs_hs02_china_2002_2010
LIMIT 10
"""
s3.run_query(
            query=query,
            database=bd,
            s3_output=s3_output,
            filename='chinese_applied_mfn_tariffs_hs02_china_2002_2010',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )
```

Sigma

```python
query = """
SELECT *
FROM sigma_industry
LIMIT 10
"""
s3.run_query(
            query=query,
            database=bd,
            s3_output=s3_output,
            filename='chinese_sigma_industry',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )
```

Country name

```python
query = """
SELECT *
FROM country_cn_en
LIMIT 10
"""
s3.run_query(
            query=query,
            database='chinese_lookup',
            s3_output=s3_output,
            filename='chinese_country_cn_en',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )
```

City name

```python
query = """
SELECT *
FROM city_cn_en
LIMIT 10
"""
s3.run_query(
            query=query,
            database='chinese_lookup',
            s3_output=s3_output,
            filename='chinese_city_cn_en',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )
```
