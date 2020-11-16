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
```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false
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

```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false
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
# Parameter file

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
            "name":"Join export, tariff and tax",
            "execution":[
               {
                  "database":"chinese_trade",
                  "name":"VAT_export_2003_2010",
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
      "name":"Join export, tariff and tax",
      "execution":[
         {
            "database":"chinese_trade",
            "name":"VAT_export_2003_2010",
            "output_id":"",
            "query":{
               "top":"",
               "middle":"",
               "bottom":""
            }
         }
      ]
   }
}
```

To add a query execution with a within, use the following template inside the list `STEPS_X.execution`

```
{
   "database":"chinese_trade",
   "name":"VAT_export_2003_2010",
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

```python
### If chinese characters, set  ensure_ascii=False
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

```python
parameters['GLOBAL']['DATABASE'] = 'chinese_trade'
```

## 2. Prepare `TABLES.CREATION`

This part usually starts with raw/transformed data in S3. The typical architecture in the S3 is:

- `DATA/RAW_DATA` or `DATA/UNZIP_DATA_APPEND_ALL` or `DATA/TRANSFORMED`. One of our rule is, if the user needs to create a table from a CSV/JSON (raw or transformed), then the query should be written in the key `TABLES.CREATION` and the notebook in the folder `01_prepare_tables`

One or more notebooks in the folder `01_prepare_tables` are used to create the raw tables. Please, use the notebook named `XX_template_table_creation_AWS` to create table using the key `TABLES.CREATION`

```python
new_tables = [
    {
        "database": "chinese_trade",
        "name": "base_hs6_VAT_2002_2012",
        "s3_location": "ATHENA/MAIN",
        "output_id": "",
        "separator": ",",
        "s3URI": "s3://datalake-datascience/DATA/ECON/TAX_DATA/TRANSFORMED/CHINA_VAT_REBATE/",
        "schema": [
            {"Name": "hs6", "Type": "string", "Comment": "6 digits product line"},
            {"Name": "year", "Type": "string", "Comment": ""},
            {"Name": "vat_m", "Type": "float", "Comment": "6 digits product line tax"},
            {"Name": "vat_reb_m", "Type": "float", "Comment": "Percentage of tax refunded"},
            {"Name": "tax_rebate", "Type": "float", "Comment": "Effective tax level: vat_m - vat_reb_m "}
        ],
    },
    {
        "database": "chinese_trade",
        "name": "applied_mfn_tariffs_hs02_china_2002_2010",
        "s3_location": "ATHENA/MAIN",
        "output_id": "",
        "separator": ",",
        "s3URI": "s3://datalake-datascience/DATA/ECON/TAX_DATA/TRANSFORMED/APPLIED_MFN_TARIFFS_HS2/",
        "schema": [
            {"Name": "reporter", "Type": "string", "Comment": ""},
            {"Name": "year", "Type": "string", "Comment": ""},
            {"Name": "import_tax", "Type": "float", "Comment": ""},
            {"Name": "HS02", "Type": "string", "Comment": ""},
        ],
    },
    {
        "database": "chinese_trade",
        "name": "sigma_industry",
        "s3_location": "ATHENA/MAIN",
        "output_id": "",
        "separator": ",",
        "s3URI": "s3://datalake-datascience/DATA/ECON/INDUSTRY/ADDITIONAL_DATA/SIGMAS_HS3/",
        "schema": [
            {"Name": "ccode", "Type": "string", "Comment": "Country code"},
            {"Name": "cname", "Type": "string", "Comment": "countr name"},
            {"Name": "sigma", "Type": "float", "Comment": "sigma"},
            {"Name": "HS3", "Type": "string", "Comment": "industry code"},
        ],
    },
    {
        "database": "chinese_lookup",
        "name": "country_cn_en",
        "s3_location": "ATHENA/LOOKUP",
        "output_id": "",
        "separator": ",",
        "s3URI": "s3://datalake-datascience/DATA/ECON/LOOKUP_DATA/COUNTRY_NAME_CHINESE/",
        "schema": [
            {
                "Name": "country_cn",
                "Type": "string",
                "Comment": "Country name in Chinese",
            },
            {
                "Name": "country_en",
                "Type": "string",
                "Comment": "Country name in English",
            },
            {"Name": "iso_alpha", "Type": "string", "Comment": "Country code"},
            {"Name": "code_2", "Type": "string", "Comment": "Country code WB"},
        ],
    },
    {
        "database": "chinese_lookup",
        "name": "city_cn_en",
        "s3_location": "ATHENA/LOOKUP",
        "output_id": "",
        "separator": ",",
        "s3URI": "s3://datalake-datascience/DATA/ECON/LOOKUP_DATA/CITY_NAME_CHINESE/",
        "schema": [
            {"Name": "extra_code", "Type": "string", "Comment": "Correspondence code"},
            {"Name": "geocode4_corr", "Type": "string", "Comment": "Official code"},
            {"Name": "citycn", "Type": "string", "Comment": "City name in Chinese"},
            {"Name": "cityen", "Type": "string", "Comment": "City name in English"},
            {
                "Name": "province_cn",
                "Type": "string",
                "Comment": "Province name in Chinese",
            },
            {
                "Name": "province_en",
                "Type": "string",
                "Comment": "Province name in English",
            },
        ],
    },
    {
        "database": "chinese_trade",
        "name": "import_export",
        "s3_location": "ATHENA/MAIN",
        "output_id": "",
        "separator": ",",
        "s3URI": "s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/TRANSFORMED/IMPORT_EXPORT/",
        "schema": [
            {"Name": "date", "Type": "string", "Comment": ""},
            {"Name": "ID", "Type": "string", "Comment": ""},
            {"Name": "business_type", "Type": "string", "Comment": ""},
            {"Name": "intermediate", "Type": "string", "Comment": ""},
            {"Name": "trade_type", "Type": "string", "Comment": ""},
            {"Name": "province", "Type": "string", "Comment": ""},
            {"Name": "city_prod", "Type": "string", "Comment": ""},
            {"Name": "matching_city", "Type": "string", "Comment": ""},
            {"Name": "imp_exp", "Type": "string", "Comment": ""},
            {"Name": "hs", "Type": "string", "Comment": ""},
            {"Name": "origin_or_destination", "Type": "string", "Comment": ""},
            {"Name": "value", "Type": "int", "Comment": ""},
            {"Name": "quantities", "Type": "int", "Comment": ""},
        ],
    },
]
```

To remove an item from the list, use `pop` with the index to remove. Exemple `parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(6)` will remove the 5th item

```python
to_remove = False
if to_remove:
    parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(-1)
```

```python
parameters['TABLES']['CREATION']['ALL_SCHEMA'].extend(new_tables)
```

```python
parameters['TABLES']['CREATION']['ALL_SCHEMA'][-1]
```

```python
#print(json.dumps(parameters, indent=4, sort_keys=False, ensure_ascii=False))
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

```python
import shutil
shutil.move('parameters_ETL.json', '../parameters_ETL.json')
```

<!-- #region heading_collapsed=true -->
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

# Execute jobs

The cell below will execute the queries in the key `TABLES.PREPARATION` for all the steps in `ALL_SCHEMA` 

## Steps

1. Add `quality_vat_export_2003_2010` 
    - No need to execute all the JSON, filter the last table. Use the following code to select a table to create
    
```
for key, value in parameters["TABLES"]["CREATION"].items():
    if key == "ALL_SCHEMA":
        for table_info in value:
            if table_info['name'] in ['TABLE NAME']:
```

and add an intendation
<!-- #endregion -->

```python
s3_output = parameters['GLOBAL']['QUERIES_OUTPUT']
db = parameters['GLOBAL']['DATABASE']
```

```python
for key, value in parameters["TABLES"]["CREATION"].items():
    if key == "ALL_SCHEMA":
        for table_info in value:
            # CREATE QUERY

            ### Create top/bottom query
            table_top = parameters["TABLES"]["CREATION"]["template"]["top"].format(
                        table_info["database"], table_info["name"]
                    )
            table_bottom = parameters["TABLES"]["CREATION"]["template"][
                        "bottom_Lazyserde"
                    ].format(table_info["separator"], table_info["s3URI"])

            ### Create middle
            table_middle = ""
            nb_var = len(table_info["schema"])
            for i, val in enumerate(table_info["schema"]):
                if i == nb_var - 1:
                    table_middle += parameters["TABLES"]["CREATION"]["template"][
                                "middle"
                            ].format(val['Name'], val['Type'], ")")
                else:
                    table_middle += parameters["TABLES"]["CREATION"]["template"][
                                "middle"
                            ].format(val['Name'], val['Type'], ",")

            query = table_top + table_middle + table_bottom

            ## DROP IF EXIST

            s3.run_query(
                            query="DROP TABLE {}".format(table_info["name"]),
                            database=db,
                            s3_output=s3_output
                    )

            ## RUN QUERY
            output = s3.run_query(
                        query=query,
                        database=table_info["database"],
                        s3_output=table_info['s3_location'],
                        filename=None,  ## Add filename to print dataframe
                        destination_key=None,  ### Add destination key if need to copy output
                    )

                ## SAVE QUERY ID
            table_info['output_id'] = output['QueryID']

                     ### UPDATE CATALOG
            glue.update_schema_table(
                        database=table_info["database"],
                        table=table_info["name"],
                        schema=table_info["schema"],
                    )

            print(output)
```

Get the schema of the lattest job

```python
schema = glue.get_table_information(
    database = table_info["database"],
    table = table_info["name"])['Table']
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
    table_info["database"], table_info["name"]
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
            table_info["database"], table_info["name"], field["Name"]
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
primary_key = ""
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
                table_info["database"], table_info["name"], primary_key, field["Name"]
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
                       x['Type'] in ["float", "double", "bigint"]])
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if value["Type"] in ["float", "double", "bigint"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"][
                "bottom"
            ].format(table_info["database"], table_info["name"], value["Name"], key)
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["middle_1"],
                parameters["ANALYSIS"]["CONTINUOUS"]["DISTRIBUTION"]["bottom"].format(
                    table_info["database"], table_info["name"], value["Name"], key
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
primary_key = ""
table_top = ""
table_top_var = ""
table_middle = ""
table_bottom = ""
var_index = 0
cont = 0
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                "bottom"
            ].format(
                table_info["database"], table_info["name"], value["Name"], key, primary_key
            )
        else:
            temp_middle_1 = "{} {}".format(
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "middle_1"
                ],
                parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                    "bottom"
                ].format(
                    table_info["database"], table_info["name"], value["Name"], key, primary_key
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
primary_key = ''
secondary_key = ''
```

```python
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint"]:

        query = parameters["ANALYSIS"]["CONTINUOUS"]["TWO_PAIRS_DISTRIBUTION"].format(
            table_info["database"],
            table_info["name"],
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
