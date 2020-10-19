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
* VAT_export_2003_2010
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

## 2. Prepare `TABLES.CREATION` 

This part should already been done with the notebooks in the folder `01_prepare_tables`. At this stage, the key `TABLES.CREATION`  in parameter file should have content, except if the project needs tables already created from a different project

## 3. Prepare `TABLES.PREPARATION`

In this stage of the ETL, we are processing the data from existing tables in Athena. This stage is meant to use one or more table to create temporary, intermediate or final tables to use in the analysis. The notebook template is named `XX_template_table_preprocessing_AWS` and should be saved in the child folder `02_prepare_tables_model`

```python
### If chinese characters, set  ensure_ascii=False
s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
```

### Detail steps

1. Filter the trade data
    1. Filter year 2002 to 2010
    2. Keep processing and ordinary trade and create regime variable
    3. Keep domestic players (remove foreign firms)
    4. Remove intermediate companies
    5. Exclude negative quality and value
2. Aggregate quantity and value
    1. Aggregate quantiy and value by year, regime (ordinary, processing), HS6 (6 digits product ID), city and destination country
3. Create unit value
    1. Compute unit value as value/quantity. Remove when division is null
    2. Merge import tariff
    3. Merge VAT tax
    4. Merge city name in english and city ID
    5. Merge with country name
    6. Get the lag of tax and tariff 
    7. Compute log of tax and tariff
    8. Remove NULL value of tax and tariff

```python
step_0 = {
   "STEPS_0":{
      "name":"Join export, tariff and tax",
      "execution":[
         {
            "database":"chinese_trade",
            "name":"VAT_export_2003_2010",
            "s3_location": "ATHENA/MAIN", 
            "output_id":"",
            "query":{
               "top":"WITH filter_data AS ( SELECT date as year, id, trade_type, business_type, CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, city_prod, origin_or_destination as destination, quantities, value, CASE WHEN trade_type = '进料加工贸易' OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime FROM chinese_trade.import_export WHERE date in ('2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010') AND imp_exp = '出口' AND ( trade_type = '进料加工贸易' OR trade_type = '一般贸易' OR trade_type = '来料加工装配贸易' OR trade_type = '加工贸易') AND intermediate = 'No' AND ( business_type = '国有企业' OR business_type = '私营企业' OR business_type = '集体企业' OR business_type = '国有' OR business_type = '私营' OR business_type = '集体' ) AND quantities > 0 AND value > 0)",
               "middle":"SELECT * FROM ( WITH aggregate AS ( SELECT city_prod, year, regime, hs6, destination, SUM(quantities) as quantity, SUM(value) as value FROM filter_data GROUP BY year, regime, HS6, city_prod, destination )",
               "bottom":"SELECT cityen, geocode4_corr, aggregate.year, regime, aggregate.hs6, country_en, ISO_alpha, quantity, value, CASE WHEN quantity = 0 THEN NULL ELSE CAST( value AS DECIMAL(16, 5) )/ CAST( quantity AS DECIMAL(16, 5) ) END AS unit_price, lag_tax_rebate, ln(1 + lag_tax_rebate) as ln_lag_tax_rebate, lag_vat_reb_m, ln(1 + lag_vat_reb_m) as ln_lag_vat_reb_m, lag_import_tax, ln(1 + lag_import_tax) AS ln_lag_import_tax FROM aggregate INNER JOIN ( SELECT DISTINCT(citycn) as citycn, cityen, geocode4_corr FROM chinese_lookup.city_cn_en ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod LEFT JOIN chinese_lookup.country_cn_en ON country_cn_en.Country_cn = aggregate.destination INNER JOIN ( SELECT year, hs02, LAG(import_tax, 1) OVER ( PARTITION BY hs02 ORDER BY hs02, year ) AS lag_import_tax FROM chinese_trade.applied_mfn_tariffs_hs02_china_2002_2010 WHERE import_tax IS NOT NULL ) as import_tarrif ON import_tarrif.year = aggregate.year AND import_tarrif.HS02 = aggregate.hs6 LEFT JOIN ( SELECT HS6, year, tax_rebate, vat_m, vat_reb_m, LAG(vat_m, 1) OVER ( PARTITION BY hs6 ORDER BY hs6, year ) AS lag_vat_m, LAG(vat_reb_m, 1) OVER ( PARTITION BY hs6 ORDER BY HS6, year ) AS lag_vat_reb_m, LAG(tax_rebate, 1) OVER ( PARTITION BY hs6 ORDER BY HS6, year ) AS lag_tax_rebate FROM chinese_trade.base_hs6_vat_2002_2012 WHERE vat_m IS NOT NULL ) as vat ON aggregate.year = vat.year AND aggregate.HS6 = vat.hs6 WHERE lag_tax_rebate IS NOT NULL AND lag_import_tax IS NOT NULL ORDER BY geocode4_corr, HS6, year, regime )"
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

```python
#step_0
```

To remove an item from the list, use `pop` with the index to remove. Exemple `parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].pop(6)` will remove the 5th item

```python
to_remove = True
if to_remove:
    parameters['TABLES']['PREPARATION']['ALL_SCHEMA'].pop(0)
```

```python
parameters["TABLES"]["PREPARATION"]['ALL_SCHEMA'].append(step_0)
```

```python
parameters["TABLES"]["PREPARATION"]['ALL_SCHEMA'][-1]
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
* Computation VAT tax:
  * rebate: (vat_m-vat_reb_m)
  * ln_vat_tax = log(1+(vat_m-vat_reb_m))
* Computation VAT rebate:
  * rebate: (vat_m-vat_reb_m)
  * ln_vat_rebate = log(1+vat_reb_m)
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
                    s3_output=step_n['s3_location'],
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
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if value["Type"] in ["float", "double", "bigint"]:

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

            if key == len(schema["StorageDescriptor"]["Columns"]) - 1:

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
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint"]:

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

            if key == len(schema["StorageDescriptor"]["Columns"]) - 1:

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
secondary_key = 'cityen'
```

```python
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint"]:

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
create_report(extension = "html", keep_code= True)
```
