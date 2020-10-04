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

# Create GDP per capita table from the world bank

# Objective(s)

Create world_gdp_per_capita table from the world bank API (data saved in S3). 
* Add the query to the ETL table creation key

# Metadata

* Epic: Epic 1
* US: US 1
* Date Begin: 10/4/2020
* Duration Task: 0
* Description: Create Athena table with gdp per capita information
* Step type: Raw table
* Status: Active
* Source URL: US 01 create tables Athena
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #athena,#world-bank
* Toggl Tag: #data-preparation
* Meetings:  
* Email Information:  
  * thread: Number of threads: 0(Default 0, to avoid display email)
  *  

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* S3
* Name: 
* TRADE_DATA/RAW/WORLD_BANK/NY.GNP.PCAP.CD_NY.GDP.PCAP.KD
* Github: 
  * https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/00_download_data_from_/WORLD_BANK/gdp_per_capita.py

# Destination Output/Delivery

## Table/file

* Origin: 
* Athena
* Name:
* world_gdp_per_capita
* GitHub:
* https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/01_prepare_tables/03_create_gdp_per_capita_table.md
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
# Creation tables

The data creation, and transformation are done through a JSON file located in the S3 in [DATA/ETL/parameters_ETL_TEMPLATE.json]().

This file will be used for different purposes:

- Validate and run the ETL
- Generate the data catalog in Glue and in Github
- Generate an analysis on the fly and detect erros in the preparation

Please, make sure you carefully modify the document. If it is well-maintained, tremendous time are saved. Besides, this file is versioned in Github, from the root directory `01_data_preprocessing`. Indeed, the parameter file is moved from subfolder to root directory within each notebook.

## How to use

1. Load the json file from the S3, and start populate it
    - If this is the first time, load the template `parameters_ETL_TEMPLATE.json`, else use `parameters_ETL.json`
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

Each variable has to parse should be written inside the schema:

- `Name`: Variable name
- `Type`: Type of variable. Refer to [athena/latest/ug/data-types](https://docs.aws.amazon.com/athena/latest/ug/data-types.html) for the accepted data type
- `Comment`: Provide a comment to the variable

You can add other fields if needed, they will be pushed to Glue.

## Templare prepare table

- To prepare a new table using existing table (i.e Athena tables), copy the template below and paste it inside the list `TABLES.PREPARATION.ALL_SCHEMA`
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

There are three steps to prepare the parameter file:

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

## 2. Prepare `TABLES.CREATION`

This part usually starts with raw/transformed data in S3. The typical architecture in the S3 is:

- `DATA/RAW_DATA` or `DATA/UNZIP_DATA_APPEND_ALL` or `DATA/TRANSFORMED`. One of our rule is, if the user needs to create a table from a CSV/JSON (raw or transformed), then the query should be written in the key `TABLES.CREATION` and the notebook in the folder `01_prepare_tables`

One or more notebooks in the folder `01_prepare_tables` are used to create the raw tables. Please, use the notebook named `XX_template_table_creation_AWS` to create table using the key `TABLES.CREATION`

```python
'Long Name', 'Country Code', 'Country ISO3', 'date',
'GNI per Capita', 'GDP per Capita', 'Income Group'

new_table = [{
    "database": "world_bank",
    "name": "world_gdp_per_capita",
    "output_id": "",
    "separator": ",",
    "s3URI": "s3://chinese-data/TRADE_DATA/RAW/WORLD_BANK/NY.GNP.PCAP.CD_NY.GDP.PCAP.KD",
    "schema": [
        {'Name': 'country', 'Type': 'string', 'Comment': 'Country name'},
        {'Name': 'iso_alpha', 'Type': 'string', 'Comment': 'Country code'},
        {'Name': 'iso_alpha03', 'Type': 'string', 'Comment': 'Country code, iso 03'},
        {'Name': 'year', 'Type': 'string', 'Comment': 'Year'},
        {'Name': 'gni_per_capita', 'Type': 'float', 'Comment': """GDP per capita is gross domestic product divided by midyear population"""},
        {'Name': 'gpd_per_capita', 'Type': 'float', 'Comment': """GNI per capita (formerly GNP per capita) is the gross national income, converted to U.S. dollars using the World Bank Atlas method, divided by the midyear population"""},
        {'Name': 'income_group', 'Type': 'string', 'Comment': """One of 'Others', 'Low income', 'Upper middle income','High income: nonOECD', 'Lower middle income', 'High income: OECD'"""},
    ]
}
]
#len(parameters['TABLES']['CREATION']['ALL_SCHEMA'])
```

To remove an item from the list, use `pop` with the index to remove. Exemple `parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(6)` will remove the 5th item

```python
to_remove = True
if to_remove:
    parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(-1)
```

```python
parameters['TABLES']['CREATION']['ALL_SCHEMA'].extend(new_table)
```

```python
parameters['TABLES']['CREATION']['ALL_SCHEMA'][-1]
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

If you don't need to execute all the schema, change to code below from
    
```
for key, value in parameters["TABLES"]["CREATION"].items():
    if key == "ALL_SCHEMA":
        for table_info in value:
``` 

To 
    
```
for key, value in parameters["TABLES"]["CREATION"].items():
    if key == "ALL_SCHEMA":
        for table_info in value:
            if table_info['name'] in ['TABLE NAME']:
```

Alter `TABLE NAME` with the tables you want to create

## Warning

There are two ways to create a table from csv in Athena:

- `Lazyserde`
- `OpenCSVSerde` 

Both method are available in the template `parameters["TABLES"]["CREATION"]["template"]`. To choose one of the methode, use the key:

- `Lazyserde` Default -> `parameters["TABLES"]["CREATION"]["template"]["bottom_Lazyserde"]`
- `OpenCSVSerde` -> `parameters["TABLES"]["CREATION"]["template"]["bottom_OpenCSVSerde"]`
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
            if table_info['name'] in ['world_gdp_per_capita']:

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
                                query="DROP TABLE {}.{}".format(table_info["database"],table_info["name"]),
                                database=db,
                                s3_output=s3_output
                        )

                ## RUN QUERY
                output = s3.run_query(
                            query=query,
                            database=table_info["database"],
                            s3_output=s3_output,
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

<!-- #region -->
### Create and save data catalog

The data catalog is available in Glue. Although, we might want to get a quick access to the tables in Github. In this part, we are generating a `README.md` in the folder `00_data_catalogue`. All tables used in the project will be added to the catalog. We use the ETL parameter file and the schema in Glue to create the README. 


To generate the table of content, please add:
 
- Repo's owner
- Repo's name

Bear in mind the code will erase the previous README. 
<!-- #endregion -->

```python
add_catalog_github = True
owner = 'thomaspernet'
repo_name = 'VAT_rebate_quality_china'
```

```python
if add_catalog_github:
    README = """
# Data Catalogue

{}

    """

    top_readme = """

## Table of Content

    """

    github_link = os.path.join("https://github.com/", owner, repo_name,"tree/master/00_data_catalogue#table-")

    for key in parameters['TABLES'].keys():
        for i, table in enumerate(parameters['TABLES'][key]['ALL_SCHEMA']):
            if key == 'CREATION':
                github_db = table['database']
                github_tb = table['name']
            else:
                step_name = "STEPS_{}".format(i)
                github_db =table[step_name]['execution'][0]['database']
                github_tb = table[step_name]['execution'][0]['name']

            toc = "{}{}".format(github_link, github_tb)
            top_readme += '\n- [{0}]({1})'.format(github_tb,toc)
            schema_github = glue.get_table_information(
            database = github_db,
            table = github_tb
        )

            github_owner = schema_github['Table']['Owner']
            github_db = schema_github['Table']['DatabaseName']
            github_table = schema_github['Table']['Name']
            github_location = schema_github['Table']['StorageDescriptor']['Location'].replace(
                's3://', 'https://s3.console.aws.amazon.com/s3/buckets/')
            github_s3uril = schema_github['Table']['StorageDescriptor']['Location']
            tb = pd.json_normalize(schema_github['Table']['StorageDescriptor']['Columns']).to_markdown()

            template = """

## Table {0}

- Owner: {1} 
- Database: {2}
- Filename: {0}
- Location: {3}
- S3uri: `{4}`


{5}

    """
            README += template.format(github_table,
                                      github_owner,
                                      github_db,
                                      github_location,
                                      github_s3uril,
                                      tb)
    README = README.format(top_readme)
    with open(os.path.join(str(Path(path).parent.parent), '00_data_catalogue/README.md'), "w") as outfile:
            outfile.write(README)
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
primary_key = "income_group"
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
primary_key = 'income_group'
secondary_key = 'country'
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
