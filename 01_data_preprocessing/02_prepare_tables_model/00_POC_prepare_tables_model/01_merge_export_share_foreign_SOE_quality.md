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

# POC merge ownership export values with quality_vat_export_2003_2010

# Objective(s)

In the context of a poc,  we previously created ownership export values. Now, we need to merge the tables with quality_vat_export_2003_2010 to make sure the query is correct. Need to be careful with the duplicates

# Metadata

* Epic: Epic 1
* US: US 3
* Date Begin: 10/3/2020
* Duration Task: 0
* Description: Merge export values with quality_vat_export_2003_2010 to see if the query generates duplicates
* Step type:  
* Status: Active
* Source URL: US 03 Export share
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 8
* Task tag: #lookup-table,#athena,#sql,#data-preparation
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
* Github: 
  * https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/00_POC_prepare_tables_model/00_export_share_foreign_SOE.md

# Destination Output/Delivery

## Table/file

* Origin: 
* Athena
* Name:
* quality_vat_export_covariate_2003_2010
* GitHub:
* https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/00_POC_prepare_tables_model/01_merge_export_share_foreign_SOE_quality.md
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
parent_path = str(Path(path).parent.parent.parent)


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

# Prepare parameters file

There are three steps to prepara the parameter file:

1. Prepare `GLOBAL` parameters
2. Prepare `TABLES.CREATION`:
    - Usually a notebook in the folder `01_prepare_tables` 
3. Prepare `TABLES.PREPARATION`
    - Usually a notebook in the folder `02_prepare_tables_model` 
    
The parameter file is named `parameters_ETL.json` and will be moved each time in the root folder `01_data_preprocessing` for versioning. When the parameter file is finished, we will use it in the deployment process to run the entire process
 


# Steps

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

# Table `XX`

- Table name: `XX`

```python
query = """
DROP TABLE XX
"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                )
```

```python
query = """

"""
s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
                )
```

# Analytics

In this part, we are providing basic summary statistic. Since we have created the tables, we can parse the schema in Glue and use our json file to automatically generates the analysis.

The cells below execute the job in the key `ANALYSIS`. You need to change the `primary_key` and `secondary_key` 


## Table `XX`

```python
table = 'XX'
schema = glue.get_table_information(
    database = db,
    table = table
)['Table']
schema
```

## Count missing values

```python
from datetime import date
today = date.today().strftime('%Y%M%d')
```

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

    if value["Type"] in ["float", "double", "bigint"]:
        cont +=1

        if var_index == 0:
            table_top_var += "{} ,".format(value["Name"])
            table_top = parameters["ANALYSIS"]["CONTINUOUS"]["ONE_PAIR_DISTRIBUTION"][
                "bottom"
            ].format(
                sdb, table, value["Name"], key, primary_key
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
secondary_key = 'cityen'
```

```python
for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):

    if value["Type"] in ["float", "double", "bigint"]:

        query = parameters["ANALYSIS"]["CONTINUOUS"]["TWO_PAIRS_DISTRIBUTION"].format(
            db, table,
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
