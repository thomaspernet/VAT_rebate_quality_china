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

# Data catalog and pre analysis




# Connexion server

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil, json
import sidetable


path = os.getcwd()
parent_path = str(Path(path).parent)


name_credential = 'XXX.csv'
region = ''
bucket = ''
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False)
glue = service_glue.connect_glue(client = client,
                      bucket = bucket)
```

```python
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

```python nteract={"transient": {"deleting": false}} outputExpanded=false
pd.set_option('display.max_columns', None)
```

# Documente and analyse data

In the first part of the notebook, we will create the data catalog and put the data in the `README.md`. In the second part of the notebook, we will analyse the data. Data analysis contains categorical and continuous variables. It is a batch analysis, nothing should done.

# Download data locally

First of all, load the data locally. Use the function `list_all_files_with_prefix` to parse all the files in a given folder. Change the prefix to the name of the folder in which the data are located.

```python
prefix = 'DATA/RAW_DATA'
```

```python
to_download = False
if to_download:
LOCAL_PATH_CONFIG_FILE = os.getcwd()
FILES_TO_UPLOAD = s3.list_all_files_with_prefix(prefix=prefix)
list(
    map(
        lambda x:
        s3.download_file(key=x, path_local=LOCAL_PATH_CONFIG_FILE),
        FILES_TO_UPLOAD
    )
)
```

## Data catalog

The data catalogue is a json file that we save in the folder `schema`. The schema is the following:

```
{
        "Table": {"Name": "", "StorageDescriptor": {"Columns": [], "Location": ""}}
    }
``` 

The schema is automatically detected and generated from `FILES_TO_UPLOAD`. Since we don't know in advance the field, we cannot add comments at first. To add comments, please refer to the next part. 

### Create and save data catalog

The schemas are saved locally in `schema/FILENAME`. Push the schema to GitHub for availability

```python
def prepare_schema(filename):
    """
    Prepare a json which is similar to glue schema.
    It includes table name, columns, and path to S3

    Output saved in schema/FILENAME
    ARGS:

    filename: string. filename of the doc to get the schema
    """

    schema_ = {
        "Table": {"Name": "", "StorageDescriptor": {"Columns": [], "Location": {'s3URI':"", 's3Bucket': ''}}}
    }

    temp = pd.read_excel(filename)
    schema = pd.io.json.build_table_schema(temp)
    schema_["Table"]["Name"] = filename
    schema_["Table"]["StorageDescriptor"]["Location"]['s3URI'] = os.path.join(
        "s3://", bucket, prefix, filename
    )
    schema_["Table"]["StorageDescriptor"]["Location"]['s3Bucket'] = os.path.join(
        "https://s3.console.aws.amazon.com/s3", bucket, prefix, filename
    )
    for i, name in enumerate(schema["fields"]):
        col = {"Name": name["name"], "Type": name["type"], "Comment": ""}
        schema_["Table"]["StorageDescriptor"]["Columns"].append(col)

    path_name = os.path.join("schema", filename)
    with open("{}.json".format(path_name), "w") as outfile:
        json.dump(schema_, outfile)
        
    return schema_
```

```python
for key, value in enumerate(FILES_TO_UPLOAD):
    table = os.path.split(value)[1]
    schema = prepare_schema(table)
    print(json.dumps(schema, indent=4, sort_keys=False, ensure_ascii=False))
```

### Add comment

This part is optional but strongly recommended. In this part, you are free to add any comment you need. To add a comment, alter the metadata of the file you want. To modify the comment, please, use:

```
[
   {
      "Name":"",
      "Type":"",
      "Comment":""
   }
]
```

Fill only the variables you need to alter

```python
def update_schema_table(filename, schema):
    """
    database: Database name
        table: Table name
        schema: a list of dict:
        [
        {
        'Name': 'geocode4_corr',
        'Type': '',
        'Comment': 'Official chinese city ID'}
        ]
    """
    with open('{}.json'.format(os.path.join("schema", filename)), 'r') as fp:
        parameters = json.load(fp)
        
    list_schema = parameters['Table']['StorageDescriptor']['Columns']
    for field in list_schema:
        try:
            field['Comment'] = next(
                    item for item in schema if item["Name"] == field['Name']
                )['Comment']

        except:
            pass
        
    parameters['Table']['StorageDescriptor']['Columns'] = list_schema
    path_name = os.path.join("schema", filename)
    with open("{}.json".format(path_name), "w") as outfile:
        json.dump(parameters, outfile)
        
    return parameters


```

```python
filename_to_alter = ''
new_schema = [
   {
      "Name":"",
      "Type":"",
      "Comment":" "
   }
]
update_schema_table(filename = filename_to_alter, schema = new_schema)
```

## Generate README 

The README is generated from `FILES_TO_UPLOAD` and will parse all the schema is `schema/FILENAME`

```python
README = "# Data Catalogue"
for key, value in enumerate(FILES_TO_UPLOAD):
    filename = os.path.split(value)[1]
    with open('{}.json'.format(os.path.join("schema", filename)), 'r') as fp:
        parameters = json.load(fp)
    tb = pd.json_normalize(parameters['Table']['StorageDescriptor']['Columns']).to_markdown()
    template = """

## Table {0}

- Filename: {1}
- Location: {2}
- S3uri: `{3}`


{4}

"""
    filename_no_extension = os.path.splitext(filename)[0]
    filename_extension = parameters['Table']['Name']
    location = parameters['Table']['StorageDescriptor']['Location']['s3Bucket']
    uri = parameters['Table']['StorageDescriptor']['Location']['s3URI']
    
    README += template.format(filename_no_extension, filename_extension,location, uri, tb)
    
```

```python
with open("README.md", "w") as outfile:
    outfile.write(README)
```

# Analysis

The notebook file already contains code to analyse the dataset. It contains codes to count the number of observations for a given variables, for a group and a pair of group. It also has queries to provide the distribution for a single column, for a group and a pair of group. The queries are available in the key `ANALYSIS`


## Categorical Description

During the categorical analysis, we wil count the number of observations for a given group and for a pair.

**Count obs by group**

- Index: primary group
- nb_obs: Number of observations per primary group value
- percentage: Percentage of observation per primary group value over the total number of observations

Returns the top 20 only


# FILENAME 1

```python
df_test = pd.read_excel(os.path.split(FILES_TO_UPLOAD[0])[1])
```

Get the values fior each object

```python
dic_ = {'var': [],
       'count':[],
       'values': []}
for v in df_test.select_dtypes(include='object').columns:
    cat = df_test[v].nunique()
    value_cat  = df_test[v].unique()
    dic_['var'].append(v)
    dic_['count'].append(cat)
    dic_['values'].append(value_cat)
(pd.DataFrame(dic_)
 .sort_values(by = ['count'], ascending = False)
 .set_index('var')
)
```

Number of missing values

```python
df_test.isna().sum().sort_values()
```

Frequency 

```python
for objects in list(df_test.select_dtypes(include=["string", "object"]).columns):
    df_count = df_test.stb.freq([objects])
    if df_count.shape[0] > 20:
        df_count = df_count.iloc[:20, :]
    display(
        (
            df_count.reset_index(drop=True)
            .style.format(
                "{0:,.2%}", subset=["Percent", "Cumulative Percent"], na_rep="-"
            )
            .bar(subset=["Cumulative Percent"], color="#d65f5f")
        )
    )
```

## Count obs by two pair

You need to pass the primary group in the cell below

- Index: primary group
- Columns: Secondary key -> All the categorical variables in the dataset
- nb_obs: Number of observations per primary group value
- Total: Total number of observations per primary group value (sum by row)
- percentage: Percentage of observations per primary group value over the total number of observations per primary group value (sum by row)

Returns the top 20 only

```python
primary_key = ""
```

```python
for objects in list(df_test.select_dtypes(include=["string", "object"]).columns):
    if objects not in [primary_key]:
        df_count = df_test.stb.freq([objects])
        if df_count.shape[0] > 20:
            df_count = df_count.iloc[:20, :]
        display(
            (
                df_test.stb.freq([primary_key, objects])
                .set_index([primary_key, objects])
                .drop(columns=['Cumulative Count', 'Cumulative Percent'])
                .iloc[:20, :]
                .unstack(-1)
                .style
                .format(
                    "{0:,.2%}", subset=["Percent"], na_rep="-"
                )
                .format(
                    "{0:,.2f}", subset=["Count"], na_rep="-"
                )
                .background_gradient(
                    cmap=sns.light_palette("green", as_cmap=True), subset=("Count")
                )

            )
        )
```

## Continuous description

There are three possibilities to show the ditribution of a continuous variables:

- Display the percentile
- Display the percentile, with one primary key
- Display the percentile, with one primary key, and a secondary key

```python
filename_to_alter = ""
df_test = pd.read_excel(filename_to_alter)
```

```python
df_test.columns
```

```python
(
    df_test
    .describe()
    .style.format("{0:.2f}")
)
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
```

```python
for objects in list(df_test.select_dtypes(include=["string", "object", 'boolean']).columns)[:1]:
    if objects not in [primary_key]:
        display(
            (
                df_test
                .groupby(primary_key)
                .describe()[objects]
                .sort_values(by='count', ascending=False)
                .iloc[:20, :]
                .style.format("{0:.2f}")
            )
        )
```

<!-- #region nteract={"transient": {"deleting": false}} -->
# Generate reports
<!-- #endregion -->

```python nteract={"transient": {"deleting": false}} outputExpanded=false
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python nteract={"transient": {"deleting": false}} outputExpanded=false
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

```python nteract={"transient": {"deleting": false}} outputExpanded=false
create_report(extension = "html")
```
