---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.8.0
  kernel_info:
    name: python3
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region -->
# US Name

Transform china export tariff tax table and by merging china_applied_mfn_tariffs_hs2 and others variables and constructing unit_price and others variables


# Description

 
 
 

## Variables

 
 
 
 
 

## Merge

china_applied_mfn_tariffs_hs2
hs6_china_vat_rebate

### Complementary information

**Reminder** 



# Target

- The file is saved in S3:
- bucket: datalake-datascience
- path: DATA/ECON/TRADE_DATA/CHINA/EXPORT_TARIFF_TAX
- Glue data catalog should be updated
- database: chinese_trade
- Table prefix: china_
- table name: china_export_tariff_tax
- Analytics
- HTML: ANALYTICS/HTML_OUTPUT/china_export_tariff_tax
- Notebook: ANALYTICS/OUTPUT/china_export_tariff_tax

# Metadata

- Key: 178_VAT_rebate_quality
- Epic: Dataset transformation
- US: create vat export temporary table
- Task tag: #data-preparation, #export, #vat-tax
- Analytics reports: https://htmlpreview.github.io/?https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/00_data_catalog/HTML_ANALYSIS/CHINA_EXPORT_TARIFF_TAX.html

# Input Cloud Storage

## Table/file

**Name**

- china_import_export
- china_applied_mfn_tariffs_hs2
- hs6_china_vat_rebate

**Github**

- https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/00_download_data/TRADE_CHINA/import_export.py
- https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/00_download_data/APPLIED_MFN_TARIFFS/mnf_tariff.py
- https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/00_download_data/VAT_REBATE/vat_rebate.py

# Destination Output/Delivery

## Table/file

**Name**

china_export_tariff_tax

**GitHub**

- https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/00_export_vat.md
<!-- #endregion -->
```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil, json, re

path = os.getcwd()
parent_path = str(Path(path).parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-2'
bucket = 'datalake-london'
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

# Prepare query 

Write query and save the CSV back in the S3 bucket `datalake-datascience` 


# Steps


## Example step by step

```python
DatabaseName = 'chinese_trade'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

```python
query= """
WITH filter_data AS (
  SELECT 
    date as year, 
    id, 
    trade_type, 
    business_type, 
    CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, 
    city_prod, 
    origin_and_destination as destination, 
    quantity, 
    value, 
    CASE WHEN trade_type = '进料加工贸易' 
    OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime 
  FROM 
    chinese_trade.china_import_export 
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
    AND (
      business_type = '国有企业' 
      OR business_type = '私营企业' 
      OR business_type = '集体企业' 
      OR business_type = '国有' 
      OR business_type = '私营' 
      OR business_type = '集体'
    ) 
    AND quantity > 0 
    AND value > 0
) 
SELECT 
  * 
FROM 
  (
    WITH aggregate AS (
      SELECT 
        city_prod, 
        year, 
        regime, 
        hs6, 
        destination, 
        SUM(quantity) as quantity, 
        SUM(value) as value 
      FROM 
        filter_data 
      GROUP BY 
        year, 
        regime, 
        HS6, 
        city_prod, 
        destination
    ) 
    SELECT 
      cityen, 
      geocode4_corr, 
      aggregate.year, 
      regime, 
      aggregate.hs6, 
      country_en, 
      ISO_alpha, 
      quantity, 
      value, 
      CASE WHEN quantity = 0 THEN NULL ELSE CAST(
        value AS DECIMAL(16, 5)
      ) / CAST(
        quantity AS DECIMAL(16, 5)
      ) END AS unit_price, 
      lag_vat_m, 
      ln(1 + lag_vat_m) as ln_lag_vat_m, 
      lag_vat_reb_m, 
      ln(1 + lag_vat_reb_m) as ln_lag_vat_reb_m, 
      lag_tax_rebate, 
      ln(1 + lag_tax_rebate) as ln_lag_tax_rebate, 
      lag_import_tax, 
      ln(1 + lag_import_tax) AS ln_lag_import_tax 
    FROM 
      aggregate 
      INNER JOIN (
        SELECT 
          DISTINCT(citycn) as citycn, 
          cityen, 
          geocode4_corr 
        FROM 
          chinese_lookup.city_cn_en
      ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod 
      LEFT JOIN chinese_lookup.china_country_name ON china_country_name.country_cn = aggregate.destination 
      INNER JOIN (
        SELECT 
          year, 
          hs6, 
          LAG(import_tax, 1) OVER (
            PARTITION BY hs6 
            ORDER BY 
              hs6, 
              year
          ) AS lag_import_tax 
        FROM 
          chinese_trade.china_applied_mfn_tariffs_hs2 
        WHERE 
          import_tax IS NOT NULL
      ) as import_tarrif ON import_tarrif.year = aggregate.year 
      AND import_tarrif.hs6 = aggregate.hs6 
      LEFT JOIN (
        SELECT 
          HS6, 
          year, 
          LAG(vat_m, 1) OVER (
            PARTITION BY hs6 
            ORDER BY 
              hs6, 
              year
          ) AS lag_vat_m, 
          LAG(vat_reb_m, 1) OVER (
            PARTITION BY hs6 
            ORDER BY 
              HS6, 
              year
          ) AS lag_vat_reb_m, 
          LAG(tax_rebate, 1) OVER (
            PARTITION BY hs6 
            ORDER BY 
              HS6, 
              year
          ) AS lag_tax_rebate 
        FROM 
          chinese_trade.hs6_china_vat_rebate 
        WHERE 
          vat_m IS NOT NULL
      ) as vat ON aggregate.year = vat.year 
      AND aggregate.HS6 = vat.hs6 
    WHERE 
      lag_tax_rebate IS NOT NULL 
      AND lag_import_tax IS NOT NULL 
    ORDER BY 
      geocode4_corr, 
      HS6, 
      year, 
      regime
  )
LIMIT 10
"""
```

```python
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

<!-- #region -->
# Table `china_export_tariff_tax`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```

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

Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
s3_output = 'DATA/ECON/TRADE_DATA/CHINA/EXPORT_TARIFF_TAX'
table_name = 'china_export_tariff_tax'
```

First, we need to delete the table (if exist)

```python
try:
    response = glue.delete_table(
        database=DatabaseName,
        table=table_name
    )
    print(response)
except Exception as e:
    print(e)
```

Clean up the folder with the previous csv file. Be careful, it will erase all files inside the folder

```python
s3.remove_all_bucket(path_remove = s3_output)
```

```python
%%time
query = """
CREATE TABLE chinese_trade.china_export_tariff_tax WITH (format = 'PARQUET') AS
WITH filter_data AS (
  SELECT 
    date as year, 
    id, 
    trade_type, 
    business_type, 
    CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, 
    city_prod, 
    origin_and_destination as destination, 
    quantity, 
    value, 
    CASE WHEN trade_type = '进料加工贸易' 
    OR trade_type = '一般贸易' THEN 'ELIGIBLE' ELSE 'NOT_ELIGIBLE' END as regime 
  FROM 
    chinese_trade.china_import_export 
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
    AND (
      business_type = '国有企业' 
      OR business_type = '私营企业' 
      OR business_type = '集体企业' 
      OR business_type = '国有' 
      OR business_type = '私营' 
      OR business_type = '集体'
    ) 
    AND quantity > 0 
    AND value > 0
) 
SELECT 
  * 
FROM 
  (
    WITH duplicate AS (
      SELECT 
        * 
      FROM 
        (
          WITH aggregate AS (
            SELECT 
              city_prod, 
              year, 
              regime, 
              hs6, 
              destination, 
              SUM(quantity) as quantity, 
              SUM(value) as value 
            FROM 
              filter_data 
            GROUP BY 
              year, 
              regime, 
              HS6, 
              city_prod, 
              destination
          ) 
          SELECT 
            cityen, 
            geocode4_corr, 
            aggregate.year, 
            regime, 
            aggregate.hs6, 
            country_en, 
            ISO_alpha, 
            quantity, 
            value, 
            CASE WHEN quantity = 0 THEN NULL ELSE CAST(
              value AS DECIMAL(16, 5)
            ) / CAST(
              quantity AS DECIMAL(16, 5)
            ) END AS unit_price, 
            lag_vat_m, 
            ln(1 + lag_vat_m) as ln_lag_vat_m, 
            lag_vat_reb_m, 
            ln(1 + lag_vat_reb_m) as ln_lag_vat_reb_m, 
            lag_tax_rebate, 
            ln(1 + lag_tax_rebate) as ln_lag_tax_rebate, 
            lag_import_tax, 
            ln(1 + lag_import_tax) AS ln_lag_import_tax 
          FROM 
            aggregate 
            INNER JOIN (
              SELECT 
                DISTINCT(citycn) as citycn, 
                cityen, 
                geocode4_corr 
              FROM 
                chinese_lookup.china_city_code_normalised
            ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod 
            LEFT JOIN chinese_lookup.china_country_name ON china_country_name.country_cn = aggregate.destination 
            INNER JOIN (
              SELECT 
                year, 
                hs6, 
                LAG(import_tax, 1) OVER (
                  PARTITION BY hs6 
                  ORDER BY 
                    hs6, 
                    year
                ) AS lag_import_tax 
              FROM 
                chinese_trade.china_applied_mfn_tariffs_hs2 
              WHERE 
                import_tax IS NOT NULL
            ) as import_tarrif ON import_tarrif.year = aggregate.year 
            AND import_tarrif.hs6 = aggregate.hs6 
            LEFT JOIN (
              SELECT 
                HS6, 
                year, 
                LAG(vat_m, 1) OVER (
                  PARTITION BY hs6 
                  ORDER BY 
                    hs6, 
                    year
                ) AS lag_vat_m, 
                LAG(vat_reb_m, 1) OVER (
                  PARTITION BY hs6 
                  ORDER BY 
                    HS6, 
                    year
                ) AS lag_vat_reb_m, 
                LAG(tax_rebate, 1) OVER (
                  PARTITION BY hs6 
                  ORDER BY 
                    HS6, 
                    year
                ) AS lag_tax_rebate 
              FROM 
                chinese_trade.hs6_china_vat_rebate 
              WHERE 
                vat_m IS NOT NULL
            ) as vat ON aggregate.year = vat.year 
            AND aggregate.HS6 = vat.hs6 
          WHERE 
            lag_tax_rebate IS NOT NULL 
            AND lag_import_tax IS NOT NULL 
          ORDER BY 
            geocode4_corr, 
            HS6, 
            year, 
            regime
        )
    ) 
    SELECT 
      duplicate.geocode4_corr, 
      duplicate.year, 
      duplicate.regime, 
      duplicate.hs6, 
      duplicate.country_en, 
      iso_alpha, 
      quantity, 
      value, 
      unit_price, 
      lag_vat_m, 
      ln_lag_vat_m, 
      lag_vat_reb_m, 
      ln_lag_vat_reb_m, 
      lag_tax_rebate, 
      ln_lag_tax_rebate, 
      lag_import_tax, 
      ln_lag_import_tax, 
      cnt 
    FROM 
      duplicate 
      INNER JOIN (
        SELECT 
          COUNT(*) AS cnt, 
          geocode4_corr, 
          year, 
          regime, 
          hs6, 
          country_en 
        FROM 
          duplicate 
        GROUP BY 
          geocode4_corr, 
          year, 
          regime, 
          hs6, 
          country_en
      ) as tmp ON duplicate.geocode4_corr = tmp.geocode4_corr 
      AND duplicate.year = tmp.year 
      AND duplicate.regime = tmp.regime 
      AND duplicate.hs6 = tmp.hs6 
      AND duplicate.country_en = tmp.country_en 
  ) 
WHERE 
  cnt = 1
  AND ln_lag_vat_reb_m IS NOT NULL
  AND lag_vat_reb_m IS NOT NULL

""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output,
                )
output
```

```python
query_count = """
SELECT COUNT(*) AS CNT
FROM {}.{} 
""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query_count,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output
```

# Validate query

This step is mandatory to validate the query in the ETL. If you are not sure about the quality of the query, go to the next step.


To validate the query, please fillin the json below. Don't forget to change the schema so that the crawler can use it.

1. Change the schema if needed. It is highly recommanded to add comment to the fields
2. Add a partition key:
    - Inform if there is group in the table so that, the parser can compute duplicate
3. Provide a description -> detail the steps 


1. Change the schema

Bear in mind that CSV SerDe (OpenCSVSerDe) does not support empty fields in columns defined as a numeric data type. All columns with missing values should be saved as string. 

```python
glue.get_table_information(
    database = DatabaseName,
    table = table_name)['Table']['StorageDescriptor']['Columns']
```

```python
schema = [{'Name': 'cityen', 'Type': 'string', 'Comment': 'English city name'},
 {'Name': 'geocode4_corr', 'Type': 'string', 'Comment': 'Chinese city code'},
 {'Name': 'year', 'Type': 'string', 'Comment': 'year'},
 {'Name': 'regime', 'Type': 'varchar(12)', 'Comment': 'eligible or no eligible to vat rebate'},
 {'Name': 'hs6', 'Type': 'string', 'Comment': 'HS6 6 digit'},
 {'Name': 'country_en', 'Type': 'string', 'Comment': 'English country name'},
 {'Name': 'iso_alpha', 'Type': 'string', 'Comment': ''},
 {'Name': 'quantity', 'Type': 'bigint', 'Comment': 'Export quantity'},
 {'Name': 'value', 'Type': 'bigint', 'Comment': 'Export value'},
 {'Name': 'unit_price', 'Type': 'decimal(21,5)', 'Comment': 'product unit price'},
 {'Name': 'lag_vat_m', 'Type': 'float', 'Comment': 'lag vat tax'},
 {'Name': 'ln_lag_vat_m', 'Type': 'double', 'Comment': 'log lag vat tax'},
 {'Name': 'lag_vat_reb_m', 'Type': 'float', 'Comment': 'lag vat rebate'},
 {'Name': 'ln_lag_vat_reb_m', 'Type': 'double', 'Comment': 'log lag vat rebate'},
 {'Name': 'lag_tax_rebate', 'Type': 'float', 'Comment': 'lag vat rebate to pay'},
 {'Name': 'ln_lag_tax_rebate', 'Type': 'double', 'Comment': 'log lag vat rebate to pay'},
 {'Name': 'lag_import_tax', 'Type': 'float', 'Comment': 'lag import tax'},
 {'Name': 'ln_lag_import_tax', 'Type': 'double', 'Comment': 'log lag import tax'},
         {'Name': 'cnt', 'Type': 'bigint', 'Comment': 'remove duplicate'}]
```

2. Provide a description

```python
description = """
Filter trade data, aggregate quantity and values and construct unit price
"""
```

3. provide metadata

- DatabaseName:
- TablePrefix:
- input: 
- filename: Name of the notebook or Python script: to indicate
- Task ID: from Coda
- index_final_table: a list to indicate if the current table is used to prepare the final table(s). If more than one, pass the index. Start at 0
- if_final: A boolean. Indicates if the current table is the final table -> the one the model will be used to be trained

```python
name_json = 'parameters_ETL_VAT_rebate_quality_china.json'
path_json = os.path.join(str(Path(path).parent.parent), 'utils',name_json)
```

```python
with open(path_json) as json_file:
    parameters = json.load(json_file)
```

```python
partition_keys = ["geocode4_corr", "year", "regime", "hs6", "country_en" ]
notebookname =  "00_export_vat.ipynb"
index_final_table = [0]
if_final = 'False'
```

```python
github_url = os.path.join(
    "https://github.com/",
    parameters['GLOBAL']['GITHUB']['owner'],
    parameters['GLOBAL']['GITHUB']['repo_name'],
    "blob/master",
    re.sub(parameters['GLOBAL']['GITHUB']['repo_name'],
           '', re.sub(
               r".*(?={})".format(parameters['GLOBAL']['GITHUB']['repo_name'])
               , '', path))[1:],
    re.sub('.ipynb','.md',notebookname)
)
```

Grab the input name from query

```python
list_input = []
tables = glue.get_tables(full_output = False)
regex_matches = re.findall(r'(?=\.).*?(?=\s)|(?=\.\").*?(?=\")', query)
for i in regex_matches:
    cleaning = i.lstrip().rstrip().replace('.', '').replace('"', '')
    if cleaning in tables and cleaning != table_name:
        list_input.append(cleaning)
```

```python
json_etl = {
    'description': description,
    'query': query,
    'schema': schema,
    'partition_keys': partition_keys,
    'metadata': {
        'DatabaseName': DatabaseName,
        'TableName': table_name,
        'input': list_input,
        'target_S3URI': os.path.join('s3://', bucket, s3_output),
        'from_athena': 'True',
        'filename': notebookname,
        'index_final_table' : index_final_table,
        'if_final': if_final,
         'github_url':github_url
    }
}
json_etl['metadata']
```

**Chose carefully PREPARATION or TRANSFORMATION**

```python
index_to_remove = next(
                (
                    index
                    for (index, d) in enumerate(parameters['TABLES']['TRANSFORMATION']['STEPS'])
                    if d['metadata']['TableName'] == table_name
                ),
                None,
            )
if index_to_remove != None:
    parameters['TABLES']['TRANSFORMATION']['STEPS'].pop(index_to_remove)
parameters['TABLES']['TRANSFORMATION']['STEPS'].append(json_etl)
```

```python
print("Currently, the ETL has {} tables".format(len(parameters['TABLES']['TRANSFORMATION']['STEPS'])))
```

Save JSON

```python
with open(path_json, "w") as json_file:
    json.dump(parameters, json_file)
```

# Create or update the data catalog

The query is saved in the S3 (bucket `datalake-datascience`) but the table is not available yet in the Data Catalog. Use the function `create_table_glue` to generate the table and update the catalog.

Few parameters are required:

- name_crawler: Name of the crawler
- Role: Role to temporary provide an access tho the service
- DatabaseName: Name of the database to create the table
- TablePrefix: Prefix of the table. Full name of the table will be `TablePrefix` + folder name

To update the schema, please use the following structure

```
schema = [
    {
        "Name": "VAR1",
        "Type": "",
        "Comment": ""
    },
    {
        "Name": "VAR2",
        "Type": "",
        "Comment": ""
    }
]
```

```python
glue.update_schema_table(
    database = DatabaseName,
    table = table_name,
    schema= schema)
```

## Check Duplicates

One of the most important step when creating a table is to check if the table contains duplicates. The cell below checks if the table generated before is empty of duplicates. The code uses the JSON file to create the query parsed in Athena. 

You are required to define the group(s) that Athena will use to compute the duplicate. For instance, your table can be grouped by COL1 and COL2 (need to be string or varchar), then pass the list ['COL1', 'COL2'] 

```python
#partition_keys = []

with open(path_json) as json_file:
    parameters = json.load(json_file)
```

```python
### COUNT DUPLICATES
if len(partition_keys) > 0:
    groups = ' , '.join(partition_keys)

    query_duplicates = parameters["ANALYSIS"]['COUNT_DUPLICATES']['query'].format(
                                DatabaseName,table_name,groups
                                )
    dup = s3.run_query(
                                query=query_duplicates,
                                database=DatabaseName,
                                s3_output="SQL_OUTPUT_ATHENA",
                                filename="duplicates_{}".format(table_name))
    display(dup)

```

## Count missing values

```python
#table = 'XX'
schema = glue.get_table_information(
    database = DatabaseName,
    table = table_name
)['Table']
```

```python
from datetime import date
today = date.today().strftime('%Y%M%d')
```

```python
table_top = parameters["ANALYSIS"]["COUNT_MISSING"]["top"]
table_middle = ""
table_bottom = parameters["ANALYSIS"]["COUNT_MISSING"]["bottom"].format(
    DatabaseName, table_name
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
    database=DatabaseName,
    s3_output="SQL_OUTPUT_ATHENA",
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

# Update Github Data catalog

The data catalog is available in Glue. Although, we might want to get a quick access to the tables in Github. In this part, we are generating a `README.md` in the folder `00_data_catalogue`. All tables used in the project will be added to the catalog. We use the ETL parameter file and the schema in Glue to create the README. 

Bear in mind the code will erase the previous README. 

```python
README = """
# Data Catalogue

{}

    """

top_readme = """

## Table of Content

    """

template = """

## Table {0}

- Database: {1}
- S3uri: `{2}`
- Partitition: {3}
- Script: {5}

{4}

    """
github_link = os.path.join("https://github.com/", parameters['GLOBAL']['GITHUB']['owner'],
                           parameters['GLOBAL']['GITHUB']['repo_name'], "tree/master/00_data_catalog#table-")
for key, value in parameters['TABLES'].items():
    if key == 'CREATION':
        param = 'ALL_SCHEMA'
    else:
        param = 'STEPS'
        
    for schema in parameters['TABLES'][key][param]:
        description = schema['description']
        DatabaseName = schema['metadata']['DatabaseName']
        target_S3URI = schema['metadata']['target_S3URI']
        partition = schema['partition_keys']
        script = schema['metadata']['github_url']
        
        if param =='ALL_SCHEMA':
            table_name_git = '{}{}'.format(
                schema['metadata']['TablePrefix'],
                os.path.basename(schema['metadata']['target_S3URI']).lower()
            )
        else:
            try:
                table_name_git = schema['metadata']['TableName']
            except:
                table_name_git = '{}{}'.format(
                schema['metadata']['TablePrefix'],
                os.path.basename(schema['metadata']['target_S3URI']).lower()
            )
        
        tb = pd.json_normalize(schema['schema']).to_markdown()
        toc = "{}{}".format(github_link, table_name_git)
        top_readme += '\n- [{0}]({1})'.format(table_name_git, toc)

        README += template.format(table_name_git,
                                  DatabaseName,
                                  target_S3URI,
                                  partition,
                                  tb,
                                  script
                                  )
README = README.format(top_readme)
with open(os.path.join(str(Path(path).parent.parent), '00_data_catalog/README.md'), "w") as outfile:
    outfile.write(README)
```

# Analytics

In this part, we are providing basic summary statistic. Since we have created the tables, we can parse the schema in Glue and use our json file to automatically generates the analysis.

The cells below execute the job in the key `ANALYSIS`. You need to change the `primary_key` and `secondary_key` 


For a full analysis of the table, please use the following Lambda function. Be patient, it can takes between 5 to 30 minutes. Times varies according to the number of columns in your dataset.

Use the function as follow:

- `output_prefix`:  s3://datalake-datascience/ANALYTICS/OUTPUT/TABLE_NAME/
- `region`: region where the table is stored
- `bucket`: Name of the bucket
- `DatabaseName`: Name of the database
- `table_name`: Name of the table
- `group`: variables name to group to count the duplicates
- `primary_key`: Variable name to perform the grouping -> Only one variable for now
- `secondary_key`: Variable name to perform the secondary grouping -> Only one variable for now
- `proba`: Chi-square analysis probabilitity
- `y_var`: Continuous target variables

Check the job processing in Sagemaker: https://eu-west-3.console.aws.amazon.com/sagemaker/home?region=eu-west-3#/processing-jobs

The notebook is available: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience?region=eu-west-3&prefix=ANALYTICS/OUTPUT/&showversions=false

Please, download the notebook on your local machine, and convert it to HTML:

```
cd "/Users/thomas/Downloads/Notebook"
aws s3 cp s3://datalake-datascience/ANALYTICS/OUTPUT/asif_unzip_data_csv/Template_analysis_from_lambda-2020-11-22-08-12-20.ipynb .

## convert HTML no code
jupyter nbconvert --no-input --to html Template_analysis_from_lambda-2020-11-21-14-30-45.ipynb
jupyter nbconvert --to html Template_analysis_from_lambda-2020-11-22-08-12-20.ipynb
```

Then upload the HTML to: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience?region=eu-west-3&prefix=ANALYTICS/HTML_OUTPUT/

Add a new folder with the table name in upper case

```python
import boto3

key, secret_ = con.load_credential()
client_lambda = boto3.client(
    'lambda',
    aws_access_key_id=key,
    aws_secret_access_key=secret_,
    region_name = region)
```

```python
primary_key = 'year'
secondary_key = 'regime'
y_var = 'unit_price'
```

```python
payload = {
    "input_path": "s3://datalake-datascience/ANALYTICS/TEMPLATE_NOTEBOOKS/template_analysis_from_lambda.ipynb",
    "output_prefix": "s3://datalake-datascience/ANALYTICS/OUTPUT/{}/".format(table_name.upper()),
    "parameters": {
        "region": "{}".format(region),
        "bucket": "{}".format(bucket),
        "DatabaseName": "{}".format(DatabaseName),
        "table_name": "{}".format(table_name),
        "group": "{}".format(','.join(partition_keys)),
        "keys": "{},{}".format(primary_key,secondary_key),
        "y_var": "{}".format(y_var),
        "threshold":0
    },
}
payload
```

```python
response = client_lambda.invoke(
    FunctionName='RunNotebook',
    InvocationType='RequestResponse',
    LogType='Tail',
    Payload=json.dumps(payload),
)
response
```

# Generation report

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
import sys
sys.path.append(os.path.join(parent_path, 'utils'))
import make_toc
import create_schema
```

```python
def create_report(extension = "html", keep_code = False, notebookname = None):
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
            notebookname = notebookname  
    
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
create_report(extension = "html", keep_code = True, notebookname =  notebookname)
```

```python
create_schema.create_schema(path_json, path_save_image = os.path.join(parent_path, 'utils'))
```

```python
### Update TOC in Github
for p in [parent_path,
          str(Path(path).parent),
          #os.path.join(str(Path(path).parent), "00_download_data_from"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis", "00_statistical_exploration"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis", "01_model_estimation"),
         ]:
    try:
        os.remove(os.path.join(p, 'README.md'))
    except:
        pass
    path_parameter = os.path.join(parent_path,'utils', name_json)
    md_lines =  make_toc.create_index(cwd = p, path_parameter = path_parameter)
    md_out_fn = os.path.join(p,'README.md')
    
    if p == parent_path:
    
        make_toc.replace_index(md_out_fn, md_lines, Header = os.path.basename(p).replace('_', ' '), add_description = True, path_parameter = path_parameter)
    else:
        make_toc.replace_index(md_out_fn, md_lines, Header = os.path.basename(p).replace('_', ' '), add_description = False)
```
