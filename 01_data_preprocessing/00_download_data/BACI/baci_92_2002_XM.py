import pandas as pd
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from pathlib import Path
import os
import re
import json
from tqdm import tqdm
import zipfile
import tempfile
import requests
import glob
import numpy as np
# Connect to Cloud providers
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-2'
bucket = 'datalake-london'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)

# AWS
con = aws_connector.aws_instantiate(credential=path_cred,
                                    region=region)
client = con.client_boto()
s3 = service_s3.connect_S3(client=client,
                           bucket=bucket, verbose=True)
# Copy destination in S3 without bucket and "/" at the end
PATH_S3 = "DATA/ECON/TRADE_DATA/BACI/IMPORT_EXPORT/2002_2020"
# GCP
url = 'http://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_HS02_V202201.zip'
r = requests.get(url, allow_redirects=True)
open('BACI_HS02_V202201.zip', 'wb').write(r.content)
with zipfile.ZipFile('BACI_HS02_V202201.zip', 'r') as zip_ref:
    zip_ref.extractall('BACI_HS02_V202201')

# preprocess data

df_products = pd.read_csv('BACI_HS02_V202201/product_codes_HS02_V202201.csv',
                          encoding='latin-1', dtype={'code': 'str'})
df_products = (df_products
               .assign(**{
                   "{}".format(col): df_products[col]
                   .astype(str)
                   .str.replace('\,', '|', regex=True)
                   .str.replace(r'\/|\(|\)|\?|\.|\:|\-', '', regex=True)
                   .str.replace('__', '_')
                   .str.replace('\\n', '', regex=True)
                   .str.replace('\"', '', regex=True)
                   # .str.upper()
                   for col in df_products.select_dtypes(include="object").columns
               }
               ).replace('NAN', np.nan)
               )

df_products.head()
df_country = pd.read_csv('BACI_HS02_V202201/country_codes_V202201.csv',
                         encoding='latin-1', dtype={'country_code': 'str'})

for i in tqdm(glob.glob(os.path.join('BACI_HS02_V202201', "*.csv"))):
    if i not in ['BACI_HS02_V202201/country_codes_V202201.csv',
    'BACI_HS02_V202201/product_codes_HS02_V202201.csv']:
        (
        pd.read_csv(i,
                    dtype={'i': 'str', 'j': 'str', 'i': 'str', 'k': 'str'})
        .rename(
            columns={'i': 'origin', 'j': 'destination', 'k': 'hs6'}
        )
        .merge(df_country.rename(columns={
            'country_code': 'origin',
            'country_name_abbreviation': 'country_name_abbreviation_o',
            'country_name_full': 'country_name_full_o',
            'iso_2digit_alpha': 'iso_2digit_alpha_o',
            'iso_3digit_alpha': 'iso_3digit_alpha_o'
        }))
        .merge(df_country.rename(columns={
               'country_code': 'destination',
               'country_name_abbreviation': 'country_name_abbreviation_d',
               'country_name_full': 'country_name_full_d',
               'iso_2digit_alpha': 'iso_2digit_alpha_d',
               'iso_3digit_alpha': 'iso_3digit_alpha_d'
               }))
        .merge(df_products.rename(columns={'code': 'hs6'}))
        .to_csv("cleaned_{}.gz".format(i.split('/')[1]), index =False, compression="gzip")
    )
        s3.upload_file("cleaned_{}".format(i.split('/')[1]), PATH_S3)
        os.remove("cleaned_{}".format(i.split('/')[1]))

# SAVE S3
#for i in pd.io.json.build_table_schema(test)['fields']:
#    if i['type'] in ['number', 'integer']:
#        i['type'] = 'int'
#    print("{},".format({'Name':i['name'], 'Type':i['type'],'Comment':''}))

# ADD SHCEMA
schema = [
    {'Name': 'index', 'Type': 'int', 'Comment': ''},
{'Name': 't', 'Type': 'string', 'Comment': 'year'},
{'Name': 'origin', 'Type': 'string', 'Comment': 'Origin country'},
{'Name': 'destination', 'Type': 'string', 'Comment': 'Destination'},
{'Name': 'hs6', 'Type': 'string', 'Comment': 'HS6 product'},
{'Name': 'v', 'Type': 'int', 'Comment': 'Value of the trade flow (in thousands current USD)'},
{'Name': 'q', 'Type': 'int', 'Comment': 'Quantity (in metric tons)'},
{'Name': 'country_name_abbreviation_o', 'Type': 'string', 'Comment': ''},
{'Name': 'country_name_full_o', 'Type': 'string', 'Comment': ''},
{'Name': 'iso_2digit_alpha_o', 'Type': 'string', 'Comment': ''},
{'Name': 'iso_3digit_alpha_o', 'Type': 'string', 'Comment': ''},
{'Name': 'country_name_abbreviation_d', 'Type': 'string', 'Comment': ''},
{'Name': 'country_name_full_d', 'Type': 'string', 'Comment': ''},
{'Name': 'iso_2digit_alpha_d', 'Type': 'string', 'Comment': ''},
{'Name': 'iso_3digit_alpha_d', 'Type': 'string', 'Comment': ''},
{'Name': 'description', 'Type': 'string', 'Comment': ''},
]

# ADD DESCRIPTION
description = 'Download Baci export values HS6 country source destination from 2002 to 2020'

glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://", bucket, PATH_S3)
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "trade"
TablePrefix = 'baci_'  # add "_" after prefix, ex: hello_


glue.create_table_glue(
    target_S3URI,
    name_crawler,
    Role,
    DatabaseName,
    TablePrefix,
    from_athena=False,
    update_schema=schema,
)

# Add tp ETL parameter files
filename = 'baci_92_2002_XM.py'
path_to_etl = os.path.join(str(Path(path).parent.parent.parent),
                           'utils', 'parameters_ETL_VAT_rebate_quality_china.json')
with open(path_to_etl) as json_file:
    parameters = json.load(json_file)
github_url = os.path.join(
    "https://github.com/",
    parameters['GLOBAL']['GITHUB']['owner'],
    parameters['GLOBAL']['GITHUB']['repo_name'],
    re.sub(parameters['GLOBAL']['GITHUB']['repo_name'],
           '', re.sub(
               r".*(?={})".format(parameters['GLOBAL']['GITHUB']['repo_name']), '', path))[1:],
    filename
)
table_name = '{}{}'.format(TablePrefix, os.path.basename(target_S3URI).lower())
json_etl = {
    'description': description,
    'schema': schema,
    'partition_keys': [],
    'metadata': {
        'DatabaseName': DatabaseName,
        'TablePrefix': TablePrefix,
        'TableName': table_name,
        'target_S3URI': target_S3URI,
        'from_athena': 'False',
        'filename': filename,
        'github_url': github_url
    }
}


with open(path_to_etl) as json_file:
    parameters = json.load(json_file)

# parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(0)

index_to_remove = next(
    (
        index
        for (index, d) in enumerate(parameters['TABLES']['CREATION']['ALL_SCHEMA'])
        if d['metadata']['filename'] == filename
    ),
    None,
)
if index_to_remove != None:
    parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(index_to_remove)

parameters['TABLES']['CREATION']['ALL_SCHEMA'].append(json_etl)

with open(path_to_etl, "w")as outfile:
    json.dump(parameters, outfile)
