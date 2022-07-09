import zipfile
import pandas as pd
import numpy as np
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
import requests
from pathlib import Path
import os
import re
import json
from tqdm import tqdm

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
PATH_S3 = "DATA/ECON/TRADE_DATA/NON_TARIFF_MEASURES/WIIW_NTM"
# DOWNLOAD DATA TO temporary_local_data folder

url = 'https://wiiw.ac.at/files/data/set/ds-0002-02-ntm_csv.zip'
r = requests.get(url, allow_redirects=True)
open('ds-0002-02-ntm_csv.zip', 'wb').write(r.content)
with zipfile.ZipFile('ds-0002-02-ntm_csv.zip', 'r') as zip_ref:
    zip_ref.extractall('ds-0002-02-ntm_csv')

# READ DATA: Two dataset
def convert_to_text(x):
    try:
        x = "|".join(x.tolist())
    except:
        x = np.nan
    return x
# preprocess data
var = (
    pd.read_csv('ds-0002-02-ntm_csv/NTM_Notifications_1995_2019.csv', dtype={
        'ID': 'str',
        'STC': 'str',
        'Date_Inforce': 'str',
        'Date_Withdrawal': 'str',
        'Year_Initiation': 'str',
        'Year_Inforce': 'str',
        'Year_Withdrawal': 'str'
    })
    .replace(',', '', regex=True)
    .replace('#N/A|#VALUE!|\?\?\?', np.nan, regex=True)
    .replace('', np.nan)
    .replace('\n', ' ', regex=True)
    .replace('NAN', np.nan)
)
var.columns = (var.columns
               .str.strip()
               .str.replace(' ', '_')
               .str.replace('-', '_')
               .str.replace('.', '', regex=True)
               .str.lower()
               .str.replace('\n', '')
               .str.replace("\%", '_pct', regex=True)
               .str.replace('_#', '')
               .str.replace('\_\(', '_', regex=True)
               .str.replace('\)', '', regex=True)
               .str.replace('_\+_', '', regex=True)
               .str.replace('\/', '_', regex=True)
               .str.replace('__', '_')
               )

var = var.assign(**{
    "{}".format(col): var[col]
    .astype(str)
    .str.replace('\,', '|', regex=True)
    .str.replace(r'\/|\(|\)|\?|\.|\:|\-', '', regex=True)
    .str.replace('__', '_')
    .str.replace('\\n', '', regex=True)
    .str.replace('\"', '', regex=True)
    # .str.upper()
    for col in var.select_dtypes(include="object").columns
}
).replace('NAN', np.nan)

# We need to add the HS6
var = (
    var
    .merge(
        (
            pd.read_csv('ds-0002-02-ntm_csv/NTM_HS1996.csv', dtype={
                'ID': 'str', 'HS1996': 'str', 'HScombined': 'str'
            }).rename(columns={'ID': 'id', 'HS1996': 'hs1996', 'HScombined': 'hs_combined'})
            .groupby('id')
            .agg({'hs1996': 'unique', 'hs_combined': 'unique'})
            .reset_index()
        ), how='left'
    )
    .assign(
    hs1996 = lambda x: x.apply(
    lambda x:
    convert_to_text(x['hs1996']),axis =1),
    hs_combined = lambda x: x.apply(
    lambda x:
    convert_to_text(x['hs_combined']),axis =1)
    )
)


var.to_csv("wiif_non_tariff_measures.csv", index=False)
# SAVE S3
s3.upload_file("wiif_non_tariff_measures.csv", PATH_S3)
import shutil
shutil.rmtree('ds-0002-02-ntm_csv')
os.remove('ds-0002-02-ntm_csv.zip')
os.remove('wiif_non_tariff_measures.csv')
# ADD SHCEMA
# for i in pd.io.json.build_table_schema(var)['fields']:
#    if i['type'] in ['number', 'integer']:
#        i['type'] = 'int'
#    print("{},".format({'Name':i['name'], 'Type':i['type'],'Comment':''}))
schema = [
{'Name': 'id', 'Type': 'string', 'Comment': 'ID of NTM Notifications, linking the file to HS codes'},
{'Name': 'imp', 'Type': 'string', 'Comment': 'Name of the NTM-imposing country'},
{'Name': 'imp_iso3', 'Type': 'string', 'Comment': 'ISO3 country codes of the NTM-imposing country'},
{'Name': 'aff', 'Type': 'string', 'Comment': 'Name of the country affected by the NTM'},
{'Name': 'aff_iso3', 'Type': 'string', 'Comment': 'ISO3 country codes of the affected country'},
{'Name': 'ntm', 'Type': 'string', 'Comment': 'NTM types covered'},
{'Name': 'sub', 'Type': 'string', 'Comment': 'Sub-requirements: e.g. price-based or volume-based measure, specific or ad valorem tariff increase etc'},
{'Name': 'stc', 'Type': 'string', 'Comment': ''},
{'Name': 'date_initiation', 'Type': 'string', 'Comment': 'Year or date of (i) the initiation of a measure, (ii) its entry into force and (iii) its withdrawal'},
{'Name': 'date_inforce', 'Type': 'string', 'Comment': 'Year or date of (i) the initiation of a measure, (ii) its entry into force and (iii) its withdrawal'},
{'Name': 'date_withdrawal', 'Type': 'string', 'Comment': 'Year or date of (i) the initiation of a measure, (ii) its entry into force and (iii) its withdrawal'},
{'Name': 'year_initiation', 'Type': 'string', 'Comment': 'Year or date of (i) the initiation of a measure, (ii) its entry into force and (iii) its withdrawal'},
{'Name': 'year_inforce', 'Type': 'string', 'Comment': 'Year or date of (i) the initiation of a measure, (ii) its entry into force and (iii) its withdrawal'},
{'Name': 'year_withdrawal', 'Type': 'string', 'Comment': 'Year or date of (i) the initiation of a measure, (ii) its entry into force and (iii) its withdrawal'},
{'Name': 'doc', 'Type': 'string', 'Comment': 'References to WTO documents'},
{'Name': 'hsorigin', 'Type': 'string', 'Comment': 'Information on how the product code was retrieved (step of the imputation procedure)'},
{'Name': 'dborigin', 'Type': 'string', 'Comment': 'WTO I-TIP or TTBD'},
{'Name': 'measuredesc', 'Type': 'string', 'Comment': 'Description of the measure'},
{'Name': 'proddesc', 'Type': 'string', 'Comment': 'Description of the affected products'},
{'Name': 'keywords', 'Type': 'string', 'Comment': 'Indicated keywords '},
{'Name': 'id_ttbd', 'Type': 'string', 'Comment': 'Case ID used in the TTBD database'},
{'Name': 'hs1996', 'Type': 'string', 'Comment': 'HS_combined converted to HS revision 1996'},
{'Name': 'hs_combined', 'Type': 'string', 'Comment': '6-digit product code of the Harmonised System implied by the given raw HS code'},
]

# ADD DESCRIPTION
description = 'The wiiw NTM Database constitutes a data compilation of notifications of non-tariff measures (NTM) to the WTO, publicly accessible via the Integrated Trade Intelligence Portal'

glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://", bucket, PATH_S3)
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "trade"
TablePrefix = 'wto_'  # add "_" after prefix, ex: hello_


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
filename = 'non_tarrif_measure.py'
path_to_etl = os.path.join(
    str(Path(path).parent.parent.parent), 'utils', 'parameters_ETL_VAT_rebate_quality_china.json')
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
