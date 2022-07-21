import pandas as pd
import numpy as np
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
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
PATH_S3 = "DATA/ECON/CHINA/CONCORDANCE/SECTORS/HS_GBT"
# GCP
auth = authorization_service.get_authorization(
    #path_credential_gcp=os.path.join(parent_path, "creds", "service.json"),
    path_credential_drive=os.path.join(parent_path, "creds"),
    verbose=False,
    scope=['https://www.googleapis.com/auth/spreadsheets.readonly',
           "https://www.googleapis.com/auth/drive"]
)
gd_auth = auth.authorization_drive(path_secret=os.path.join(
    parent_path, "creds", "credentials.json"))
drive = connect_drive.drive_operations(gd_auth)

# DOWNLOAD DATA TO temporary_local_data folder
FILENAME_DRIVE = 'HS-CIC.dta'
FILEID = drive.find_file_id(FILENAME_DRIVE, to_print=False)

var = (
    drive.download_file(
        filename=FILENAME_DRIVE,
        file_id=FILEID,
        local_path=os.path.join(parent_path, "00_data_catalog", "temporary_local_data"))
)

# READ DATA
input_path = os.path.join(parent_path, "00_data_catalog",
                          "temporary_local_data", FILENAME_DRIVE)
input_path_save = os.path.join(parent_path, "00_data_catalog",
                          "temporary_local_data", FILENAME_DRIVE.split('.')[0] + ".csv")

# preprocess data
var = (
    pd.read_stata(input_path)
    .replace(',', '', regex=True)
    .assign(
        hs96=lambda x: np.where(x['hs96'].isin([np.nan]),
                                np.nan,
                                x['hs96'].astype('Int64').astype(
                                    'str').str.zfill(6)
                                ),
        hs2002=lambda x: np.where(x['hs2002'].isin([np.nan]),
                                  np.nan,
                                  x['hs2002'].astype('Int64').astype(
                                      'str').str.zfill(6)
                                  ),
        cic_adj=lambda x: np.where(x['cic_adj'].isin([np.nan]),
                                   np.nan,
                                   x['cic_adj'].astype('Int64').astype(
                                       'str').str.zfill(4)
                                   ),
        cic03=lambda x: np.where(x['cic03'].isin([np.nan]),
                                 np.nan,
                                 x['cic03'].astype('Int64').astype(
                                     'str').str.zfill(4)
                                 ),
        cic02=lambda x: np.where(x['cic02'].isin([np.nan]),
                                 np.nan,
                                 x['cic02'].astype('Int64').astype(
                                     'str').str.zfill(4)
                                 ),
    )
)

var.to_csv(input_path_save, index=False)
# SAVE S3
s3.upload_file(input_path_save, PATH_S3)
# os.remove(input_path)

# ADD SHCEMA
#for i in pd.io.json.build_table_schema(var)['fields']:
#    if i['type'] in ['number', 'integer']:
#        i['type'] = 'int'
#    print("{},".format({'Name':i['name'], 'Type':i['type'],'Comment':''}))
schema = [
{'Name': 'hs96', 'Type': 'string', 'Comment': 'HS 96'},
{'Name': 'hs2002', 'Type': 'string', 'Comment': 'HS 2002'},
{'Name': 'cic_adj', 'Type': 'string', 'Comment': 'CIC adjusted No idea how'},
{'Name': 'cic03', 'Type': 'string', 'Comment': 'CIC 2003'},
{'Name': 'cic02', 'Type': 'string', 'Comment': 'CIC 2002'},

]

# ADD DESCRIPTION
description = 'The concordance table between GBT sectors and HS6 products comes from Upward et al. (2013)'

glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://", bucket, PATH_S3)
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "industry"
TablePrefix = 'industry_'  # add "_" after prefix, ex: hello_


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
filename = 'concordance_hs_gbt.py'
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
