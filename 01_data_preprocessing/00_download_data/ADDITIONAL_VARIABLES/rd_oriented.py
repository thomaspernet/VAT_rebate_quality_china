import pandas as pd
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
from pathlib import Path
import os, re
import json
from tqdm import tqdm
import numpy as np
### Connect to Cloud providers
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-3'
bucket = 'datalake-datascience'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)

#### AWS
con = aws_connector.aws_instantiate(credential=path_cred,
                                    region=region)
client = con.client_boto()
s3 = service_s3.connect_S3(client=client,
                           bucket=bucket, verbose=True)
PATH_S3 = "DATA/ECON/LOOKUP_DATA/CHINA/INDUSTRY_CHARACTERISTICS/RD_ORIENTED"  ### Copy destination in S3 without bucket and "/" at the end
### GCP
auth = authorization_service.get_authorization(
    #path_credential_gcp=os.path.join(parent_path, "creds", "service.json"),
    path_credential_drive=os.path.join(parent_path, "creds"),
    verbose=False,
    #scope = ['https://www.googleapis.com/auth/spreadsheets.readonly',
    #"https://www.googleapis.com/auth/drive"]
)
gd_auth = auth.authorization_drive(path_secret = os.path.join(parent_path, "creds", "credentials.json"))
drive = connect_drive.drive_operations(gd_auth)

### DOWNLOAD DATA TO temporary_local_data folder
FILENAME_DRIVE = 'test_rd'
FILEID = drive.find_file_id(FILENAME_DRIVE + ".dta", to_print=False)

var = (
    drive.download_file(
        filename=FILENAME_DRIVE+ ".dta",
        file_id=FILEID,
        local_path=os.path.join(parent_path,"00_data_catalog", "temporary_local_data"))
)

### READ DATA
input_path = os.path.join(parent_path,"00_data_catalog", "temporary_local_data")
(
pd.read_stata(os.path.join(input_path, FILENAME_DRIVE + ".dta"))
.assign(rd_oriented = lambda x: np.where(
x['h_rd_o'], "RD_ORIENTED", "NO_RD_ORIENTED")
)
.loc[lambda x: x['rd_oriented'].isin(['RD_ORIENTED'])]
.assign(
hs4 = lambda x: np.where(
x['gbt'].apply(lambda x: len(str(x))) <4, '0' + x['gbt'].astype('str'), x['gbt'].astype('str')
))
.reindex(columns = ['hs4', 'rd_oriented'])
.to_csv(os.path.join(input_path, FILENAME_DRIVE  + ".csv"), index = False)
)

#### SAVE S3
s3.upload_file(os.path.join(input_path, FILENAME_DRIVE  + ".csv"), PATH_S3)
#os.remove(input_path)

### ADD SHCEMA
schema = [{"Name":"hs4", "Type":"string", "Comment":"HS6 6 digits"},
{"Name":"rd_oriented", "Type":"string", "Comment":"Industry rd oriented"}
]

### ADD DESCRIPTION
description = 'List industry HS4 rd oriented'

glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://datalake-datascience", PATH_S3)
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "chinese_lookup"
TablePrefix = 'industry_' ## add "_" after prefix, ex: hello_


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
filename = 'energy.py'
path_to_etl = os.path.join(str(Path(path).parent.parent.parent), 'utils','parameters_ETL_VAT_rebate_quality_china.json')
with open(path_to_etl) as json_file:
    parameters = json.load(json_file)
github_url = os.path.join(
    "https://github.com/",
    parameters['GLOBAL']['GITHUB']['owner'],
    parameters['GLOBAL']['GITHUB']['repo_name'],
    re.sub(parameters['GLOBAL']['GITHUB']['repo_name'],
           '', re.sub(
               r".*(?={})".format(parameters['GLOBAL']['GITHUB']['repo_name'])
               , '', path))[1:],
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
        'github_url':github_url
    }
}


with open(path_to_etl) as json_file:
    parameters = json.load(json_file)

#parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(0)

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
