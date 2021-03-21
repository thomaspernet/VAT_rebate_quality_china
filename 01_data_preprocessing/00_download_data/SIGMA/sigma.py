import pandas as pd
import numpy as np
from pathlib import Path
import os, re,  requests, json
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-3'
bucket = 'datalake-datascience'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)

con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True)

auth = authorization_service.get_authorization(
    path_credential_gcp = os.path.join(parent_path, "creds", "service.json"),
    path_credential_drive = os.path.join(parent_path, "creds"),
    verbose = False#
)

gd_auth = auth.authorization_drive()
drive = connect_drive.drive_operations(gd_auth)

### Load from spreadsheet
sigmas = drive.upload_data_from_spreadsheet(
    sheetID = '1YLr4n2xLWKIxYftf8ODSMw6tsoiukLMxs1L5mopTDfk',
    sheetName = 'Sigmas',
	 to_dataframe = True)

input_path = os.path.join(parent_path,"00_data_catalog", "temporary_local_data", "sigmas_china.csv")
sigmas.to_csv(input_path, index = False)
pd.read_csv(input_path).head()

### Move to S3
s3.upload_file(file_to_upload = input_path,
destination_in_s3 = 'DATA/ECON/INDUSTRY/ADDITIONAL_DATA/SIGMAS_HS3')
os.remove(input_path)

schema = [{"Name": "ccode", "Type": "string", "Comment": "Country code"},
            {"Name": "cname", "Type": "string", "Comment": "countr name"},
            {"Name": "sigma", "Type": "float", "Comment": "sigma"},
            {"Name": "HS3", "Type": "string", "Comment": "industry code"}]

glue = service_glue.connect_glue(client=client)
target_S3URI = "s3://datalake-datascience/DATA/ECON/INDUSTRY/ADDITIONAL_DATA/SIGMAS_HS3"
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "chinese_trade"
TablePrefix = 'china_'


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
filename = 'sigma.py'
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
json_etl = {
    'description': 'Download sigma coefficient restricted to China',
    'schema': schema,
    'partition_keys': [],
    'metadata': {
        'DatabaseName': DatabaseName,
        'TablePrefix': TablePrefix,
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
