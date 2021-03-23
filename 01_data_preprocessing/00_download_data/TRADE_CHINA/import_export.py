import pandas as pd
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from pathlib import Path
import os, re
import json
from tqdm import tqdm

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
PATH_S3 = "DATA/ECON/TRADE_DATA/CHINA/IMPORT_EXPORT"  ### Copy destination in S3 without bucket and "/" at the end
#os.remove(input_path)

### ADD SHCEMA
schema = [
            {"Name": "date", "Type": "string", "Comment": "year trade recorded"},
            {"Name": "ID", "Type": "string", "Comment": "firm ID"},
            {"Name": "business_type", "Type": "string", "Comment": "business trade type"},
            {"Name": "intermediate", "Type": "string", "Comment": "company name includes the following 进出口, 贸易, 外贸,外经, 经贸, 工贸, 科贸, then there are intermediates firms"},
            {"Name": "trade_type", "Type": "string", "Comment": "Trade type including ordianry or processing"},
            {"Name": "province", "Type": "string", "Comment": "Province name"},
            {"Name": "city_prod", "Type": "string", "Comment": "production site city name"},
            {"Name": "matching_city", "Type": "string", "Comment": "matching name"},
            {"Name": "imp_exp", "Type": "string", "Comment": "import or export"},
            {"Name": "hs", "Type": "string", "Comment": "HS6 6 digit"},
            {"Name": "origin_or_destination", "Type": "string", "Comment": "source or destination country"},
            {"Name": "value", "Type": "int", "Comment": "import or export value"},
            {"Name": "quantities", "Type": "int", "Comment": "import or export quantity"},
]

### ADD DESCRIPTION
description = 'Create import export china'

glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://datalake-datascience", PATH_S3)
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "chinese_trade"
TablePrefix = 'china_' ## add "_" after prefix, ex: hello_


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
filename = 'import_export.py'
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
