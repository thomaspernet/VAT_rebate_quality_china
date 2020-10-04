import pandas as pd
from pathlib import Path
import os, re,  requests, json
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3

#### Step 1: Connect Google Drive
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
project = 'valid-pagoda-132423'

auth = authorization_service.get_authorization(
    path_credential_gcp = "{}/creds/service.json".format(parent_path),
    path_credential_drive = "{}/creds".format(parent_path),
    verbose = False#
)

gd_auth = auth.authorization_drive()
drive = connect_drive.drive_operations(gd_auth)



hs6_homo = (drive.upload_data_from_spreadsheet(
    sheetID = '1-TbLM4IkK5fHJYsZMcAnoxuYJE8yxfLRxhNtwVVCpLk',
    sheetName = 'hs_hom_dif.csv',
	 to_dataframe = True)
     .assign( classification = 'HETEROGENEOUS')
     )

#### Step 2: Move to S3: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/TRADE_DATA/RAW/GOODS_CLASSIFICATION/HOMOGENEOUS/?region=eu-west-3&tab=overview
name_credential = 'thomas_vat_credentials.csv'
region = 'eu-west-3'
bucket = 'chinese-data'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)

con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True)

hs6_homo.to_csv('hs_hom_dif.csv', index = False)
s3.upload_file(file_to_upload = 'hs_hom_dif.csv',
destination_in_s3 = 'TRADE_DATA/RAW/GOODS_CLASSIFICATION/HOMOGENEOUS')

os.remove('hs_hom_dif.csv')
