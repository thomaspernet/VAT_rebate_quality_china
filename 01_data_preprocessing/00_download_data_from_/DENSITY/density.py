import pandas as pd
from pathlib import Path
import os, re,  requests, json, shutil
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

drive.download_file(
filename = 'density.Rda',
file_id = '1omt1Yc9i4g2Ch26f9DjJw2PRS-Ea6V5e',
local_path = '../../../00_Data_catalogue/temporary_local_data'
)

#### Step 2: Move to S3: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/ADDITIONAL_DATA/DENSITY/?region=eu-west-3&tab=overview

name_credential = 'thomas_vat_credentials.csv'
region = 'eu-west-3'
bucket = 'chinese-data'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)

con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True)

s3.upload_file(file_to_upload = '../../../00_Data_catalogue/temporary_local_data/density.Rda',
destination_in_s3 = 'ADDITIONAL_DATA/DENSITY')
