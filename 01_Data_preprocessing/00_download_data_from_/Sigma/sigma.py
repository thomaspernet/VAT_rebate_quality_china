import pandas as pd
import numpy as np
from pathlib import Path
import os, re,  requests, json
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
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

### Load from spreadsheet
sigmas = drive.upload_data_from_spreadsheet(
    sheetID = '1YLr4n2xLWKIxYftf8ODSMw6tsoiukLMxs1L5mopTDfk',
    sheetName = 'Sigmas',
	 to_dataframe = True)

### Move to S3
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3

name_credential = 'thomas_vat_credentials.csv'
region = 'eu-west-3'
bucket = 'chinese-data'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)

con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True)

sigmas.to_csv('sigmas.csv', index = False)
s3.upload_file(file_to_upload = 'sigmas.csv',
destination_in_s3 = 'ADDITIONAL_DATA/SIGMAS_HS3')

os.remove('sigmas.csv')
