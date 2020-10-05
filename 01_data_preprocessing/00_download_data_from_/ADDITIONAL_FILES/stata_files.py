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

name_credential = 'thomas_vat_credentials.csv'
region = 'eu-west-3'
bucket = 'chinese-data'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)

con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True)

for files in ['energy.dta', 'high_tech.dta', 'test_es.dta', 'test_rd.dta']:

    drive.download_file(
    filename = files,
    file_id = None,
    local_path = '../../../00_Data_catalogue/temporary_local_data'
    )

    path = os.path.join('../../../00_Data_catalogue/temporary_local_data', files)
    path_output = os.path.join('../../../00_Data_catalogue/temporary_local_data','{}.csv'.format(os.path.splitext(files)[0]))

    if os.path.splitext(files)[1] == '.dta':
        pd.read_stata(path).to_csv(path_output, index = False)


    s3.upload_file(file_to_upload = path_output,
    destination_in_s3 = 'ADDITIONAL_DATA/INDUSTRY_CHARACTERISTICS')
    os.remove(path)
    os.remove(path_output)
