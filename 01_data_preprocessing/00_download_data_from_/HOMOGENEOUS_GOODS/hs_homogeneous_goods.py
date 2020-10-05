import pandas as pd
from pathlib import Path
import os, re,  requests, json
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3

### Code to get the classification
# SELECT DISTINCT(hs6) FROM quality_vat_export_covariate_2003_2010
# IN R
# library(concordance)
# hs6 <- read_csv('hs6.csv', col_types = "c") %>%
# mutate(goods=sapply(hs6, function(x) { get_proddiff(sourcevar = x, "hs", prop = "w") })) %>%
# mutate(homogeneous = ifelse(goods > 0.5, "HOMOGENEOUS", "HETEREGENEOUS"))
# head(hs6)
# write.csv(hs6, 'homogeneous.csv')

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
    sheetID = '1LLKUCueN0l2eTNEyQz4EQ1m5fauRXHAPjdIGxV_LJ-c',
    sheetName = 'homogeneous.csv',
	 to_dataframe = True)
     .iloc[:, 1:]
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

hs6_homo.to_csv('homogeneous.csv', index = False)
s3.upload_file(file_to_upload = 'homogeneous.csv',
destination_in_s3 = 'TRADE_DATA/RAW/GOODS_CLASSIFICATION/HOMOGENEOUS')

os.remove('homogeneous.csv')
