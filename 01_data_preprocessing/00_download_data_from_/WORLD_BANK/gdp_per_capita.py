import wbdata
import pandas as pd
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from pathlib import Path
import os, shutil, json

#### Step 1 Download the GDP and GNI
#set up the indicator I want (just build up the dict if you want more than one)
indicators = {
'NY.GNP.PCAP.CD':'GNI per Capita',
'NY.GDP.PCAP.KD':'GDP per Capita'}
df = wbdata.get_dataframe(indicators).dropna()

meta_data =  pd.read_excel('http://wits.worldbank.org/data/public/WITSCountryProfile-Country_Indicator_ProductMetada-en.xlsx',
sheet_name="Country-Metadata")

meta_data.head()

df_final= (df
.reset_index()
.merge(meta_data, how = 'inner', left_on = ['country'], right_on = ['Country Name'])
.drop(columns = ['country', 'Country Name'])
.reindex(columns = ['Long Name', 'Country Code', 'Country ISO3', 'date',
'GNI per Capita', 'GDP per Capita', 'Income Group'])
)

df_final['Income Group'].unique()

#### Step 2: Save to S3: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/TRADE_DATA/RAW/WORLD_BANK/NY.GNP.PCAP.CD_NY.GDP.PCAP.KD
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)


name_credential = 'thomas_vat_credentials.csv'
region = 'eu-west-3'
bucket = 'chinese-data'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)

con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True)
df_final.to_csv('gdp_gni_per_capita.csv', index = False)

s3.upload_file('gdp_gni_per_capita.csv', 'TRADE_DATA/RAW/WORLD_BANK/NY.GNP.PCAP.CD_NY.GDP.PCAP.KD')

os.remove('gdp_gni_per_capita.csv')
