import wbdata
import pandas as pd
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import os, shutil, json, re

#### Step 2: Save to S3: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/TRADE_DATA/RAW/WORLD_BANK/NY.GNP.PCAP.CD_NY.GDP.PCAP.KD
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


input_path = os.path.join(parent_path,"00_data_catalog", "temporary_local_data", 'gdp_gni_per_capita.csv')
df_final.to_csv(input_path, index = False)

s3.upload_file(input_path, 'DATA/ECON/TRADE_DATA/RAW/WORLD_BANK/GDP_PER_CAPITA')
os.remove(input_path)

schema = [{'Name': 'country', 'Type': 'string', 'Comment': 'Country name'},
        {'Name': 'iso_alpha', 'Type': 'string', 'Comment': 'Country code'},
        {'Name': 'iso_alpha03', 'Type': 'string', 'Comment': 'Country code, iso 03'},
        {'Name': 'year', 'Type': 'string', 'Comment': 'Year'},
        {'Name': 'gni_per_capita', 'Type': 'float', 'Comment': "GDP per capita is gross domestic product divided by midyear population"},
        {'Name': 'gpd_per_capita', 'Type': 'float', 'Comment': "GNI per capita (formerly GNP per capita) is the gross national income, converted to U.S. dollars using the World Bank Atlas method, divided by the midyear population"},
        {'Name': 'income_group', 'Type': 'string', 'Comment': "One of 'Others', 'Low income', 'Upper middle income','High income: nonOECD', 'Lower middle income', 'High income: OECD'"}]

glue = service_glue.connect_glue(client=client)
target_S3URI = "s3://datalake-datascience/DATA/ECON/TRADE_DATA/RAW/WORLD_BANK/GDP_PER_CAPITA"
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "world_bank"
TablePrefix = 'world_bank_'


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
filename = 'gdp_per_capita.py'
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
    'description': 'Download GDP per capita from the World Bank Website',
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
