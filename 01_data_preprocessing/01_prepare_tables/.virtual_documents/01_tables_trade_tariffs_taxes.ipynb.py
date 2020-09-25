from awsPy.aws_authorization import aws_connector
from awsPy.aws_athena import service_athena
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil, json

path = os.getcwd()
parent_path = str(Path(path).parent.parent)


name_credential = 'thomas_vat_credentials.csv'
region = 'eu-west-3'
bucket = 'chinese-data'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)


con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True) 


pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)


parameters = {
   "GLOBAL":{
      "DATABASE":"chinese_trade",
      "QUERIES_OUTPUT":"SQL_OUTPUT_ATHENA"
   },
   "TABLES":{
      "CREATION":{
         "template":{
            "top":"CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (",
            "middle":"{0} {1} {2}",
            "bottom":"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'serialization.format' = ',', 'field.delim' = '{0}') LOCATION '{1}' TBLPROPERTIES ('has_encrypted_data'='false', 'skip.header.line.count'='1')"
         },
         "ALL_SCHEMA":[
            {
               "database":"chinese_trade",
               "name":"import_export",
               "output_id":"",
               "separator":",",
               "s3URI":"s3://chinese-data/TRADE_DATA/TRANSFORMED/",
               "schema":{
                  "variables":[
                     "date",
                     "ID",
                     "business_type",
                     "intermediate",
                     "trade_type",
                     "province",
                     "city_prod",
                     "matching_city",
                     "imp_exp",
                     "hs",
                     "origin_or_destination",
                     "values",
                     "quantities"
                  ],
                  "format":[
                     "string",
                     "string",
                     "string",
                     "string",
                     "string",
                     "string",
                     "string",
                     "string",
                     "string",
                     "string",
                     "string",
                     "int",
                     "int"
                  ],
                  "comments":[
                     
                  ]
               }
            },
            {
               "database":"chinese_trade",
               "name":"base_hs6_VAT_2002_2012",
               "output_id":"",
               "separator":",",
               "s3URI":"s3://chinese-data/TAX_DATA/TRANSFORMED/VAT_REBATE/",
               "schema":{
                  "variables":[
                     "hs6",
                     "year",
                     "tax_rebate",
                     "ln_vat_rebate",
                     "vat_m",
                     "vat_reb_m"
                  ],
                  "format":[
                     "string",
                     "string",
                     "float",
                     "float",
                     "float",
                     "float"
                  ],
                  "comments":[
                     
                  ]
               }
            },
            {
               "database":"chinese_trade",
               "name":"applied_mfn_tariffs_hs02_china_2002_2010",
               "output_id":"",
               "separator":",",
               "s3URI":"s3://chinese-data/TAX_DATA/TRANSFORMED/APPLIED_MFN_TARIFFS/",
               "schema":{
                  "variables":[
                     "reporter",
                     "year",
                     "import_tax",
                     "HS02"
                  ],
                  "format":[
                     "string",
                     "string",
                     "float",
                     "string"
                  ],
                  "comments":[
                     
                  ]
               }
            },
             {
               "database":"chinese_trade",
               "name":"sigma_industry",
               "output_id":"",
               "separator":",",
               "s3URI":"s3://chinese-data/ADDITIONAL_DATA/SIGMAS_HS3/",
               "schema":{
                  "variables":[
                      "ccode", "cname", "sigma", "HS3"
                  ],
                  "format":[
                      "string", "string", "float", "string"
                  ],
                  "comments":[
                     'Country code', 'countr name', 'sigma', 'industry code'
                  ]
               }
          } 
         ]
      },
      "PREPARATION":[
         {
            "STEPS_XX":{
               "query":{
                  "top":{
                     
                  },
                  "middle":{
                     
                  },
                  "bottom":{
                     
                  }
               }
            },
            "output_id":[
               
            ]
         }
      ]
   }
}


json_filename ='parameters_ETL.json'
json_file = json.dumps(parameters)
f = open(json_filename,"w")
f.write(json_file)
f.close()
s3.upload_file(json_filename, 'DATA/ETL')


s3.download_file(key = 'DATA/ETL/parameters_ETL.json')
with open('parameters_ETL.json', 'r') as fp:
    parameters = json.load(fp)
print(parameters)


s3_output = parameters['GLOBAL']['QUERIES_OUTPUT']
bd = parameters['GLOBAL']['DATABASE']


for key, value in parameters["TABLES"]["CREATION"].items():
    if key == "ALL_SCHEMA":
        for table_info in value:

            ## CREATE QUERY

            ### Create top/bottom query
            table_top = parameters["TABLES"]["CREATION"]["template"]["top"].format(
                table_info["database"], table_info["name"]
            )
            table_bottom = parameters["TABLES"]["CREATION"]["template"][
                "bottom"
            ].format(table_info["separator"], table_info["s3URI"])

            ### Create middle
            table_middle = ""
            for i, val in enumerate(table_info["schema"]["variables"]):
                if i == len(table_info["schema"]["variables"]) - 1:
                    table_middle += parameters["TABLES"]["CREATION"]["template"][
                        "middle"
                    ].format(val, table_info["schema"]["format"][i], ")")
                else:
                    table_middle += parameters["TABLES"]["CREATION"]["template"][
                        "middle"
                    ].format(val, table_info["schema"]["format"][i], ",")
            query = table_top + table_middle + table_bottom
            ## DROP IF EXIST
        
            s3.run_query(
                    query="DROP TABLE {}".format(table_info["name"]),
                    database=bd,
                    s3_output=s3_output
            )

            ## RUN QUERY
            output = s3.run_query(
                query=query,
                database=bd,
                s3_output=s3_output,
                filename=None,  ## Add filename to print dataframe
                destination_key=None,  ### Add destination key if need to copy output
            )
            ## SAVE QUERY ID
            table_info['output_id'] = output['QueryID']
            print(output)


query = """
SELECT *
FROM import_export
LIMIT 10
"""
s3.run_query(
            query=query,
            database=bd,
            s3_output=s3_output,
            filename='chinese_import_export',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )


query = """
SELECT COUNT(*) AS CNT
FROM import_export
"""
s3.run_query(
            query=query,
            database=bd,
            s3_output=s3_output,
            filename='count_chinese_import_export',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )


query = """
SELECT *
FROM base_hs6_VAT_2002_2012
LIMIT 10
"""
s3.run_query(
            query=query,
            database=bd,
            s3_output=s3_output,
            filename='chinese_base_hs6_VAT_2002_2012',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )


query = """
SELECT *
FROM applied_mfn_tariffs_hs02_china_2002_2010
LIMIT 10
"""
s3.run_query(
            query=query,
            database=bd,
            s3_output=s3_output,
            filename='chinese_applied_mfn_tariffs_hs02_china_2002_2010',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )


query = """
SELECT *
FROM sigma_industry
LIMIT 10
"""
s3.run_query(
            query=query,
            database=bd,
            s3_output=s3_output,
            filename='chinese_sigma_industry',  ## Add filename to print dataframe
            destination_key=None,  ### Add destination key if need to copy output
        )
