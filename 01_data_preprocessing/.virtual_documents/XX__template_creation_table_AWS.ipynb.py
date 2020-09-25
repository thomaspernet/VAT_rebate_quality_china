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


name_credential = 'XXX_credentials.csv'
region = ''
bucket = ''
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
      "DATABASE":"",
      "QUERIES_OUTPUT":""
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
               "database":"",
               "name":"",
               "output_id":"",
               "separator":",",
               "s3URI":"",
               "schema":{
                  "variables":[
                      
                     
                  ],
                  "format":[
 
                     
                  ],
                  "comments":[
                     
                  ]
               }
            }
         ]
      }
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


print(table_middle)


import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp


def create_report(extension = "html", keep_code = False):
    """
    Create a report from the current notebook and save it in the 
    Report folder (Parent-> child directory)
    
    1. Exctract the current notbook name
    2. Convert the Notebook 
    3. Move the newly created report
    
    Args:
    extension: string. Can be "html", "pdf", "md"
    
    
    """
    
    ### Get notebook name
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split('-', 1)[0].split('.')[0]

    for srv in notebookapp.list_running_servers():
        try:
            if srv['token']=='' and not srv['password']:  
                req = urllib.request.urlopen(srv['url']+'api/sessions')
            else:
                req = urllib.request.urlopen(srv['url']+ \
                                             'api/sessions?token=' + \
                                             srv['token'])
            sessions = json.load(req)
            notebookname = sessions[0]['name']
        except:
            pass  
    
    sep = '.'
    path = os.getcwd()
    #parent_path = str(Path(path).parent)
    
    ### Path report
    #path_report = "{}/Reports".format(parent_path)
    #path_report = "{}/Reports".format(path)
    
    ### Path destination
    name_no_extension = notebookname.split(sep, 1)[0]
    source_to_move = name_no_extension +'.{}'.format(extension)
    dest = os.path.join(path,'Reports', source_to_move)
    
    ### Generate notebook
    if keep_code:
        os.system('jupyter nbconvert --to {} {}'.format(
    extension,notebookname))
    else:
        os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))


create_report(extension = "html")
