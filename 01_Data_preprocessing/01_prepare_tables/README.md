# Data Preprocessing

The folder `01_prepare_tables` should contain one or more notebooks to prepape the raw data before to get them ready for the creation of the final table(s).



Preparing the raw data is never an easy task. Many circumstances can lead to a wrong start breaking all the pipeline. Therefore, the data scientist can find a subfolder named `00_POC_prepare_tables` where she/he can make some tests on how to prepare correctly the data before generating clean and documented notebooks in this folder (i.e. `01_prepare_tables`)



## Parameter json template

All queries to prepare and transform the data should be saved in a JSON. The template is the following:

```
{
   "GLOBAL":{
      "DATABASE":"",
      "QUERIES_OUTPUT":""
   },
   "TABLES":{
      "CREATION":{
         "template":{
            "top":"CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} (",
            "middle":"",
            "bottom":"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'serialization.format' = ',', 'field.delim' = '{0}') LOCATION '{1}' TBLPROPERTIES ('has_encrypted_data'='false')"
         },
         "ALL_SCHEMA":[
            {
               "database":"",
               "name":"",
               "query":"",
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
```



## Template creation table

```
QUERY = """CREATE EXTERNAL TABLE IF NOT EXISTS DB.TABLE (
    
`VAR_1` string,
`VAR_2` string,
`VAR_3` string

     )
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     WITH SERDEPROPERTIES (
      'serialization.format' = ',',
      'field.delim' = ',') 
     LOCATION 's3://BUCKETNAME/PATH'
     TBLPROPERTIES ('has_encrypted_data'='false');"""
```