# Data Preprocessing

The data preparation is composed by three steps:

1. Download the raw data
   - Folder `00_download_data_from_`
2. Prepare the raw data
   1. Folder `01_prepare_tables` 
      1. A subfolder `00_POC_prepare_tables` is available to make some tests before finalizing the preparation of the raw data
3. Prepare the data for the model
   1. Folder `02_prepare_tables_model` 



The ETL makes use of a JSON file with three keys, one for the creation of tables, one for the transformation of tables and a last key for analysing the tables. This JSON file is used to bring the queries to the ETL, say differently in production. All queries in the JSON files are made to be deployed in the workflow. 

Before to push the queries to the JSON files, we strongly recommend you to create and validate the queries in notebooks saved in the subfolder `00_POC_prepare_tables`. Each notebook is referenced by an US. Therefore, there is a first batch of US meant to design and validate the queries. A second batch of US has the objective of bringing the queries to the JSON files (i.e. the ETL). These notebooks will be in the root directory, not in the POC subdirectory. 



Each child folder has a folder named `Reports` to host HTML report from the notebook. It is recommended to saved the notebook in `md` format for versioning purpose, reset the notebook to avoid overload the notebook and generate an HTML (with or without the code) to ease the reading.



# Data Preparation Workflow

