- # VAT_rebate_quality_china

  Describe your project here

  ## Table of Content VAT_rebate_quality_china

  - [Documentation](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/Documentation)
  - [Data Catalogue](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue)
  - [Data preprocessing](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing)
    - [00_download_from_ftp](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_from_ftp)
    - [01_prepare_tables](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/01_prepare_tables)
      - [00_POC_prepare_tables](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/01_prepare_tables/00_POC_prepare_tables)
      - [Reports](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/01_prepare_tables/Reports)
      - [Archive](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/01_prepare_tables/Archive)
    - [02_prepare_tables_model](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_prepare_tables_model)
      - [00_POC_prepare_tables_model](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_prepare_tables_model/00_POC_prepare_tables_model)
      - [Reports](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_prepare_tables_model/Reports)
      - [Archive](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_prepare_tables_model/Archive)
  - [Data Analysis](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis)
    - [00_analysis](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/00_analysis)
      - [00_analysis_XX_POC](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/00_analysis_XX_POC)
    - [01_model_train_evaluate](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/01_model_train_evaluate)
      - [00_model_XX_POC](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/01_model_train_evaluate/00_model_XX_POC)
      - [final_results](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/01_model_train_evaluate/final_results)
      - [Reports](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/01_model_train_evaluate/Reports)
      - [Docker](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/01_model_train_evaluate/Docker)
      - [Python_scripts](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/01_model_train_evaluate/Python_scripts)
    - [02_model_train_deploy](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/02_model_train_deploy)
      - [Docker](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/02_model_train_deploy/Docker)
    - [03_model_prediction](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/03_model_prediction)
  - [Production](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/03_production)
    - [container](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/03_production/container)
  - [04_lambda_function](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/04_lambda_function)
    - [Layers](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/04_lambda_function/Layers)
    - [00_FUNCTION_1](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/04_lambda_function/00_FUNCTION_1)


  The S3 architecture has the following structure:

  ```
  S3_ARCHITECTURE
      ├── ALGORITHM
      │   ├── EVALUATION
      │   ├── PYTHON_SCRIPT
      │   └── YYYYMMDD
      │       ├── ALGO_NAME
      │       │   └── MODEL
      │       ├── DATA
      │       │   ├── EVALUATION
      │       │   │   ├── RAW
      │       │   │   └── TRANSFORM
      │       │   ├── PREDICT
      │       │   │   ├── LIST_CLASS_POSITIVE
      │       │   │   └── RAW
      │       │   └── TRAIN
      │       │       ├── RAW
      │       │       └── TRANSFORM
      │       └── EVALUATION
      ├── ANALYSIS
      ├── CREDENTIALS
      ├── DATA
      │   ├── ADDITIONAL_FILES
      │   ├── ETL
      │   ├── RAW_DATA
      │   ├── UNZIP_DATA
      │   └── UNZIP_DATA_APPEND_ALL
      ├── LAMBDA_LAYERS
      ├── LOGS_FTP_EXECUTION
      ├── LOGS_QUERY_EXECUTION
      ├── NOTEBOOKS_WORKFLOW
      │   └── OUTPUT
      └── SQL_OUTPUT_ATHENA
  ```

  - `ALGORITHM` : training and evaluation data/models

    - `EVALUATION` : `evaluation_YYYYMMDD.json` -> logs of the training. This file can be used to trigger the prediction notebook

    - `PYTHON_SCRIPT`: Host the Python scripts used in Sagemaker to train, evaluate and predict

    - `YYYYMMDD`: Host the models training. Used if the model is retrained on a daily or monthly basis

      - `ALGO_NAME`: Rename the folder according to the algorithm trained. For Ensemble, use `ENSEMBLE` 

        - `MODEL`: store the pickle or sav files containing the steps to trained the model (preprocessing, training, etc)
          - Can store as many files as there are models

      - `DATA`: Host the training, evaluation and prediction data

        - `TRAIN`: Store the training data

          - `RAW`: Upload and save the raw data 
          - `TRANSFORM`: [Optional] Save the transformed data (post processing) if needed
    - `EVALUATION`: Store the evaluation data
      - `RAW`: Upload and save the raw data 
      - `TRANSFORM`: [Optional] Save the transformed data (post processing) if needed
        - `PREDICT`: Store the prediction data
          - `RAW`: Upload and save the raw data 
          - `TRANSFORM`: [Optional] Save the transformed data (post processing) if needed
          - `LIST_CLASS_POSITIVE`: [Optional] Save the positive class as csv or JSON if needed
  - `ANALYSIS`: [Optional] Store any analysis usefull for the project.
  - `CREDENTIALS`: [Optional] Store IAM or other credentials. It is used by Sagemaker during the discovery and analysis phases. 
  - `DATA`: Store the raw and prepared data so that Athena (or other services) can parse it
    - `ETL`: Store the parameters file -> it's a JSON file containing all the parameters and queries to run the ETL properly. Sagemaker will use this file to run the queries in Athena
    - `RAW_DATA`: Store the raw data. No modification or transformation in this folder.
    - `UNZIP_DATA`: [Optional] Unzip the data from `RAW_DATA` using the same structure as the raw data. If the unzip data has three folders, then create three folders in `UNZIP_DATA`. If the data is refreshed, do not append them in this folder
    - `UNZIP_DATA_APPEND_ALL`: [Optional] Unzip and append the data from `RAW_DATA`. Create subfolder corresponding to the table you need to create in Athena. Athena parses the content of a folder. 
  - `LAMBDA_LAYERS`: [Optional] Store the layers used by Lambda
  - `LOGS_FTP_EXECUTION`: [Optional] Store the FTP logs. It is a json file `XXXX_YYYYMMDD.json`. It is created by the lambda function that download the data. This file can be used to trigger the ETL notebook
  - `LOGS_QUERY_EXECUTION`: [Optional] Store the queries ID attached to the query parameter file stored in `DATA/ETL`. This file can be used to trigger the training and analysis notebooks
  - `NOTEBOOKS_WORFLOW`: Store the notebooks used by the Lambda function. Lambda function opens Sagemaker and run a notebook using Papermill to pass arguments in the notebook. Each notebook inside this folder are incremental (00_XX, 01_XX, 02_XX). 
    - `OUTPUT`: Store Sagemaker job -> It is the notebook output. 
  - `SQL_OUTPUT_ATHENA`: Store Athena's output queries
