---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.5.0
  kernelspec:
    display_name: SoS
    language: sos
    name: sos
---

<!-- #region kernel="SoS" -->
# Estimate baseline equation 

This notebook has been generated on 08/14/2020

## Objective(s)

*  Estimate the baseline regression with the new dataset. The baseline regression is the following:

Regress the quality index at the firm, product, city, destination and time  on the VAT rebate tax and eligibility and other control variables plus a bunch of fixed effect detailed later on

![](https://drive.google.com/uc?export=view&id=1stdefCGutycsRZU9EuLgUoe3am8tgg5T)

* Need to find a negative and significant coefficient
* include product-year fixed effect

## Metadata

* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  10
* Task tag
  *  #linear-regression,#baseline-results,#fixed-effect
* Toggl Tag
  * #baseline-result

## Input Cloud Storage [AWS/GCP]

* BigQuery 
  * Table: quality_vat_export_2003_2010
    * Notebook construction file (data lineage) 
      * md : [01_preparation_quality.md](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_Data_preprocessing/01_preparation_quality.md)

## Destination Output/Delivery

1. Latex table (Latex & pdf)
  * Description: The table should look like the one from the paper: thomaspernet/VAT_rebate_quality_china: table 1
  * Github branch: master 
  * Folder: [02_Data_analysis/02_new_baseline_table/Tables](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_Data_analysis/02_new_baseline_table/Tables)

## Things to know (Steps, Attention points or new flow of information)

* Documentation 
  * Coda: 
    * [US 1 Empirical analysis Baseline](https://coda.io/d/VAT-Rebate_d_s12qjWA8O/US-1-Empirical-analysis-Baseline_sugol): Details about FE and baseline regression
* Github
    1. Repo: [thomaspernet/VAT_rebate_quality_china: New FE table](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/02_Data_analysis/01_new_fixed_effect/01_baseline_table.md#new-fe-table) → Table with the fixed effect to reproduce and baseline table
<!-- #endregion -->

<!-- #region kernel="SoS" -->
# Load Dataset

## inputs

- Filename: quality_vat_export_2003_2010
- Link: [BigQuery](https://console.cloud.google.com/bigquery?project=valid-pagoda-132423&p=valid-pagoda-132423&d=China&t=quality_vat_export_2003_2010&page=table)
- Type: Table
<!-- #endregion -->

```sos kernel="Python 3"
import pandas as pd 
import numpy as np
from pathlib import Path
import os, re,  requests, json 
from GoogleDrivePy.google_authorization import authorization_service
from GoogleDrivePy.google_platform import connect_cloud_platform
```

```sos kernel="Python 3"
import function.latex_beautify as lb

%load_ext autoreload
%autoreload 2
```

```sos kernel="R"
options(warn=-1)
library(tidyverse)
library(lfe)
library(lazyeval)
library('progress')
path = "function/table_golatex.R"
source(path)
```

```sos kernel="SoS"
path = os.getcwd()
parent_path = str(Path(path).parent)
project = 'valid-pagoda-132423'


auth = authorization_service.get_authorization(
    path_credential_gcp = "{}/creds/service.json".format(parent_path),
    verbose = False#
)

gcp_auth = auth.authorization_gcp()
gcp = connect_cloud_platform.connect_console(project = project, 
                                             service_account = gcp_auth) 
```

```sos kernel="SoS"
query = (
          "SELECT * "
            "FROM China.quality_vat_export_2003_2010 "

        )
```

```sos kernel="R"
#df_final = gcp.upload_data_from_bigquery(query = query, location = 'US')
#df_final.head()
path = '../../00_Data_catalogue/temporary_local_data/quality_vat_export_2003_2010.csv'
df_final <- read_csv(path) %>%
mutate_if(is.character, as.factor) %>%
    mutate_at(vars(starts_with("FE")), as.factor) %>%
mutate(regime = relevel(regime, ref='Not_Eligible'))
```

```sos kernel="SoS"
#import pandas as pd
#path = '../../00_Data_catalogue/temporary_local_data/quality_vat_export_2003_2010.csv'
#print(pd.read_csv(path).dtypes.to_markdown())
```

<!-- #region kernel="SoS" -->
# Models to estimate

Variables:


|       Variables        | Type    |
|:-----------------------|:--------|
| ID                     | int64   |
| year                   | int64   |
| regime                 | object  |
| Trade_type             | object  |
| Business_type          | object  |
| HS6                    | int64   |
| HS4                    | int64   |
| HS3                    | int64   |
| citycn                 | object  |
| geocode4_corr          | int64   |
| cityen                 | object  |
| destination            | object  |
| Country_en             | object  |
| ISO_alpha              | object  |
| Quantity               | int64   |
| value                  | int64   |
| unit_price             | float64 |
| sigma                  | float64 |
| sigma_price            | float64 |
| lag_vat_m              | int64   |
| lag_vat_reb_m          | int64   |
| lag_tax_rebate         | float64 |
| ln_lag_tax_rebate      | float64 |
| y                      | float64 |
| prediction             | float64 |
| residual               | float64 |
| price_adjusted_quality | float64 |
| kandhelwal_quality     | float64 |
| FE_ct                  | int64   |
| FE_fpr                 | int64   |
| FE_str                 | int64   |
| FE_dt                  | int64   |
| FE_pt                  | int64   |
<!-- #endregion -->

<!-- #region kernel="SoS" -->
## Fixed Effect

| Benchmark | Origin    | Name                     | Description                                                                                                                                                                                                                                                                                                                                    | Math_notebook     |
|-----------|-----------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|
| Yes       | Current   | firm-product-eligibility | captures all the factors that affect firms regardless of the time and type of regime. This firm‒product pair eliminates the demand shocks that firms face and that are not correlated with the types of status. The fixed effects are also responsible for potential correlations between subsidies, R&D, or trade policies and VAT rebates.   | $\alpha^{E}_{it}$ |
| Yes       | Current   | HS4-year-eligibility     |                                                                                                                                                                                                                                                                                                                                                | $\alpha^{E}_{st}$ |
| Yes       | Current   | city-year                | captures the differences in demand, capital intensity, or labor supply that prevail between cities each year                                                                                                                                                                                                                                   | $\alpha_{ct}$     |
| Yes       | Current   | destination-year         | Captures additional level of control, encompassing all the shocks and developments in the economies to which China exports.                                                                                                                                                                                                                    | $\alpha_{dt}$     |
|           | Candidate | Product-year             | account for all factors that affect product-level export irrespective of the trade regime in a given year                                                                                                                                                                                                                                      | $\alpha_{pt}$     |
|           | Candidate | product-destination      |                                                                                                                                                                                                                                                                                                                                                | $\alpha_{pd}$     |
|           | Candidate | Product-destination-year |                                                                                                                                                                                                                                                                                                                                                | $\alpha_{pdt}$    |
<!-- #endregion -->

<!-- #region kernel="SoS" -->
## Table XX

Equation to estimate:


- Overleaf:
<!-- #endregion -->

```sos kernel="R"
#t_1 <- felm(log(unit_price) ~ln_lag_tax_rebate* regime
#            | FE_fpr + FE_str + FE_pt + FE_dt|0 | HS3 + geocode4_corr, df_final,
#            exactDOF = TRUE)
#summary(t_1)
```

```sos kernel="SoS"
try:
    os.remove("table_1.txt")
except:
    pass
try:
    os.remove("table_1.tex")
except:
    pass
```

```sos kernel="SoS"
dep <- "Dependent variable: XX"
table_1 <- go_latex(list(
    t_1
),
    title="TITLE",
    dep_var = dep,
    addFE='',
    save=TRUE,
    note = FALSE,
    name="table_1.txt"
)
```

```sos kernel="SoS"
tbe1 = ""
```

```sos kernel="SoS"
lb.beautify(table_number = 1,
            new_row= False,
           table_nte = tbe1,
           jupyter_preview = True,
            resolution = 150)
```

<!-- #region kernel="SoS" -->
# CREATE REPORT
<!-- #endregion -->

```sos kernel="SoS"
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```sos kernel="SoS"
def create_report(extension = "html"):
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
    kernel_id = connection_file.split('-', 1)[1].split('.')[0]

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
    #path = os.getcwd()
    #parent_path = str(Path(path).parent)
    
    ### Path report
    #path_report = "{}/Reports".format(parent_path)
    #path_report = "{}/Reports".format(path)
    
    ### Path destination
    name_no_extension = notebookname.split(sep, 1)[0]
    source_to_move = name_no_extension +'.{}'.format(extension)
    dest = os.path.join(path,'Reports', source_to_move)
    
    ### Generate notebook
    os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))
```
