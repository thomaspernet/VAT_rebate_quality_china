---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.5.0
  kernel_info:
    name: python3
  kernelspec:
    display_name: SoS
    language: sos
    name: sos
---

<!-- #region kernel="SoS" -->
# POC replicate table 5 of the previous paper by subsetting according to tax level

# Objective(s)

Replicate table 5 of the previous paper

https://drive.google.com/uc?export=view&id=151HkrKsUMiAEEgNQ66JkrIvIaoVZvYQI

* Column 1 add shocks with the main fixed effect:
  * city-product-regime: fe_ckr 
  * city-sector-regime-year: fe_csrt 
  * product-year: fe_kt 
  * Shocks: hs6 + destination + year = 2003 
* Column 2 includes cities presents full years with the main fixed effect:
  * city-product-regime: fe_ckr 
  * city-sector-regime-year: fe_csrt 
  * product-year: fe_kt 
* Column 3 switch eligible to non eligle with the main fixed effect:
  * city-product-regime: fe_ckr 
  * city-sector-regime-year: fe_csrt 
  * product-year: fe_kt 
* Column 4 switch non eligible to eligible with the main fixed effect:
  * city-product-regime: fe_ckr 
  * city-sector-regime-year: fe_csrt 
  * product-year: fe_kt 
* Column 5 keep rebates 17% with the main fixed effect:
  * city-product-regime: fe_ckr 
  * city-sector-regime-year: fe_csrt 
  * product-year: fe_kt 
* Column 6 exclude rebates 0% with the main fixed effect:
  * city-product-regime: fe_ckr 
  * city-sector-regime-year: fe_csrt 
  * product-year: fe_kt 

# Metadata

* Epic: Epic 2
* US: US 5
* Date Begin: 10/5/2020
* Duration Task: 0
* Description: Replicate table 5 with and without the export share covariates
* Step type:  
* Status: Active
* Source URL: US 05 Table 5 Tax level
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #econometrics,#ols
* Toggl Tag: #model-training
* Meetings:  
* Email Information:  
  * thread: Number of threads: 0(Default 0, to avoid display email)
  *  

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* Name: 
    * quality_vat_export_covariate_2003_2010
    * base_hs6_VAT_2002_2012
* Github: 
  * https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/02_transform_table_baseline_covariates.md
  * https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/01_prepare_tables/01_tables_trade_tariffs_taxes.md
<!-- #endregion -->

<!-- #region kernel="SoS" -->
# Connexion server
<!-- #endregion -->

```sos kernel="Python 3"
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
#import seaborn as sns
import os, shutil

path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)


name_credential = 'thomas_vat_credentials.csv'
region = 'eu-west-3'
bucket = 'chinese-data'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```sos kernel="Python 3"
os.environ['KMP_DUPLICATE_LIB_OK']='True'
```

```sos kernel="Python 3"
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False)
glue = service_glue.connect_glue(client = client) 
```

```sos kernel="Python 3"
pandas_setting = True
if pandas_setting:
    #cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

```sos kernel="R"
#density\_china\_ville:ln\_lag\_import\_tax
change_import_tax_order <- function(table){
    check_three_int <- grep("density_china_ville:regimeELIGIBLE:ln_lag_import_tax$", rownames(table$coef))
    check_two_int <- grep("density_china_ville:ln_lag_import_tax$", rownames(table$coef))
    check_two_eligible <- grep("^regimeELIGIBLE:balassa$", rownames(table$coef))
    
    if (length(check_three_int) !=0) {
    rownames(table$coefficients)[check_three_int] <- 'ln_lag_import_tax:density_china_ville:regimeELIGIBLE'
    rownames(table$beta)[check_three_int] <- 'ln_lag_import_tax:density_china_ville:regimeELIGIBLE'
}
    if (length(check_two_int) !=0) {
    rownames(table$coefficients)[check_two_int] <- 'ln_lag_import_tax:density_china_ville'
    rownames(table$beta)[check_two_int] <- 'ln_lag_import_tax:density_china_ville'
}
    if (length(check_two_eligible) !=0) {
    rownames(table$coefficients)[check_two_eligible] <- 'balassa:regimeELIGIBLE'
    rownames(table$beta)[check_two_eligible] <- 'balassa:regimeELIGIBLE'
}
    
    return (table)
}
```

```sos kernel="R"
loadRData <- function(fileName){
#loads an RData file, and returns it
    load(fileName)
    get(ls()[ls() != "fileName"])
}
```

<!-- #region kernel="SoS" -->
# Load tables

Since we load the data as a Pandas DataFrame, we want to pass the `dtypes`. We load the schema from Glue to guess the types
<!-- #endregion -->

<!-- #region kernel="SoS" -->
- Filename: XX
- Main table S3: https://s3.console.aws.amazon.com/s3/buckets/vat-rebate-quality/DATA/TRANSFORMED/?region=eu-west-3&tab=overview
- Athena:
    - base_hs6_vat_2002_2012
<!-- #endregion -->

```sos kernel="Python 3"
download_data = True
db = 'chinese_trade'
if download_data:

    query = """
    SELECT *
    FROM chinese_trade.base_hs6_vat_2002_2012
    """
    output = s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=None,  # Add filename to print dataframe
            destination_key=None # Add destination key if need to copy output
        )
    s3.download_file(
        key = os.path.join('SQL_OUTPUT_ATHENA', "{}.csv".format(output['QueryID'])),
    path_local = os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalogue/temporary_local_data")
)
    os.rename(os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalogue/temporary_local_data", "{}.csv".format(output['QueryID'])),
         os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalogue/temporary_local_data/base_hs6_vat_2002_2012.csv")
         )
    
```

<!-- #region kernel="SoS" -->
# Models to estimate

The model to estimate is: 

## Fixed Effect

| Benchmark | Origin            | Name                     | Description                                                                                                                                                                                                                                                                                                                                    | Math_notebook     |
|-----------|-------------------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|
| Yes       | Current           | city-product             |                                                                                                                                                                                                                                                                                                                                                | $\alpha_{ck}$     |
| Yes       | Current           | city-product-regime      |                                                                                                                                                                                                                                                                                                                                                | $\alpha_{ck}^R$   |
| Yes       | Current           | city-sector-year         | Sector is defined as GBT 4 digit                                                                                                                                                                                                                                                                                                               | $\alpha_{cst}$    |
| Yes       | Current           | city-sectorーregime-year | Sector is defined as GBT 4 digit                                                                                                                                                                                                                                                                                                               | $\alpha_{cst}^R$  |
| Yes       | Current           | product-destination      |                                                                                                                                                                                                                                                                                                                                                | $\alpha_{pj}$     |
| Yes       | Previous baseline | Product-year             | account for all factors that affect product-level export irrespective of the trade regime in a given year                                                                                                                                                                                                                                      | $\alpha_{pt}$     |
| No        | Previous baseline | firm-product-eligibility | captures all the factors that affect firms regardless of the time and type of regime. This firm‒product pair eliminates the demand shocks that firms face and that are not correlated with the types of status. The fixed effects are also responsible for potential correlations between subsidies, R&D, or trade policies and VAT rebates.   | $\alpha^{E}_{it}$ |
| No        | Previous baseline | HS4-year-eligibility     |                                                                                                                                                                                                                                                                                                                                                | $\alpha^{E}_{st}$ |
| No        | Previous baseline | city-year                | captures the differences in demand, capital intensity, or labor supply that prevail between cities each year                                                                                                                                                                                                                                   | $\alpha_{ct}$     |
| No        | Candidate         | destination-year         | Captures additional level of control, encompassing all the shocks and developments in the economies to which China exports.                                                                                                                                                                                                                    | $\alpha_{dt}$     |


- city-product: `fe_ck`
- City-sector-year: `fe_cst`
- City-product-regime: `fe_ckr`
- City-sector-regime-year: `fe_csrt`
- Product-year: `fe_kt`
- Product-destination: `fe_pj`
- Destination-year: `fe_jt`
<!-- #endregion -->

```sos kernel="Python 3"
import function.latex_beautify as lb

%load_ext autoreload
%autoreload 2
```

```sos kernel="R"
options(warn=-1)
library(tidyverse)
library(lfe)
#library(lazyeval)
library('progress')
path = "function/table_golatex.R"
source(path)
```

<!-- #region kernel="R" -->
Create the fixed effect using Pandas is faster. We then save it locally and we will remove the file at the end of the notebook
<!-- #endregion -->

```sos kernel="Python 3"
add_shock = True
if add_shock:
    path = '../../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010.csv'
    temp = pd.read_csv(path)
    ## Shocks
    temp["fe_group_shock"] = pd.factorize(
        temp["hs6"].astype('str') +
        temp["country_en"].astype('str') + 
        temp["year"].astype('str'))[0]
    path_to_save = '../../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010_temp.csv'
    temp.to_csv(path_to_save, index = False)
```

```sos kernel="R"
path_vat = '../../../00_Data_catalogue/temporary_local_data/base_hs6_vat_2002_2012.csv'
vat <- read_csv(path_vat, col_types = "dddddd")
```

```sos kernel="R"
path_density = '../../../00_Data_catalogue/temporary_local_data/density.Rda'
density_city <- loadRData(file = path_density) 
density_city <- density_city %>% ungroup() %>% mutate(
    hs6 = as.double(hs6), geocode4_corr = as.double(geocode4_corr))
```

<!-- #region kernel="SoS" -->
## Table 1: Tax level, no covariates

$$
\begin{aligned}
\operatorname{Quality}_{c,k,j, t}^{R} &=\alpha \ln \operatorname{VAT} \operatorname{Export} \operatorname{tax}_{k, t-1} \times \text { Eligibility }^{R} \\
&+F E_{c,k}^{R}+F E_{c,s,t}^{R}+ F E_{k, t}+\epsilon_{ck,j, t}^{R}
\end{aligned}
$$

* Column 1 add shocks with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
  * Shocks: `hs6` + `country_en` + `year` 
* Column 2 includes cities presents full years with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 3 switch eligible to non eligle with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 4 switch non eligible to eligible with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 5 keep rebates 17% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 6 exclude rebates 0% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
  
Sector is defined as the GBT 4 digits
<!-- #endregion -->

```sos kernel="R"
path = '../../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010_temp.csv'
df_final <- read_csv(path) %>%
mutate_if(is.character, as.factor) %>%
mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(regime = relevel(regime, ref='NOT_ELIGIBLE')) %>%
left_join(vat)

# Dataset `switch eligible to non eligle` and `switch non eligible to eligible`
temp <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE) %>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==1)

temp_2 <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE)%>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==2)
```

```sos kernel="R"
#### SHOCKS
t_0 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax
            | fe_ckr + fe_csrt+fe_kt + fe_group_shock|0 | hs6, df_final,
            exactDOF = TRUE)

#### BALANCE
t_1 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax 
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% group_by(geocode4_corr) %>%
  mutate(length = length(unique(year))) %>%
  filter(length ==8),
            exactDOF = TRUE)
```

```sos kernel="R"
temp_f_no_eli <- temp_2 %>%
  filter(regime =="NOT_ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)    
#### ELIGIBLE TO NON ELIGIBLE
t_2 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
```

```sos kernel="R"
temp_f_no_eli <- temp_2 %>%
  filter(regime =="ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="NOT_ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)
    
#### NON ELIGIBLE TO ELIGIBLE
t_3 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
```

```sos kernel="R"
##### ONLY 17% 
t_4 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% filter(vat_m==17),
            exactDOF = TRUE)

##### EXCLUDE 0%
t_5 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% filter(vat_reb_m != 0),
            exactDOF = TRUE)
```

```sos kernel="Python 3"
import os
try:
    os.remove("Tables/table_0.txt")
except:
    pass
try:
    os.remove("Tables/table_0.tex")
except:
    pass
try:
    os.remove("Tables/table_0.pdf")
except:
    pass
```

```sos kernel="R"
dep <- "Dependent variable: Product quality"
fe1 <- list(
    c("City-product-regime fixed effects","Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("City-sector-regime-year fixed effects","Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("product-year fixed effects", "Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("product-year-destination fixed effects", "Yes", "No", "No", "No","No","No")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4,t_5
),
    title="VAT export tax and firm’s quality upgrading, Robustness checks",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="Tables/table_0.txt"
)
```

```sos kernel="Python 3"
tbe1  = """
This table estimates eq(3). 
Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group.
Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund.
Sectors are defined following the Chinese 4-digit GB/T industry
classification and regroup several products.
Heteroskedasticity-robust standard errors
clustered at the product level appear inparentheses.
\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."""

multicolumn ={
    'Shocks': 1,
    'Balance': 1,
    'Eligible to non eligible': 1,
    'Non eligible to eligible': 1,
    'Only 17\%': 1,
    'No zero rebate': 1
}
multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& Eligible', 'Non-Eligible', 'All', 'All benchmark']
lb.beautify(table_number = 0,
            #multi_lines_dep = None,
            multi_lines_dep = multi_lines_dep,
            new_row= False,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200)
```

<!-- #region kernel="Python 3" -->
## Table 2: Density, Tax level, with covariates city, regime destination, product

* Column 1 add shocks with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj` 
  * Shocks: `hs6` + `country_en` + `year` 
* Column 2 add shocks with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
  * Shocks: `hs6` + `country_en` + `year` 
* Column 3 includes cities presents full years with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 4 includes cities presents full years with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt` 
* Column 5 switch eligible to non eligle with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 6 switch eligible to non eligle with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt` 
* Column 7 switch non eligible to eligible with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 8 switch non eligible to eligible with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
* Column 9 keep rebates 17% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 10 keep rebates 17% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
* Column 11 exclude rebates 0% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 12 exclude rebates 0% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
<!-- #endregion -->

```sos kernel="R"
path = '../../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010_temp.csv'
df_final <- read_csv(path) %>%
mutate_if(is.character, as.factor) %>%
mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(regime = relevel(regime, ref='NOT_ELIGIBLE')) %>%
left_join(vat) %>%
inner_join(density_city)

# Dataset `switch eligible to non eligle` and `switch non eligible to eligible`
temp <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE) %>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==1)

temp_2 <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE)%>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==2)
```

```sos kernel="R"
#### SHOCKS
t_0 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_pj + fe_group_shock|0 | hs6, df_final,
            exactDOF = TRUE)
t_0<- change_import_tax_order(t_0)
t_1<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_kt + fe_group_shock|0 | hs6, df_final,
            exactDOF = TRUE)
t_1 <- change_import_tax_order(t_1)
#### BALANCE
t_2 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% group_by(geocode4_corr) %>%
  mutate(length = length(unique(year))) %>%
  filter(length ==8),
            exactDOF = TRUE)
t_2<- change_import_tax_order(t_2)
t_3<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% group_by(geocode4_corr) %>%
  mutate(length = length(unique(year))) %>%
  filter(length ==8),
            exactDOF = TRUE)
t_3 <- change_import_tax_order(t_3)
```

```sos kernel="R"
#### ELIGIBLE TO NON ELIGIBLE
temp_f_no_eli <- temp_2 %>%
  filter(regime =="NOT_ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)   


t_4 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_4<- change_import_tax_order(t_4)
t_5<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_5 <- change_import_tax_order(t_5)
```

```sos kernel="R"
#### NON ELIGIBLE TO ELIGIBLE
temp_f_no_eli <- temp_2 %>%
  filter(regime =="ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="NOT_ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)
    
t_6 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_6<- change_import_tax_order(t_6)
t_7<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_7 <- change_import_tax_order(t_7)
```

```sos kernel="R"
##### ONLY 17% 
t_8 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% filter(vat_m==17),
            exactDOF = TRUE)
t_8<- change_import_tax_order(t_8)
t_9<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% filter(vat_m==17),
            exactDOF = TRUE)
t_9 <- change_import_tax_order(t_9)

##### EXCLUDE 0%
t_10 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% filter(vat_reb_m != 0),
            exactDOF = TRUE)
t_10<- change_import_tax_order(t_10)
t_11<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% filter(vat_reb_m != 0),
            exactDOF = TRUE)
t_11 <- change_import_tax_order(t_11)
```

```sos kernel="Python 3"
import os
try:
    os.remove("Tables/table_1.txt")
except:
    pass
try:
    os.remove("Tables/table_1.tex")
except:
    pass
try:
    os.remove("Tables/table_1.pdf")
except:
    pass
```

```sos kernel="R"
dep <- "Dependent variable: Product quality"
fe1 <- list(
    
    c("City-product-regime fixed effects", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("City-sector-year fixed effects", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Product-destination fixed effect","Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No"),
        
    c("product-year fixed effects", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11
),
    title="VAT export tax and firm’s quality upgrading, Robustness checks",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="Tables/table_1.txt"
)
```

```sos kernel="Python 3"
reorder = {
    # Old, New
    4:2, ## Density * eligible
    8:3, ## comp * eligibile
}

tbe1  = """
This table estimates eq(3). 
Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group.
Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund.
Sectors are defined following the Chinese 4-digit GB/T industry
classification and regroup several products.
Heteroskedasticity-robust standard errors
clustered at the product level appear inparentheses.
\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."""

multicolumn ={
    'Shocks': 2,
    'Balance': 2,
    'Eligible to non eligible': 2,
    'Non eligible to eligible': 2,
    'Only 17\%': 2,
    'No zero rebate': 2
}
multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& Eligible', 'Non-Eligible', 'All', 'All benchmark']
lb.beautify(table_number = 1,
            #multi_lines_dep = None,
            multi_lines_dep = multi_lines_dep,
            new_row= False,
            multicolumn = multicolumn,
            reorder_var = None,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 300)
```

<!-- #region kernel="Python 3" -->
## Table 3: Tax level, with covariates city, regime destination, product

$$
\begin{aligned}
\operatorname{Quality}_{c,k,j, t}^{R} &=\alpha \ln \operatorname{VAT} \operatorname{Export} \operatorname{tax}_{k, t-1} \times \text { Eligibility }^{R} \\
&+F E_{c,k}^{R}+F E_{c,s,t}^{R}+ F E_{k, t}+\epsilon_{ck,j, t}^{R}
\end{aligned}
$$

* Column 1 add shocks with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
  * Shocks: `hs6` + `country_en` + `year` 
* Column 2 includes cities presents full years with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 3 switch eligible to non eligle with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 4 switch non eligible to eligible with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 5 keep rebates 17% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 6 exclude rebates 0% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
  
Sector is defined as the GBT 4 digits
<!-- #endregion -->

```sos kernel="R"
path = '../../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010_temp.csv'
df_final <- read_csv(path) %>%
mutate_if(is.character, as.factor) %>%
mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(regime = relevel(regime, ref='NOT_ELIGIBLE')) %>%
left_join(vat)

# Dataset `switch eligible to non eligle` and `switch non eligible to eligible`
temp <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE) %>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==1)

temp_2 <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE)%>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==2)
```

```sos kernel="R"
#### SHOCKS
t_0 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_csrt+fe_kt + fe_group_shock|0 | hs6, df_final,
            exactDOF = TRUE)

#### BALANCE
t_1 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% group_by(geocode4_corr) %>%
  mutate(length = length(unique(year))) %>%
  filter(length ==8),
            exactDOF = TRUE)
```

```sos kernel="R"
temp_f_no_eli <- temp_2 %>%
  filter(regime =="NOT_ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)

#data_set_d <- data_set %>%
#  anti_join(temp_f_eli)
    
#### ELIGIBLE TO NON ELIGIBLE
t_2 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
```

```sos kernel="R"
temp_f_no_eli <- temp_2 %>%
  filter(regime =="ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="NOT_ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)

#data_set_d <- data_set %>%
#  anti_join(temp_f_eli)
    
#### NON ELIGIBLE TO ELIGIBLE
t_3 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
```

```sos kernel="R"
##### ONLY 17% 
t_4 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% filter(vat_m==17),
            exactDOF = TRUE)

##### EXCLUDE 0%
t_5 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% filter(vat_reb_m != 0),
            exactDOF = TRUE)
```

```sos kernel="Python 3"
import os
try:
    os.remove("Tables/table_2.txt")
except:
    pass
try:
    os.remove("Tables/table_2.tex")
except:
    pass
try:
    os.remove("Tables/table_2.pdf")
except:
    pass
```

```sos kernel="R"
dep <- "Dependent variable: Product quality"
fe1 <- list(
    c("City-product-regime fixed effects","Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("City-sector-regime-year fixed effects","Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("product-year fixed effects", "Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("product-year-destination fixed effects", "Yes", "No", "No", "No","No","No")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="VAT export tax and firm’s quality upgrading, Robustness checks",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="Tables/table_2.txt"
)
```

```sos kernel="Python 3"
tbe1  = """
This table estimates eq(3). 
Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group.
Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund.
Sectors are defined following the Chinese 4-digit GB/T industry
classification and regroup several products.
Heteroskedasticity-robust standard errors
clustered at the product level appear inparentheses.
\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."""

multicolumn ={
    'Shocks': 1,
    'Balance': 1,
    'Eligible to non eligible': 1,
    'Non eligible to eligible': 1,
    'Only 17\%': 1,
    'No zero rebate': 1
}
multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& Eligible', 'Non-Eligible', 'All', 'All benchmark']
lb.beautify(table_number = 2,
            #multi_lines_dep = None,
            multi_lines_dep = multi_lines_dep,
            new_row= False,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200)
```

<!-- #region kernel="Python 3" -->
## Table 4: Density, Tax level, with covariates city, regime destination, product

* Column 1 add shocks with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj` 
  * Shocks: `hs6` + `country_en` + `year` 
* Column 2 add shocks with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
  * Shocks: `hs6` + `country_en` + `year` 
* Column 3 includes cities presents full years with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 4 includes cities presents full years with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt` 
* Column 5 switch eligible to non eligle with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 6 switch eligible to non eligle with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt` 
* Column 7 switch non eligible to eligible with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 8 switch non eligible to eligible with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
* Column 9 keep rebates 17% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 10 keep rebates 17% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
* Column 11 exclude rebates 0% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 12 exclude rebates 0% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
<!-- #endregion -->

```sos kernel="R"
path = '../../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010_temp.csv'
df_final <- read_csv(path) %>%
mutate_if(is.character, as.factor) %>%
mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(regime = relevel(regime, ref='NOT_ELIGIBLE')) %>%
left_join(vat)%>%
inner_join(density_city)

# Dataset `switch eligible to non eligle` and `switch non eligible to eligible`
temp <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE) %>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==1)

temp_2 <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE)%>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==2)
```

```sos kernel="R"
#### SHOCKS
t_0 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_pj + fe_group_shock|0 | hs6, df_final,
            exactDOF = TRUE)
t_0<- change_import_tax_order(t_0)
t_1<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_kt + fe_group_shock|0 | hs6, df_final,
            exactDOF = TRUE)
t_1 <- change_import_tax_order(t_1)
#### BALANCE
t_2 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% group_by(geocode4_corr) %>%
  mutate(length = length(unique(year))) %>%
  filter(length ==8),
            exactDOF = TRUE)
t_2<- change_import_tax_order(t_2)
t_3<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% group_by(geocode4_corr) %>%
  mutate(length = length(unique(year))) %>%
  filter(length ==8),
            exactDOF = TRUE)
t_3 <- change_import_tax_order(t_3)
```

```sos kernel="R"
#### ELIGIBLE TO NON ELIGIBLE
temp_f_no_eli <- temp_2 %>%
  filter(regime =="NOT_ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)   


t_4 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_4<- change_import_tax_order(t_4)
t_5<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_5 <- change_import_tax_order(t_5)
```

```sos kernel="R"
#### NON ELIGIBLE TO ELIGIBLE
temp_f_no_eli <- temp_2 %>%
  filter(regime =="ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="NOT_ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)
    
t_6 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_6<- change_import_tax_order(t_6)
t_7<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_7 <- change_import_tax_order(t_7)
```

```sos kernel="R"
##### ONLY 17% 
t_8 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% filter(vat_m==17),
            exactDOF = TRUE)
t_8<- change_import_tax_order(t_8)
t_9<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% filter(vat_m==17),
            exactDOF = TRUE)
t_9 <- change_import_tax_order(t_9)

##### EXCLUDE 0%
t_10 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% filter(vat_reb_m != 0),
            exactDOF = TRUE)
t_10<- change_import_tax_order(t_10)
t_11<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% filter(vat_reb_m != 0),
            exactDOF = TRUE)
t_11 <- change_import_tax_order(t_11)
```

```sos kernel="Python 3"
import os
try:
    os.remove("Tables/table_3.txt")
except:
    pass
try:
    os.remove("Tables/table_3.tex")
except:
    pass
try:
    os.remove("Tables/table_3.pdf")
except:
    pass
```

```sos kernel="R"
dep <- "Dependent variable: Product quality"
fe1 <- list(
    
    c("City-product-regime fixed effects", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("City-sector-year fixed effects", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Product-destination fixed effect","Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No"),
        
    c("product-year fixed effects", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11
),
    title="VAT export tax and firm’s quality upgrading, Robustness checks",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="Tables/table_3.txt"
)
```

```sos kernel="Python 3"
reorder = {
    # Old, New
    4:2, ## Density * eligible
    8:3, ## comp * eligibile
}

tbe1  = """
This table estimates eq(3). 
Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group.
Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund.
Sectors are defined following the Chinese 4-digit GB/T industry
classification and regroup several products.
Heteroskedasticity-robust standard errors
clustered at the product level appear inparentheses.
\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."""

multicolumn ={
    'Shocks': 2,
    'Balance': 2,
    'Eligible to non eligible': 2,
    'Non eligible to eligible': 2,
    'Only 17\%': 2,
    'No zero rebate': 2
}
multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& Eligible', 'Non-Eligible', 'All', 'All benchmark']
lb.beautify(table_number = 3,
            #multi_lines_dep = None,
            multi_lines_dep = multi_lines_dep,
            new_row= False,
            multicolumn = multicolumn,
            reorder_var = None,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 300)
```

<!-- #region kernel="Python 3" -->
## Table 5: Tax level, with covariates city, regime, product

$$
\begin{aligned}
\operatorname{Quality}_{c,k,j, t}^{R} &=\alpha \ln \operatorname{VAT} \operatorname{Export} \operatorname{tax}_{k, t-1} \times \text { Eligibility }^{R} \\
&+F E_{c,k}^{R}+F E_{c,s,t}^{R}+ F E_{k, t}+\epsilon_{ck,j, t}^{R}
\end{aligned}
$$


* Column 1 add shocks with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
  * Shocks: `hs6` + `country_en` + `year` 
* Column 2 includes cities presents full years with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 3 switch eligible to non eligle with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 4 switch non eligible to eligible with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 5 keep rebates 17% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
* Column 6 exclude rebates 0% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt`
  
Sector is defined as the GBT 4 digits
<!-- #endregion -->

```sos kernel="R"
path = '../../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010_temp.csv'
df_final <- read_csv(path) %>%
mutate_if(is.character, as.factor) %>%
mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(regime = relevel(regime, ref='NOT_ELIGIBLE')) %>%
left_join(vat)

# Dataset `switch eligible to non eligle` and `switch non eligible to eligible`
temp <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE) %>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==1)

temp_2 <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE)%>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==2)
```

```sos kernel="R"
#### SHOCKS
t_0 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_csrt+fe_kt + fe_group_shock|0 | hs6, df_final,
            exactDOF = TRUE)

#### BALANCE
t_1 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% group_by(geocode4_corr) %>%
  mutate(length = length(unique(year))) %>%
  filter(length ==8),
            exactDOF = TRUE)
```

```sos kernel="R"
temp_f_no_eli <- temp_2 %>%
  filter(regime =="NOT_ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)

#data_set_d <- data_set %>%
#  anti_join(temp_f_eli)
    
#### ELIGIBLE TO NON ELIGIBLE
t_2 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
```

```sos kernel="R"
temp_f_no_eli <- temp_2 %>%
  filter(regime =="ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="NOT_ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)

#data_set_d <- data_set %>%
#  anti_join(temp_f_eli)
    
#### NON ELIGIBLE TO ELIGIBLE
t_3 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
```

```sos kernel="R"
##### ONLY 17% 
t_4 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% filter(vat_m==17),
            exactDOF = TRUE)

##### EXCLUDE 0%
t_5 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% filter(vat_reb_m != 0),
            exactDOF = TRUE)
```

```sos kernel="Python 3"
import os
try:
    os.remove("Tables/table_4.txt")
except:
    pass
try:
    os.remove("Tables/table_4.tex")
except:
    pass
try:
    os.remove("Tables/table_4.pdf")
except:
    pass
```

```sos kernel="R"
dep <- "Dependent variable: Product quality"
fe1 <- list(
    c("City-product-regime fixed effects","Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("City-sector-regime-year fixed effects","Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("product-year fixed effects", "Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("product-year-destination fixed effects", "Yes", "No", "No", "No","No","No")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="VAT export tax and firm’s quality upgrading, Robustness checks",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="Tables/table_4.txt"
)
```

```sos kernel="Python 3"
tbe1  = """
This table estimates eq(3). 
Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund.
Sectors are defined following the Chinese 4-digit GB/T industry
classification and regroup several products.
Heteroskedasticity-robust standard errors
clustered at the product level appear inparentheses.
\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."""

multicolumn ={
    'Shocks': 1,
    'Balance': 1,
    'Eligible to non eligible': 1,
    'Non eligible to eligible': 1,
    'Only 17\%': 1,
    'No zero rebate': 1
}
multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& Eligible', 'Non-Eligible', 'All', 'All benchmark']
lb.beautify(table_number = 4,
            #multi_lines_dep = None,
            multi_lines_dep = multi_lines_dep,
            new_row= False,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200)
```

<!-- #region kernel="Python 3" -->
## Table 6: Density, Tax level, with covariates city, regime, product

* Column 1 add shocks with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj` 
  * Shocks: `hs6` + `country_en` + `year` 
* Column 2 add shocks with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
  * Shocks: `hs6` + `country_en` + `year` 
* Column 3 includes cities presents full years with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 4 includes cities presents full years with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt` 
* Column 5 switch eligible to non eligle with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 6 switch eligible to non eligle with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt`
  * product-year: `fe_kt` 
* Column 7 switch non eligible to eligible with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 8 switch non eligible to eligible with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
* Column 9 keep rebates 17% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 10 keep rebates 17% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
* Column 11 exclude rebates 0% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj`
* Column 12 exclude rebates 0% with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-year: `fe_cst` 
  * product-year: `fe_kt` 
<!-- #endregion -->

```sos kernel="R"
path = '../../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010_temp.csv'
df_final <- read_csv(path) %>%
mutate_if(is.character, as.factor) %>%
mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(regime = relevel(regime, ref='NOT_ELIGIBLE')) %>%
left_join(vat)%>%
inner_join(density_city)

# Dataset `switch eligible to non eligle` and `switch non eligible to eligible`
temp <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE) %>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==1)

temp_2 <- df_final %>%
  group_by(geocode4_corr, hs6, year) %>%
  select(c("geocode4_corr","hs6","regime","year")) %>%
  arrange(geocode4_corr, hs6, year) %>%
  distinct(.keep_all = TRUE)%>%
  ungroup() %>%
  group_by(geocode4_corr, hs6) %>%
  filter(row_number()==2) 
```

```sos kernel="R"
#### SHOCKS
t_0 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime + 
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_pj + fe_group_shock|0 | hs6, df_final,
            exactDOF = TRUE)
t_0<- change_import_tax_order(t_0)
t_1<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_kt + fe_group_shock|0 | hs6, df_final,
            exactDOF = TRUE)
t_1 <- change_import_tax_order(t_1)
#### BALANCE
t_2 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% group_by(geocode4_corr) %>%
  mutate(length = length(unique(year))) %>%
  filter(length ==8),
            exactDOF = TRUE)
t_2<- change_import_tax_order(t_2)
t_3<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% group_by(geocode4_corr) %>%
  mutate(length = length(unique(year))) %>%
  filter(length ==8),
            exactDOF = TRUE)
t_3 <- change_import_tax_order(t_3)
```

```sos kernel="R"
#### ELIGIBLE TO NON ELIGIBLE
temp_f_no_eli <- temp_2 %>%
  filter(regime =="NOT_ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)   


t_4 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_4<- change_import_tax_order(t_4)
t_5<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_5 <- change_import_tax_order(t_5)
```

```sos kernel="R"
#### NON ELIGIBLE TO ELIGIBLE
temp_f_no_eli <- temp_2 %>%
  filter(regime =="ELIGIBLE")

temp_f_eli <- temp %>%
  filter(regime =="NOT_ELIGIBLE") %>%
  inner_join(temp_f_no_eli, by = c("geocode4_corr","hs6")) 

temp_f_eli <-temp_f_eli%>%
  filter(year.x != year.y)
    
t_6 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_6<- change_import_tax_order(t_6)
t_7<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% anti_join(temp_f_eli),
            exactDOF = TRUE)
t_7 <- change_import_tax_order(t_7)
```

```sos kernel="R"
##### ONLY 17% 
t_8 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% filter(vat_m==17),
            exactDOF = TRUE)
t_8<- change_import_tax_order(t_8)
t_9<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% filter(vat_m==17),
            exactDOF = TRUE)
t_9 <- change_import_tax_order(t_9)

##### EXCLUDE 0%
t_10 <- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_pj|0 | hs6, df_final %>% filter(vat_reb_m != 0),
            exactDOF = TRUE)
t_10<- change_import_tax_order(t_10)
t_11<- felm(kandhelwal_quality ~
            ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * density_china_ville * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr + fe_cst+fe_kt|0 | hs6, df_final %>% filter(vat_reb_m != 0),
            exactDOF = TRUE)
t_11 <- change_import_tax_order(t_11)
```

```sos kernel="Python 3"
import os
try:
    os.remove("Tables/table_5.txt")
except:
    pass
try:
    os.remove("Tables/table_5.tex")
except:
    pass
try:
    os.remove("Tables/table_5.pdf")
except:
    pass
```

```sos kernel="R"
dep <- "Dependent variable: Product quality"
fe1 <- list(
    
    c("City-product-regime fixed effects", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("City-sector-year fixed effects", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Product-destination fixed effect","Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No"),
        
    c("product-year fixed effects", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11
),
    title="VAT export tax and firm’s quality upgrading, Robustness checks",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="Tables/table_5.txt"
)
```

```sos kernel="Python 3"
reorder = {
    # Old, New
    4:2, ## Density * eligible
    8:3, ## comp * eligibile
}

tbe1  = """
This table estimates eq(3). 
Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group.
Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund.
Sectors are defined following the Chinese 4-digit GB/T industry
classification and regroup several products.
Heteroskedasticity-robust standard errors
clustered at the product level appear inparentheses.
\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."""

multicolumn ={
    'Shocks': 2,
    'Balance': 2,
    'Eligible to non eligible': 2,
    'Non eligible to eligible': 2,
    'Only 17\%': 2,
    'No zero rebate': 2
}
multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& Eligible', 'Non-Eligible', 'All', 'All benchmark']
lb.beautify(table_number = 5,
            #multi_lines_dep = None,
            multi_lines_dep = multi_lines_dep,
            new_row= False,
            multicolumn = multicolumn,
            reorder_var = None,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 300)
```

```sos kernel="Python 3"
os.remove(path_to_save)
os.remove(os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalogue/temporary_local_data/base_hs6_vat_2002_2012.csv"))
```

<!-- #region nteract={"transient": {"deleting": false}} kernel="SoS" -->
# Generate reports
<!-- #endregion -->

```sos nteract={"transient": {"deleting": false}} outputExpanded=false kernel="Python 3"
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```sos nteract={"transient": {"deleting": false}} outputExpanded=false kernel="Python 3"
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
```

```sos nteract={"transient": {"deleting": false}} outputExpanded=false kernel="Python 3"
create_report(extension = "html", keep_code = True)
```
