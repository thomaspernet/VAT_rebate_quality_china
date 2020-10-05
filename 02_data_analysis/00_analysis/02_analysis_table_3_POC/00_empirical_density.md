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
# POC replicate table 3 of the previous paper by analysing the effect of density

# Objective(s)

Replicate table 3 of the previous paper

https://drive.google.com/uc?export=view&id=1hmtmIb6BF4zBg4NIgNzTqT4HqETcdA-G
* Column 1 is the baseline column with the main fixed effect:
  * city-product-regime: fe_ckr 
  * city-sector-regime-year: fe_csrt 
  * product-year: fe_kt 
* Columns 2 and 3 split the dataset into Eligible and Non eligible. Fixed effect are :
  * city-product: fe_ck 
  * city-sector-year: fe_cst 
  * product-destination: fe_pj 
* Column 4 include Comparative advantages with the main fixed effect:
  * city-product-regime: fe_ckr 
  * city-sector-regime-year: fe_csrt 
  * product-year: fe_kt 
* Comparative advantage should be computed in the notebook with R. The code will be provided
* Repeat with and without export share covariates

# Metadata

* Epic: Epic 2
* US: US 3
* Date Begin: 10/5/2020
* Duration Task: 1
* Description: Replicate table 3 with and without the export share covariates
* Step type:  
* Status: Active
* Source URL:  
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 10
* Task tag: #density,#econometrics,#ols
* Toggl Tag: #model-training
* Meetings:  
* Email Information:  
  * thread: Number of threads: 0(Default 0, to avoid display email)
  *  

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* S3
* Name: 
    * quality_vat_export_covariate_2003_2010
    * ADDITIONAL_DATA/BACI
    * ADDITIONAL_DATA/DENSITY
    * import_export
* Github: 
  * https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/02_transform_table_baseline_covariates.md
  * https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/00_download_data_from_/BACI/baci_92_2002.py
  * https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/00_download_data_from_/DENSITY/density.py
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

<!-- #region kernel="SoS" -->
# Load tables

Since we load the data as a Pandas DataFrame, we want to pass the `dtypes`. We load the schema from Glue to guess the types
<!-- #endregion -->

<!-- #region kernel="SoS" -->
- Filename:
    - Main table S3: https://s3.console.aws.amazon.com/s3/buckets/vat-rebate-quality/DATA/TRANSFORMED/?region=eu-west-3&tab=overview
    - Density: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/ADDITIONAL_DATA/DENSITY/?region=eu-west-3&tab=overview
    - baci: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/ADDITIONAL_DATA/BACI/?region=eu-west-3&tab=overview
- Export 2002 from Athena
<!-- #endregion -->

```sos kernel="Python 3"
s3.download_file(
        key = 'ADDITIONAL_DATA/DENSITY/density.Rda',
    path_local = os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalogue/temporary_local_data")
)
```

```sos kernel="Python 3"
s3.download_file(
        key = 'ADDITIONAL_DATA/BACI/baci92_2002.csv',
    path_local = os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalogue/temporary_local_data")
)
```

```sos kernel="Python 3"
download_data = True
db = 'chinese_trade'
if download_data:
    s3 = service_s3.connect_S3(client = client,
                          bucket = 'vat-rebate-quality', verbose = False)
    query  = """
    WITH trade AS (
      SELECT 
        date as year, 
        CASE WHEN length(hs) < 5 THEN CONCAT('0', hs) ELSE hs END as hs6, 
        value, 
        city_prod, 
        geocode4_corr 
      FROM 
        chinese_trade.import_export 
        INNER JOIN chinese_lookup.city_cn_en ON import_export.city_prod = city_cn_en.citycn 
      WHERE 
        date = '2002' 
    ) 
    SELECT 
      year, 
      hs6, 
      geocode4_corr, 
      CAST(SUM(value) AS DECIMAL(16,5))/1000 as total_value_city
    FROM 
      trade 
    GROUP BY 
      year, 
      hs6, 
      geocode4_corr
      
    """
    
    output = s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=None,  # Add filename to print dataframe
            destination_key=None # Add destination key if need to copy output
        )
    
    s3.copy_object_s3(
    source_key = os.path.join('SQL_OUTPUT_ATHENA', "{}.csv".format(output['QueryID'])),
    destination_key = "DATA/TRANSFORMED/export_2002.csv",
    remove = True
)
    
    s3.download_file(
        key = 'DATA/TRANSFORMED/export_2002.csv',
    path_local = os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalogue/temporary_local_data")
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
Load R dataset: https://stackoverflow.com/questions/5577221/how-can-i-load-an-object-into-a-variable-name-that-i-specify-from-an-r-data-file
<!-- #endregion -->

```sos kernel="R"
loadRData <- function(fileName){
#loads an RData file, and returns it
    load(fileName)
    get(ls()[ls() != "fileName"])
}
```

<!-- #region kernel="R" -->
## Compute RCA

In this part, we use the previous code to compute the RCA.

$$
\frac{\sum_{\mathrm{d}} \mathrm{x}_{\mathrm{isd}} / \sum_{\mathrm{d}} \mathrm{X}_{\mathrm{sd}}}{\sum_{\mathrm{wd}} \mathrm{x}_{\mathrm{iwd}} / \sum_{\mathrm{wd}} \mathrm{X}_{\mathrm{wd}}}
$$

Where s is the country of interest, d and w are the set of all countries in the world, i is the sector of interest, x is the commodity export flow and X is the total export flow. The numerator is the share of good i in the exports of country s, while the denominator is the share of good i in the exports of the world.

What does it tell us? Comparative advantage
underlies economists’ explanations for the
observed pattern of inter-industry trade. In
theoretical models, comparative advantage is
expressed in terms of relative prices evaluated in
the absence of trade. Since these are not observed,
in practice we measure comparative advantage
indirectly. Revealed comparative advantage
indices (RCA) use the trade pattern to identify the
sectors in which an economy has a comparative
advantage, by comparing the country of interests’
trade profile with the world average.
Definition: The RCA index is defined as the ratio
of two shares. The numerator is the share of a
country’s total exports of the commodity of
interest in its total exports. The denominator is
share of world exports of the same commodity in
total world exports.
Range of values: Takes a value between 0 and
+∞. A country is said to have a revealed
comparative advantage if the value exceeds unity.
Limitations: The index is affected by anything
that distorts the trade pattern, e.g., trade barriers.
<!-- #endregion -->

```sos kernel="R"
path_rca <- "../../../00_Data_catalogue/temporary_local_data/baci92_2002.csv"
path_export <- "../../../00_Data_catalogue/temporary_local_data/export_2002.csv"
export <- read_csv(path_export,col_types = "ccdd")
```

```sos kernel="R"
rca <- read_csv(path_rca) %>%
  select(-q, -t) %>%
  filter(i !="156" ) %>%
  group_by(hs6)%>%
  summarise(total_value_world = sum(v)) %>%
  ungroup() %>%
  mutate(hs6 = as.character(hs6),
         total_world = sum(total_value_world)) %>%
  inner_join(export, by= "hs6") %>%
group_by(geocode4_corr)%>% 
mutate(total_city = sum(total_value_city)) %>%
ungroup() %>%
mutate( up = total_value_city / total_city,
       down = total_value_world/total_world,
      rca = up/down,
      balassa = as.numeric(rca> 1),
      hs6 = as.double(hs6))
table(rca$balassa)
```

```sos kernel="R"
path_density = '../../../00_Data_catalogue/temporary_local_data/density.Rda'
density_city <- loadRData(file = path_density) 
density_city <- density_city %>% ungroup() %>% mutate(
    hs6 = as.double(hs6), geocode4_corr = as.double(geocode4_corr))
```

```sos kernel="R"
path = '../../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010.csv'
df_final <- read_csv(path) %>%
mutate_if(is.character, as.factor) %>%
    mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(regime = relevel(regime, ref='NOT_ELIGIBLE')) %>%
left_join(rca %>% select(geocode4_corr, hs6, balassa), by = c("geocode4_corr", "hs6")) %>%
mutate(balassa =ifelse(is.na(balassa),0, balassa)) %>%
inner_join(density_city)
```

```sos kernel="R"
dim(df_final)
```

<!-- #region kernel="SoS" -->
## Table 1: Density, no covariates

$$
\begin{equation}
\begin{split}
Quality_{kcdt} & = \beta_{1}Ordinary^{E}_{i}*\text{Ln VAT export tax}_{k, t-1}*Density_{ck}+ \\
& \beta_{5}Ordinary_{i}^{E}*\text{Ln import tax}_{k, t-1} + \\
& \alpha_{ik}+\alpha_{ct}+\alpha_{dt}+\alpha_{st}^{E}+ \epsilon_{kcdt}
\end{split}
\end{equation}
$$

* Column 1 is the baseline column with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt` 
  * product-year: `fe_kt` 
* Columns 2 and 3 split the dataset into Eligible and Non eligible. Fixed effect are :
  * city-product: `fe_ck` 
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj` 
* Column 4 include Comparative advantages with the main fixed effect:
  * city-product-regime: `fe_ckr` 
  * city-sector-regime-year: `fe_csrt` 
  * product-year: `fe_kt` 

Sector is defined as the GBT 4 digits
<!-- #endregion -->

```sos kernel="R"
#### COUNTRIES
t_0 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_import_tax * regime + ln_lag_import_tax
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final,
            exactDOF = TRUE)

t_1 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville  + 
            ln_lag_import_tax + ln_lag_import_tax
            | fe_ck + fe_cst+fe_pj |0 | hs6, df_final %>% filter(regime == 'ELIGIBLE'),
            exactDOF = TRUE)

#### GOODS
t_2 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville  + 
            ln_lag_import_tax + ln_lag_import_tax 
            | fe_ck + fe_cst+fe_pj |0 | hs6, df_final %>% filter(regime != 'ELIGIBLE'),
            exactDOF = TRUE)

t_3 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville * regime + 
            ln_lag_tax_rebate* balassa * regime + 
            ln_lag_import_tax * regime+ ln_lag_import_tax 
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final,
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
    c("City-product fixed effects", "No", "Yes", "Yes", "No"),
    
    c("City-sector-year fixed effects", "No", "Yes", "Yes", "No"),
    
    c("Product-destination fixed effect","No", "Yes", "Yes", "No"),
    
    c("City-product-regime fixed effects","Yes", "No", "No", "Yes"),
    
    c("City-sector-regime-year fixed effects","Yes", "No", "No", "Yes"),
    
    c("product-year fixed effects", "Yes", "No", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="VAT export tax and firm’s quality upgrading, Effect of density",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="Tables/table_0.txt"
)
```

```sos kernel="Python 3"
os.environ['KMP_DUPLICATE_LIB_OK']='True'
```

```sos kernel="Python 3"
tbe1  = """
This table estimates eq(3). 
LDC and DC are defined according to the World Bank country classification.
Homogeneous and heterogeneous goods are defined according to the official list of goods`s classification, Rauch (1999).
Small and large are computed based on either the count of HS6 exported by city $c$ or the total quantity exported.
When one of these two metrics are above national average, the city is considered as large.
Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group.
Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund.
Sectors are defined following the Chinese 4-digit GB/T industry
classification and regroup several products.
Heteroskedasticity-robust standard errors
clustered at the product level appear inparentheses.
\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."""

multicolumn ={
    '': 1,
    'Eligible': 1,
    'Non-Eligible': 1,
    '': 1,
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
## Table 2: Countries, goods and cities characteristics, with covariates city, regime destination, product

$$
\begin{equation}
\begin{split}
Quality_{kcdt} & = \beta_{1}Ordinary^{E}_{i}*\text{Ln VAT export tax}_{k, t-1}*Density_{ck}+ \\
& \beta_{5}Ordinary_{i}^{E}*\text{Ln import tax}_{k, t-1} + \\
& \alpha_{ik}+\alpha_{ct}+\alpha_{dt}+\alpha_{st}^{E}+ \epsilon_{kcdt}
\end{split}
\end{equation}
$$


* Column 1 is the baseline column with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt` 
  * product-year: `fe_kt` 
* Columns 2 and 3 split the dataset into Eligible and Non eligible. Fixed effect are :
  * city-product: `fe_ck` 
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj` 
* Column 4 include Comparative advantages with the main fixed effect:
  * city-product-regime: `fe_ckr` 
  * city-sector-regime-year: `fe_csrt` 
  * product-year: `fe_kt` 

Sector is defined as the GBT 4 digits
<!-- #endregion -->

```sos kernel="R"
#### COUNTRIES
t_0 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville * regime + 
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr + 
            ln_lag_import_tax * regime + ln_lag_import_tax 
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final,
            exactDOF = TRUE)

t_1 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville  + 
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr +
            ln_lag_import_tax + ln_lag_import_tax
            | fe_ck + fe_cst+fe_pj |0 | hs6, df_final %>% filter(regime == 'ELIGIBLE'),
            exactDOF = TRUE)

#### GOODS
t_2 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville  + 
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr +
            ln_lag_import_tax + ln_lag_import_tax 
            | fe_ck + fe_cst+fe_pj |0 | hs6, df_final %>% filter(regime != 'ELIGIBLE'),
            exactDOF = TRUE)

t_3 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville * regime + 
            lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr +
            ln_lag_tax_rebate* balassa * regime + 
            ln_lag_import_tax * regime+ ln_lag_import_tax 
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final,
            exactDOF = TRUE)
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
    c("City-product fixed effects", "No", "Yes", "Yes", "No"),
    
    c("City-sector-year fixed effects", "No", "Yes", "Yes", "No"),
    
    c("Product-destination fixed effect","No", "Yes", "Yes", "No"),
    
    c("City-product-regime fixed effects","Yes", "No", "No", "Yes"),
    
    c("City-sector-regime-year fixed effects","Yes", "No", "No", "Yes"),
    
    c("product-year fixed effects", "Yes", "No", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="VAT export tax and firm’s quality upgrading, Effect of density",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="Tables/table_1.txt"
)
```

```sos kernel="Python 3"
tbe1  = """
This table estimates eq(3). 
LDC and DC are defined according to the World Bank country classification.
Homogeneous and heterogeneous goods are defined according to the official list of goods`s classification, Rauch (1999).
Small and large are computed based on either the count of HS6 exported by city $c$ or the total quantity exported.
When one of these two metrics are above national average, the city is considered as large.
Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group.
Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund.
Sectors are defined following the Chinese 4-digit GB/T industry
classification and regroup several products.
Heteroskedasticity-robust standard errors
clustered at the product level appear inparentheses.
\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."""

multicolumn ={
    '': 1,
    'Eligible': 1,
    'Non-Eligible': 1,
    '': 1,
}
multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& Eligible', 'Non-Eligible', 'All', 'All benchmark']
lb.beautify(table_number = 1,
            #multi_lines_dep = None,
            multi_lines_dep = multi_lines_dep,
            new_row= False,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200)
```

<!-- #region kernel="Python 3" -->
## Table 3: Density, with covariates city, regime, product

$$
\begin{equation}
\begin{split}
Quality_{kcdt} & = \beta_{1}Ordinary^{E}_{i}*\text{Ln VAT export tax}_{k, t-1}*Density_{ck}+ \\
& \beta_{5}Ordinary_{i}^{E}*\text{Ln import tax}_{k, t-1} + \\
& \alpha_{ik}+\alpha_{ct}+\alpha_{dt}+\alpha_{st}^{E}+ \epsilon_{kcdt}
\end{split}
\end{equation}
$$


* Column 1 is the baseline column with the main fixed effect:
  * city-product-regime: `fe_ckr`
  * city-sector-regime-year: `fe_csrt` 
  * product-year: `fe_kt` 
* Columns 2 and 3 split the dataset into Eligible and Non eligible. Fixed effect are :
  * city-product: `fe_ck` 
  * city-sector-year: `fe_cst` 
  * product-destination: `fe_pj` 
* Column 4 include Comparative advantages with the main fixed effect:
  * city-product-regime: `fe_ckr` 
  * city-sector-regime-year: `fe_csrt` 
  * product-year: `fe_kt` 

Sector is defined as the GBT 4 digits
<!-- #endregion -->

```sos kernel="R"
#### COUNTRIES
t_0 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville * regime + 
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr + 
            ln_lag_import_tax * regime + ln_lag_import_tax
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final,
            exactDOF = TRUE)

t_1 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville  + 
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr +
            ln_lag_import_tax + ln_lag_import_tax
            | fe_ck + fe_cst+fe_pj |0 | hs6, df_final %>% filter(regime == 'ELIGIBLE'),
            exactDOF = TRUE)

#### GOODS
t_2 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville  + 
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr +
            ln_lag_import_tax + ln_lag_import_tax 
            | fe_ck + fe_cst+fe_pj |0 | hs6, df_final %>% filter(regime != 'ELIGIBLE'),
            exactDOF = TRUE)

t_3 <- felm(kandhelwal_quality ~ln_lag_tax_rebate * density_china_ville * regime + 
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr +
            ln_lag_tax_rebate* balassa * regime + 
            ln_lag_import_tax * regime+ ln_lag_import_tax 
            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final,
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
    c("City-product fixed effects", "No", "Yes", "Yes", "No"),
    
    c("City-sector-year fixed effects", "No", "Yes", "Yes", "No"),
    
    c("Product-destination fixed effect","No", "Yes", "Yes", "No"),
    
    c("City-product-regime fixed effects","Yes", "No", "No", "Yes"),
    
    c("City-sector-regime-year fixed effects","Yes", "No", "No", "Yes"),
    
    c("product-year fixed effects", "Yes", "No", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="VAT export tax and firm’s quality upgrading, Effect of density",
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
LDC and DC are defined according to the World Bank country classification.
Homogeneous and heterogeneous goods are defined according to the official list of goods`s classification, Rauch (1999).
Small and large are computed based on either the count of HS6 exported by city $c$ or the total quantity exported.
When one of these two metrics are above national average, the city is considered as large.
Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group.
Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund.
Sectors are defined following the Chinese 4-digit GB/T industry
classification and regroup several products.
Heteroskedasticity-robust standard errors
clustered at the product level appear inparentheses.
\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."""

multicolumn ={
    '': 1,
    'Eligible': 1,
    'Non-Eligible': 1,
    '': 1,
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
