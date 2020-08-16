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

$$
\begin{aligned}
\operatorname{Quality}_{c,k,j, t}^{R} &=\alpha \ln \operatorname{VAT} \operatorname{Export} \operatorname{tax}_{k, t-1} \times \text { Eligibility }^{R} \\
&+\lambda X_{c,k,j, t-1}^{R}+F E_{c,k}^{R}+F E_{c,s,t}^{R}+ F E_{k, t}+\epsilon_{ck,j, t}^{R}
\end{aligned}
$$

* Need to find a negative and significant coefficient
* Fixed Effect
    * city-product-regime
    * city-sector-regime-year
    * product-year

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
#
#import numpy as np
#
import os, re,  requests, json , shutil
download_data = False
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
#library(lazyeval)
library('progress')
path = "function/table_golatex.R"
source(path)
```

```sos kernel="Python 3"
if download_data:
    from GoogleDrivePy.google_authorization import authorization_service
    from GoogleDrivePy.google_platform import connect_cloud_platform
    from pathlib import Path
    import pandas as pd 

    path = os.getcwd()
    parent_path = str(Path(path).parent.parent)
    project = 'valid-pagoda-132423'


    auth = authorization_service.get_authorization(
        path_credential_gcp = "{}/creds/service.json".format(parent_path),
        verbose = False#
    )

    gcp_auth = auth.authorization_gcp()
    gcp = connect_cloud_platform.connect_console(project = project, 
                                                 service_account = gcp_auth) 
    query = (
              "SELECT * "
                "FROM China.quality_vat_export_covariate_2003_2010 "

            )
    df_final = gcp.upload_data_from_bigquery(query = query, location = 'US')
    df_final.to_csv('quality_vat_export_covariate_2003_2010.csv', index= False)
    #shutil.move('quality_vat_export_covariate_2003_2010.csv', 
    #'../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010.csv')
```

```sos kernel="R"
#df_final = gcp.upload_data_from_bigquery(query = query, location = 'US')
#df_final.head()
path = '../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010.csv'
df_final <- read_csv(path) %>%
mutate_if(is.character, as.factor) %>%
    mutate_at(vars(starts_with("FE")), as.factor) %>%
mutate(regime = relevel(regime, ref='Not_Eligible'))
```

```sos kernel="Python 3"
#import pandas as pd
#path = '../../00_Data_catalogue/temporary_local_data/quality_vat_export_covariate_2003_2010.csv'
#print(pd.read_csv(path).dtypes.to_markdown())
```

<!-- #region kernel="SoS" -->
# Models to estimate

Variables:


|       Variables        | Type    |
|:-----------------------|:--------|
| cityen                        | object  |
| geocode4_corr                 | int64   |
| year                          | int64   |
| regime                        | object  |
| HS6                           | int64   |
| HS4                           | int64   |
| HS3                           | int64   |
| Country_en                    | object  |
| ISO_alpha                     | object  |
| Quantity                      | int64   |
| value                         | int64   |
| unit_price                    | float64 |
| kandhelwal_quality            | float64 |
| price_adjusted_quality        | float64 |
| lag_tax_rebate                | float64 |
| ln_lag_tax_rebate             | float64 |
| lag_import_tax                | float64 |
| ln_lag_import_tax             | float64 |
| sigma                         | float64 |
| sigma_price                   | float64 |
| y                             | float64 |
| prediction                    | float64 |
| residual                      | float64 |
| FE_ck                         | int64   |
| FE_cst                        | int64   |
| FE_ckr                        | int64   |
| FE_csrt                       | int64   |
| FE_kt                         | int64   |
| FE_pj                         | int64   |
| FE_jt                         | int64   |
| FE_ct                         | int64   |
| growth_export_ckt_1           | float64 |
| growth_export_ckjt_1          | float64 |
| lag_foreign_export_share_ckr  | float64 |
| lag_soe_export_share_ckr      | float64 |
| lag_foreign_export_share_ckjr | float64 |
| lag_soe_export_share_ckjr     | float64 |
<!-- #endregion -->

<!-- #region kernel="SoS" -->
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
<!-- #endregion -->

<!-- #region kernel="SoS" -->
## Table 1: Baseline estimate

Equation to estimate:

$$
\begin{aligned}
\operatorname{Quality}_{c,k,j, t}^{R} &=\alpha \ln \operatorname{VAT} \operatorname{Export} \operatorname{tax}_{k, t-1} \times \text { Eligibility }^{R} \\
&+\lambda X_{c,k,j, t-1}^{R}+F E_{c,k}^{R}+F E_{c,s,t}^{R}+ F E_{k, t}+\epsilon_{ck,j, t}^{R}
\end{aligned}
$$

* Column 1: Estimate for eligible regime only
    * FE: 
        - city-product
        - city-sector-year
        - product-destination
* Column 2: Estimate for non-eligible regime only
    * FE: 
        - city-product
        - city-sector-year
        - product-destination
* Column 3: Full estimate without product-year FE -> Get two coefficients
    * FE: 
        - city-product-regime
        - city-sector-regime-year
        - product-destination
* Column 4: Baseline estimate -> Focus on the coef of interest only
    * FE: 
        - city-product-regime
        - city-sector-regime-year
        - product-year

Sector is defined as the GBT 4 digits
<!-- #endregion -->

```sos kernel="R"
t_0 <- felm(kandhelwal_quality ~ln_lag_tax_rebate+ ln_lag_import_tax  + 
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ck + FE_cst+FE_pj|0 | HS6, df_final %>% filter(regime == 'Eligible'),
            exactDOF = TRUE)
t_1 <- felm(kandhelwal_quality ~ln_lag_tax_rebate + ln_lag_import_tax+ ln_lag_import_tax  + 
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ck + FE_cst+FE_pj|0 | HS6, df_final %>% filter(regime != 'Eligible'),
            exactDOF = TRUE)
t_2 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax  + 
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ckr + FE_csrt + FE_pj|0 | HS6, df_final,
            exactDOF = TRUE)
t_3 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax  + 
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ckr + FE_csrt+FE_kt|0 | HS6, df_final,
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
fe1 <- list(c("City-product fixed effects", "Yes", "Yes", "Yes", "No"),
            
             c("City-sector-year fixed effects", "Yes", "Yes", "Yes", "No"),
            
             c("Product-destination fixed effect","Yes", "Yes", "Yes", "No"),
            
             c("City-product-regime fixed effects","No", "No", "No", "Yes"),
            
             c("City-sector-regime-year fixed effects","No", "No", "No", "Yes"),
             
            c("product-year fixed effects", "No", "No", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="VAT export tax and product's quality upgrading, baseline regression",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="table_0.txt"
)
```

```sos kernel="Python 3"
tbe1  = "This table estimates eq(3). " \
"Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group." \
"Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund." \
"Sectors are defined following the Chinese 4-digit GB/T industry" \
"classification and regroup several products." \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Eligible': 1,
    'Non-Eligible': 1,
    'All': 1,
    'All benchmark': 1,
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
            resolution = 150)
```

<!-- #region kernel="Python 3" -->
## Table 1: Baseline estimate

Equation to estimate:

$$
\begin{aligned}
\operatorname{Quality}_{c,k,j, t}^{R} &=\alpha \ln \operatorname{VAT} \operatorname{Export} \operatorname{tax}_{k, t-1} \times \text { Eligibility }^{R} \\
&+\lambda X_{c,k, t-1}^{R}+F E_{c,k}^{R}+F E_{c,s,t}^{R}+ F E_{k, t}+\epsilon_{ck,j, t}^{R}
\end{aligned}
$$

* Column 1: Estimate for eligible regime only
    * FE: 
        - city-product
        - city-sector-year
        - product-destination
* Column 2: Estimate for non-eligible regime only
    * FE: 
        - city-product
        - city-sector-year
        - product-destination
* Column 3: Full estimate without product-year FE -> Get two coefficients
    * FE: 
        - city-product-regime
        - city-sector-regime-year
        - product-destination
* Column 4: Baseline estimate -> Focus on the coef of interest only
    * FE: 
        - city-product-regime
        - city-sector-regime-year
        - product-year

Sector is defined as the GBT 4 digits
<!-- #endregion -->

```sos kernel="R"
t_0 <- felm(kandhelwal_quality ~ln_lag_tax_rebate+ ln_lag_import_tax  + 
            growth_export_ckt_1 + lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | FE_ck + FE_cst+FE_pj|0 | HS6, df_final %>% filter(regime == 'Eligible'),
            exactDOF = TRUE)
t_1 <- felm(kandhelwal_quality ~ln_lag_tax_rebate + ln_lag_import_tax+ ln_lag_import_tax  + 
            growth_export_ckt_1 + lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | FE_ck + FE_cst+FE_pj|0 | HS6, df_final %>% filter(regime != 'Eligible'),
            exactDOF = TRUE)
t_2 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax  + 
            growth_export_ckt_1 + lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | FE_ckr + FE_csrt + FE_pj|0 | HS6, df_final,
            exactDOF = TRUE)
t_3 <- felm(kandhelwal_quality ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax  + 
            growth_export_ckt_1 + lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | FE_ckr + FE_csrt+FE_kt|0 | HS6, df_final,
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
fe1 <- list(c("City-product fixed effects", "Yes", "Yes", "Yes", "No"),
            
             c("City-sector-year fixed effects", "Yes", "Yes", "Yes", "No"),
            
             c("Product-destination fixed effect","Yes", "Yes", "Yes", "No"),
            
             c("City-product-regime fixed effects","No", "No", "No", "Yes"),
            
             c("City-sector-regime-year fixed effects","No", "No", "No", "Yes"),
             
            c("product-year fixed effects", "No", "No", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="VAT export tax and product's quality upgrading, baseline regression",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="table_1.txt"
)
```

```sos kernel="Python 3"
tbe1  = "This table estimates eq(3). " \
"Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group." \
"Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund." \
"Sectors are defined following the Chinese 4-digit GB/T industry" \
"classification and regroup several products." \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Eligible': 1,
    'Non-Eligible': 1,
    'All': 1,
    'All benchmark': 1,
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
            resolution = 150)
```

<!-- #region kernel="SoS" -->
## Table 1: Baseline estimate [Optional]

Equation to estimate:

$$
\begin{aligned}
\ln \operatorname{Export value}_{c,k,j, t}^{R} &=\alpha \ln \operatorname{VAT} \operatorname{Export} \operatorname{tax}_{k, t-1} \times \text { Eligibility }^{R} \\
&+\lambda X_{c,k,j, t-1}^{R}+F E_{c,k}^{R}+F E_{c,s,t}^{R}+ F E_{k, t}+\epsilon_{ck,j, t}^{R}
\end{aligned}
$$

* Column 1: Estimate for eligible regime only
    * FE: 
        - city-product
        - city-sector-year
        - product-destination
* Column 2: Estimate for non-eligible regime only
    * FE: 
        - city-product
        - city-sector-year
        - product-destination
* Column 3: Full estimate without product-year FE -> Get two coefficients
    * FE: 
        - city-product-regime
        - city-sector-regime-year
        - product-destination
* Column 4: Baseline estimate -> Focus on the coef of interest only
    * FE: 
        - city-product-regime
        - city-sector-regime-year
        - product-year

Sector is defined as the GBT 4 digits
<!-- #endregion -->

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
t_0 <- felm(log(value) ~ln_lag_tax_rebate+ ln_lag_import_tax +
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ck + FE_cst+FE_pj|0 | HS6, df_final %>% filter(regime == 'Eligible'),
            exactDOF = TRUE)
t_1 <- felm(log(value) ~ln_lag_tax_rebate + ln_lag_import_tax+
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ck + FE_cst+FE_pj|0 | HS6, df_final %>% filter(regime != 'Eligible'),
            exactDOF = TRUE)
t_2 <- felm(log(value) ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ckr + FE_csrt + FE_pj|0 | HS6, df_final,
            exactDOF = TRUE)
t_3 <- felm(log(value) ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ckr + FE_csrt+FE_kt|0 | HS6, df_final,
            exactDOF = TRUE)
```

```sos kernel="R"
dep <- "Dependent variable: Ln Export Value"
#FE_cp + FE_cst+FE_pd
#FE_cpr + FE_csrt+FE_pt
fe1 <- list(c("City-product fixed effects", "Yes", "Yes", "Yes", "No"),
            
             c("City-sector-year fixed effects", "Yes", "Yes", "Yes", "No"),
            
             c("Product-destination fixed effect","Yes", "Yes", "Yes", "No"),
            
             c("City-product-regime fixed effects","No", "No", "No", "Yes"),
            
             c("City-sector-regime-year fixed effects","No", "No", "No", "Yes"),
             
            c("product-year fixed effects", "No", "No", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="VAT export tax and product's quality upgrading, baseline regression",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="table_2.txt"
)
```

```sos kernel="Python 3"
tbe1  = "This table estimates eq(3). " \
"Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group." \
"Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund." \
"Sectors are defined following the Chinese 4-digit GB/T industry" \
"classification and regroup several products." \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Eligible': 1,
    'Non-Eligible': 1,
    'All': 1,
    'All benchmark': 1,
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
            resolution = 150)
```

<!-- #region kernel="Python 3" -->
## Table 1: Baseline estimate [Optional]

Equation to estimate:

$$
\begin{aligned}
\ln \operatorname{Unit value}_{c,k,j, t}^{R} &=\alpha \ln \operatorname{VAT} \operatorname{Export} \operatorname{tax}_{k, t-1} \times \text { Eligibility }^{R} \\
&+\lambda X_{c,k,j, t-1}^{R}+F E_{c,k}^{R}+F E_{c,s,t}^{R}+ F E_{k, t}+\epsilon_{ck,j, t}^{R}
\end{aligned}
$$

* Column 1: Estimate for eligible regime only
    * FE: 
        - city-product
        - city-sector-year
        - product-destination
* Column 2: Estimate for non-eligible regime only
    * FE: 
        - city-product
        - city-sector-year
        - product-destination
* Column 3: Full estimate without product-year FE -> Get two coefficients
    * FE: 
        - city-product-regime
        - city-sector-regime-year
        - product-destination
* Column 4: Baseline estimate -> Focus on the coef of interest only
    * FE: 
        - city-product-regime
        - city-sector-regime-year
        - product-year

Sector is defined as the GBT 4 digits
<!-- #endregion -->

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
t_0 <- felm(log(unit_price) ~ln_lag_tax_rebate+ ln_lag_import_tax +
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ck + FE_cst+FE_pj|0 | HS6, df_final %>% filter(regime == 'Eligible'),
            exactDOF = TRUE)
t_1 <- felm(log(unit_price) ~ln_lag_tax_rebate + ln_lag_import_tax+
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ck + FE_cst+FE_pj|0 | HS6, df_final %>% filter(regime != 'Eligible'),
            exactDOF = TRUE)
t_2 <- felm(log(unit_price) ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ckr + FE_csrt + FE_pj|0 | HS6, df_final,
            exactDOF = TRUE)
t_3 <- felm(log(unit_price) ~ln_lag_tax_rebate* regime + ln_lag_import_tax * regime+
            growth_export_ckjt_1 + lag_foreign_export_share_ckjr + lag_soe_export_share_ckjr
            | FE_ckr + FE_csrt+FE_kt|0 | HS6, df_final,
            exactDOF = TRUE)
```

```sos kernel="R"
dep <- "Dependent variable: Ln Unit Price"
#FE_cp + FE_cst+FE_pd
#FE_cpr + FE_csrt+FE_pt
fe1 <- list(c("City-product fixed effects", "Yes", "Yes", "Yes", "No"),
            
             c("City-sector-year fixed effects", "Yes", "Yes", "Yes", "No"),
            
             c("Product-destination fixed effect","Yes", "Yes", "Yes", "No"),
            
             c("City-product-regime fixed effects","No", "No", "No", "Yes"),
            
             c("City-sector-regime-year fixed effects","No", "No", "No", "Yes"),
             
            c("product-year fixed effects", "No", "No", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="VAT export tax and product's quality upgrading, baseline regression",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name="table_3.txt"
)
```

```sos kernel="Python 3"
tbe1  = "This table estimates eq(3). " \
"Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group." \
"Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund." \
"Sectors are defined following the Chinese 4-digit GB/T industry" \
"classification and regroup several products." \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Eligible': 1,
    'Non-Eligible': 1,
    'All': 1,
    'All benchmark': 1,
}
multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& Eligible', 'Non-Eligible', 'All', 'All benchmark']
lb.beautify(table_number = 3,
            #multi_lines_dep = None,
            multi_lines_dep = multi_lines_dep,
            new_row= False,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150)
```

<!-- #region kernel="SoS" -->
# CREATE REPORT
<!-- #endregion -->

```sos kernel="Python 3"
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```sos kernel="Python 3"
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
    os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))
```

```sos kernel="Python 3"
create_report(extension = "html")
```
