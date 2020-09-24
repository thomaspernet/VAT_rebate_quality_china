---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.2
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Add Export covariates

This notebook has been generated on 08/16/2020

## Objective(s)

*   Add covariates related to the export profil of a given city. Six covariates can be added to the model:
  * Lag Export growth by city, product, destination
  * Lag Export growth by city, product
  * Lag foreign export share by city, product, destination, regime
  * Lag foreign export share by city, product, regime
  * Lag SOE export share by city, product, destination, regime
  * Lag SOE export share by city, product, regime
* Include DataStudio (tick if yes): false

## Metadata

* Task type:
  * Jupyter Notebook
* Users: :
  * Thomas Pernet
* Watchers:
  * Thomas Pernet
* Estimated Log points:
  * One being a simple task, 15 a very difficult one
  *  5
* Task tag
  *  #covariates
* Toggl Tag
  * #variable-computation

## Input Cloud Storage [AWS/GCP]

1. BigQuery 
  * Table: quality_vat_export_2003_2010 
    * Notebook construction file (data lineage) 
      * md : [00_preparation_baseline_db.md](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_Data_preprocessing/00_preparation_baseline_db.md)
      * md : [01_preparation_quality.md](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_Data_preprocessing/01_preparation_quality.md)
  * Table: tradedata_* 
    * Notebook construction file (data lineage) 
      * md : 
      * github: https://github.com/thomaspernet/Chinese-Trade-Data
  * Table: city_cn_en 
    * Notebook construction file (data lineage) 
      * md : 
  * Table: country_cn_en 
    * Origin:
      * https://docs.google.com/spreadsheets/d/1QYWcLI-ybZ6l7JeWvHPNhhIurNxSM3-sDyrrLB-l2VQ/edit#gid=909268406
    * Notebook construction file (data lineage) 
      * md : 

## Destination Output/Delivery

* BigQuery: 
  * Project: valid-pagoda-132423 
  * Database:China 
  *  Table: quality_vat_export_covariate_2003_2010    

## Things to know (Steps, Attention points or new flow of information)

1. Paper 1: Trade policy repercussions: the role of local product space-Evidence from China
  * Paperpile: https://paperpile.com/shared/WCeap6
  * Paper: https://docs.google.com/file/d/1n8ociwqDVBaUKRTOmFX1mJepwIYqXlbd/edit
  * Authors: Gourdon, Julien; Hering, Laura; Monjon, Stéphanie; Poncet, Sandra 


# Load Dataset

## inputs

- Filename: VAT_export_2003_2010
- Link: https://console.cloud.google.com/bigquery?project=valid-pagoda-132423&p=valid-pagoda-132423&d=China&t=VAT_export_2003_2010&page=table
- Type: Table

Save locally because too slow to load

```python
import pandas as pd 
import numpy as np
from pathlib import Path
import os, re,  requests, json 
from GoogleDrivePy.google_authorization import authorization_service
from GoogleDrivePy.google_platform import connect_cloud_platform
```

```python
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

```python
query = (
          "SELECT * "
            "FROM China.VAT_export_2003_2010 "

        )
df_vat = gcp.upload_data_from_bigquery(query = query, location = 'US')
df_vat.head()
```

```python
#df_vat.to_csv('../00_Data_catalogue/temporary_local_data/VAT_export_2003_2010.csv', index = False)
```

# Steps

1. Lag Export growth by city, product, destination
2. Lag Export growth by city, product
3. Lag foreign export share by city, product, destination, regime
4. Lag foreign export share by city, product, regime
5. Lag SOE export share by city, product, destination, regime
6. Lag SOE export share by city, product, regime
7. Merge all tables

<!-- #region -->
## Consideration’s point for the developers/analyst

From [Trade policy repercussions: the role of local product space-Evidence from China](https://docs.google.com/file/d/1n8ociwqDVBaUKRTOmFX1mJepwIYqXlbd/edit)

2 Need to keep this variable Business_type  to compute the share, and the business types we are interested in are
  * SOE: 国有企业 
  * Foreign: 外商独资企业 
  


### B-1 Construction and data sources of control variables

Since it is still possible that local export dynamics for a given product vary by trade
regime or city, we add a vector of control variables $X_{c,k,j, t-1}^{R}$, with coefficient vector $\lambda$. Therefore, we include the share of exports by foreign firms ($\text{Foreign export share}_{c,k,j, t-1}^{R}$) and the share of state-owned firms ($\text{State export share}_{c,k,j, t-1}^{R}$) defined at the city-product-regime level. 

These two controls are crucial to account for the time-varying ability of different localities to export
different products (under different regimes) as export performance in China varies greatly by
firm ownership (Amiti and Freund, 2010). We further include the change in city-product
export values from t-2 to t-1 ($\text{Export growth}_{c,k,j, t-1}$) to account for export dynamics at the
city and HS6 product level.

The Customs trade data is used to obtain several of our control variables: $\text{Export growth}_{k,j,t-1}$,
$\text{Export growth}_{c,k,j, t-1}$, $\text{Foreign export share}_{c,k,j, t-1}^{R}$
and $\text{State export share}_{c,k,j, t-1}^{R}$.

$\text{Export growth}_{k,j,t-1}$ and $\text{Export growth}_{c,k,j, t-1}$ are yearly export growth at the product-level
and at the city-product level respectively. These proxies of export dynamics are computed
using the mid-point growth rate formula using export values from t-2 and t-1. $\text{Foreign export share}_{c,k,j, t-1}^{R}$
and $\text{State export share}_{c,k,j, t-1}^{R}$ measure respectively the share of export quantities by foreign and state-owned firms for each product-city-regime combination.

<!-- #endregion -->

## Query Methodolology

For each query below, we store them in a dataset named `temporary` in BigQuery, then we will merge them with `quality_vat_export_2003_2010`.

We keep the exact same number of observations (5,816,438) in `quality_vat_export_2003_2010`, and replace `NULL`with 0.

The new tables are the following:

*  lag_export_growth_ckj,
*  lag_export_growth_ck,
*  lag_foreign_export_ckjr,
*  lag_foreign_export_ckr,
*  lag_soe_export_ckjr,
* lag_soe_export_ckr


### Step 1 Lag Export growth by city, product, destination

- Table name: lag_export_growth_ckj
- Observation: 8,528,628



```python
query = """
WITH merge_data AS (
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value,
    --CASE WHEN Trade_type = '进料加工贸易' 
    --OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime,
    --CASE WHEN Business_type = '国有企业' OR Business_type = '国有' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership,
    --CASE WHEN Business_type = '外商独资企业' OR Business_type = '独资' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership
  FROM 
    `China.tradedata_*` 
  WHERE 
    (
      _TABLE_SUFFIX BETWEEN '2000' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND (
      Trade_type = '进料加工贸易' 
      OR Trade_type = '一般贸易' 
      OR Trade_type = '来料加工装配贸易'
    ) 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '国有' 
      OR Business_type = '私营' 
      OR Business_type = '集体'
      OR Business_type = '独资'
      OR Business_type = '外商独资企业'
      
    ) 
  UNION ALL 
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value,
    --CASE WHEN Trade_type = '进料加工贸易' 
    --OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime,
    --CASE WHEN Business_type = '国有企业' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership,
    --CASE WHEN Business_type = '外商独资企业' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership
    
  FROM 
    `China.tradedata_*` 
  WHERE 
    NOT(
      _TABLE_SUFFIX BETWEEN '2000' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业'
      OR Business_type = '外商独资企业'
      
    )
) 
SELECT 
      * 
    FROM 
      (
        WITH aggregate AS (
          SELECT  
            city_prod, 
            year, 
            --regime,
            --SOE_ownership,
            HS6, 
            destination, 
            -- SUM(Quantity) as Quantity, 
            SUM(value) as value 
          FROM 
            merge_data 
          GROUP BY 
            year, 
            --regime,
            --SOE_ownership,
            HS6, 
            city_prod,
            destination
        )
        SELECT 
      * 
    FROM 
      (
        WITH add_var AS (
        SELECT 
        cityen,geocode4_corr,year,HS6, Country_en, ISO_alpha,
        value, CASE WHEN SAFE_DIVIDE((lag_value_t1 - lag_value_t2), lag_value_t2) IS NULL THEN 1
        ELSE SAFE_DIVIDE((lag_value_t1 - lag_value_t2), lag_value_t2) END as growth_export_ckjt_1
        FROM (
        SELECT
        city_prod, HS6,year,destination,
        value,
        LAG(value, 1) OVER (
                PARTITION BY city_prod, HS6, destination 
                ORDER BY 
                  city_prod,
                  HS6,
                  destination,
                  year
                  
              ) AS lag_value_t1,
        LAG(value, 2) OVER (
                PARTITION BY city_prod, HS6 
                ORDER BY 
                  city_prod,
                  HS6,
                  year
              ) AS lag_value_t2,      
        FROM aggregate
        ) AS lag
        INNER JOIN (SELECT DISTINCT(citycn) as citycn, cityen,geocode4_corr FROM China.city_cn_en )AS city_cn_en
        ON city_cn_en.citycn = lag.city_prod
        LEFT JOIN China.country_cn_en ON country_cn_en.Country_cn = lag.destination
       ) 
       SELECT 
          add_var.year, 
          add_var.geocode4_corr, 
          add_var.HS6, 
          add_var.ISO_alpha, 
          growth_export_ckjt_1 
        FROM 
          add_var 
          INNER JOIN (
            SELECT 
              year, 
              geocode4_corr, 
              HS6, 
              ISO_alpha, 
              COUNT(*) as cnt 
            FROM 
              add_var 
            GROUP BY 
              year, 
              geocode4_corr, 
              HS6, 
              ISO_alpha 
            HAVING 
              COUNT(*) = 1 
              AND ISO_alpha IS NOT NULL
          ) as no_dup ON add_var.year = no_dup.year 
          AND add_var.geocode4_corr = no_dup.geocode4_corr 
          AND add_var.HS6 = no_dup.HS6 
          AND add_var.ISO_alpha = no_dup.ISO_alpha
      )
  )      
"""
```

### Step 2: Lag Export growth by city, product

- Table name: lag_export_growth_ck
- Observation: 1,486,757

lag_export_growth_ck,lag_foreign_export_ckjr,lag_foreign_export_ckj,lag_soe_export_ckj,lag_soe_export_ckj

```python
query = """
WITH merge_data AS (
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value,
    --CASE WHEN Trade_type = '进料加工贸易' 
    --OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime,
    --CASE WHEN Business_type = '国有企业' OR Business_type = '国有' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership,
    --CASE WHEN Business_type = '外商独资企业' OR Business_type = '独资' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership
  FROM 
    `China.tradedata_*` 
  WHERE 
    (
      _TABLE_SUFFIX BETWEEN '2000' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND (
      Trade_type = '进料加工贸易' 
      OR Trade_type = '一般贸易' 
      OR Trade_type = '来料加工装配贸易'
    ) 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '国有' 
      OR Business_type = '私营' 
      OR Business_type = '集体'
      OR Business_type = '独资'
      OR Business_type = '外商独资企业'
      
    ) 
  UNION ALL 
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value,
    --CASE WHEN Trade_type = '进料加工贸易' 
    --OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime,
    --CASE WHEN Business_type = '国有企业' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership,
    --CASE WHEN Business_type = '外商独资企业' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership
    
  FROM 
    `China.tradedata_*` 
  WHERE 
    NOT(
      _TABLE_SUFFIX BETWEEN '2000' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业'
      OR Business_type = '外商独资企业'
      
    )
) 
SELECT 
      * 
    FROM 
      (
        WITH aggregate AS (
          SELECT  
            city_prod, 
            year, 
            --regime,
            --SOE_ownership,
            HS6, 
            --destination, 
            -- SUM(Quantity) as Quantity, 
            SUM(value) as value 
          FROM 
            merge_data 
          GROUP BY 
            year, 
            --regime,
            --SOE_ownership,
            HS6, 
            city_prod
            --destination
        )
        SELECT cityen, geocode4_corr,year, HS6, 
        value, CASE WHEN SAFE_DIVIDE((lag_value_t1 - lag_value_t2), lag_value_t2) IS NULL THEN 1
        ELSE SAFE_DIVIDE((lag_value_t1 - lag_value_t2), lag_value_t2) END as growth_export_ckt_1
        FROM (
        SELECT
        city_prod, HS6,year, --,destination,
        value,
        LAG(value, 1) OVER (
                PARTITION BY city_prod, HS6-- destination 
                ORDER BY 
                  city_prod,
                  HS6,
                  --destination,
                  year
                  
              ) AS lag_value_t1,
        LAG(value, 2) OVER (
                PARTITION BY city_prod, HS6 
                ORDER BY 
                  city_prod,
                  HS6,
                  year
              ) AS lag_value_t2,      
        FROM aggregate
        ) AS lag
        INNER JOIN (SELECT DISTINCT(citycn) as citycn, cityen,geocode4_corr FROM China.city_cn_en )AS city_cn_en
        ON city_cn_en.citycn = lag.city_prod
       ) 
-- 1 486 757        
"""
```

### Step 3: Lag foreign export share by city, product, destination, regime

- Table name: lag_foreign_export_ckjr
- Observation: 3,109,057



```python
query = """
WITH merge_data AS (
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value, 
    CASE WHEN Trade_type = '进料加工贸易' 
    OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime, 
    --CASE WHEN Business_type = '国有企业' OR Business_type = '国有' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership,
    CASE WHEN Business_type = '外商独资企业' 
    OR Business_type = '独资' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership 
  FROM 
    `China.tradedata_*` 
  WHERE 
    (
      _TABLE_SUFFIX BETWEEN '2002' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND (
      Trade_type = '进料加工贸易' 
      OR Trade_type = '一般贸易' 
      OR Trade_type = '来料加工装配贸易'
    ) 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '国有' 
      OR Business_type = '私营' 
      OR Business_type = '集体' 
      OR Business_type = '独资' 
      OR Business_type = '外商独资企业'
    ) 
  UNION ALL 
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value, 
    CASE WHEN Trade_type = '进料加工贸易' 
    OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime, 
    --CASE WHEN Business_type = '国有企业' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership,
    CASE WHEN Business_type = '外商独资企业' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership 
  FROM 
    `China.tradedata_*` 
  WHERE 
    NOT(
      _TABLE_SUFFIX BETWEEN '2000' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '外商独资企业'
    )
) 
SELECT 
  * 
FROM 
  (
    WITH aggregate AS (
      SELECT 
        city_prod, 
        year, 
        regime, 
        foreign_ownership, 
        HS6, 
        destination, 
        SUM(Quantity) as quantity
        --SUM(value) as value 
      FROM 
        merge_data 
      WHERE 
        foreign_ownership = 'FOREIGN' 
      GROUP BY 
        year, 
        regime, 
        foreign_ownership, 
        HS6, 
        city_prod, 
        destination
    ) 
    SELECT 
      * 
    FROM 
      (
        WITH add_var AS (
          SELECT 
            aggregate.year, 
            geocode4_corr, 
            aggregate.regime, 
            aggregate.HS6, 
            ISO_alpha, 
            foreign_ownership, 
            LAG(quantity, 1) OVER (
              PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
            ) as lag_quantity, 
            LAG(total_quantity, 1) OVER (
              PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
            ) as lag_total_quantity, 
            CASE WHEN SAFE_DIVIDE(
              LAG(quantity, 1) OVER (
                PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
              ), 
              LAG(total_quantity, 1) OVER (
                PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
              )
            ) IS NULL THEN 0 ELSE SAFE_DIVIDE(
              LAG(quantity, 1) OVER (
                PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
              ), 
              LAG(total_quantity, 1) OVER (
                PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
              )
            ) END as lag_foreign_export_share_ckjr 
          FROM 
            aggregate 
            INNER JOIN (
              SELECT 
                city_prod, 
                year, 
                regime, 
                HS6, 
                destination, 
                SUM(quantity) as total_quantity
              FROM 
                merge_data 
              GROUP BY 
                year, 
                regime, 
                HS6, 
                city_prod, 
                destination
            ) as total ON aggregate.city_prod = total.city_prod 
            AND aggregate.year = total.year 
            AND aggregate.regime = total.regime 
            AND aggregate.HS6 = total.HS6 
            AND aggregate.destination = total.destination 
            INNER JOIN (
              SELECT 
                DISTINCT(citycn) as citycn, 
                cityen, 
                geocode4_corr 
              FROM 
                China.city_cn_en
            ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod 
            INNER JOIN China.country_cn_en ON country_cn_en.Country_cn = aggregate.destination
        ) 
        SELECT -- Remove duplicates
          add_var.year, 
          add_var.geocode4_corr, 
          add_var.regime, 
          add_var.HS6, 
          add_var.ISO_alpha, 
          lag_foreign_export_share_ckjr 
        FROM 
          add_var 
          INNER JOIN (
            SELECT 
              year, 
              geocode4_corr, 
              regime, 
              HS6, 
              ISO_alpha, 
              COUNT(*) as cnt 
            FROM 
              add_var 
            GROUP BY 
              year, 
              geocode4_corr, 
              regime, 
              HS6, 
              ISO_alpha 
            HAVING 
              COUNT(*) = 1 
              AND ISO_alpha IS NOT NULL
          ) as no_dup ON add_var.year = no_dup.year 
          AND add_var.geocode4_corr = no_dup.geocode4_corr 
          AND add_var.regime = no_dup.regime 
          AND add_var.HS6 = no_dup.HS6 
          AND add_var.ISO_alpha = no_dup.ISO_alpha
      )
  )


"""
```

### Step 4: Lag foreign export share by city, product, regime

- Table name: lag_foreign_export_ckr
- Observation: 377,516



```python
query = """
WITH merge_data AS (
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value, 
    CASE WHEN Trade_type = '进料加工贸易' 
    OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime, 
    --CASE WHEN Business_type = '国有企业' 
    --OR Business_type = '国有' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership, 
    CASE WHEN Business_type = '外商独资企业' 
    OR Business_type = '独资' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership 
  FROM 
    `China.tradedata_*` 
  WHERE 
    (
      _TABLE_SUFFIX BETWEEN '2002' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND (
      Trade_type = '进料加工贸易' 
      OR Trade_type = '一般贸易' 
      OR Trade_type = '来料加工装配贸易'
    ) 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '国有' 
      OR Business_type = '私营' 
      OR Business_type = '集体' 
      OR Business_type = '独资' 
      OR Business_type = '外商独资企业'
    ) 
  UNION ALL 
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value, 
    CASE WHEN Trade_type = '进料加工贸易' 
    OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime, 
    --CASE WHEN Business_type = '国有企业' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership, 
    CASE WHEN Business_type = '外商独资企业' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership 
  FROM 
    `China.tradedata_*` 
  WHERE 
    NOT(
      _TABLE_SUFFIX BETWEEN '2000' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '外商独资企业'
    )
) 
SELECT 
  * 
FROM 
  (
    WITH share_foreign_quantity AS(
      SELECT 
        foreign_total.city_prod, 
        foreign_total.year, 
        CAST(
          CAST(foreign_total.year AS int64) -1 AS string
        ) as year_lag, 
        foreign_total.regime, 
        foreign_total.foreign_ownership, 
        foreign_total.HS6, 
        quantity, 
        total_quantity, 
        SAFE_DIVIDE(quantity, total_quantity) as share_foreign 
      FROM 
        (
          SELECT 
            city_prod, 
            year, 
            regime, 
            foreign_ownership, 
            HS6, 
            --destination, 
            SUM(Quantity) as quantity 
          FROM 
            merge_data 
          WHERE 
            foreign_ownership = 'FOREIGN' 
          GROUP BY 
            year, 
            regime, 
            foreign_ownership, 
            HS6, 
            city_prod
        ) as foreign_total 
        INNER JOIN (
          SELECT 
            city_prod, 
            year, 
            regime, 
            HS6, 
            --destination, 
            SUM(Quantity) as total_quantity 
          FROM 
            merge_data 
          GROUP BY 
            year, 
            regime, 
            HS6, 
            city_prod
        ) as total ON foreign_total.city_prod = total.city_prod 
        AND foreign_total.year = total.year 
        AND foreign_total.regime = total.regime 
        AND foreign_total.HS6 = total.HS6
    ) 
    SELECT 
      cityen, 
      geocode4_corr,
      share_foreign_quantity.year, 
      share_foreign_quantity.regime, 
      share_foreign_quantity.HS6, 
      share_foreign_quantity.share_foreign as foreign_export_share_ckr, 
      share_foreign_quantity_lag.share_foreign as lag_foreign_export_share_ckr 
    FROM 
      share_foreign_quantity 
      INNER JOIN (
        SELECT 
          city_prod, 
          year, 
          regime, 
          foreign_ownership, 
          HS6, 
          share_foreign
        FROM 
          share_foreign_quantity
      ) as share_foreign_quantity_lag ON 
      share_foreign_quantity_lag.city_prod = share_foreign_quantity.city_prod 
      AND share_foreign_quantity_lag.year = share_foreign_quantity.year_lag 
      AND share_foreign_quantity_lag.regime = share_foreign_quantity.regime 
      AND share_foreign_quantity_lag.foreign_ownership = share_foreign_quantity.foreign_ownership 
      AND share_foreign_quantity_lag.HS6 = share_foreign_quantity.HS6
      INNER JOIN (
              SELECT 
                DISTINCT(citycn) as citycn, 
                cityen, 
                geocode4_corr 
              FROM 
                China.city_cn_en
            ) AS city_cn_en ON city_cn_en.citycn = share_foreign_quantity.city_prod 
  ) -- 377516

"""
```

Distribution

```python
query = """
select
  percentiles[offset(10)] as p10,
  percentiles[offset(25)] as p25,
  percentiles[offset(50)] as p50,
  percentiles[offset(60)] as p60,
  percentiles[offset(70)] as p70,
  percentiles[offset(80)] as p80,
  percentiles[offset(90)] as p90,
  percentiles[offset(95)] as p95
from (
  select approx_quantiles(lag_foreign_export_share_ckr, 100) percentiles
 FROM `temporary.lag_foreign_export_ckr`
)
"""
```

### Step 5: Lag SOE export share by city, product, destination, regime

- Table name: lag_soe_export_ckjr
- Observation: 2,75,5797



```python
query = """
WITH merge_data AS (
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value, 
    CASE WHEN Trade_type = '进料加工贸易' 
    OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime, 
    CASE WHEN Business_type = '国有企业' OR Business_type = '国有' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership,
    --CASE WHEN Business_type = '外商独资企业' 
    --OR Business_type = '独资' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership 
  FROM 
    `China.tradedata_*` 
  WHERE 
    (
      _TABLE_SUFFIX BETWEEN '2002' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND (
      Trade_type = '进料加工贸易' 
      OR Trade_type = '一般贸易' 
      OR Trade_type = '来料加工装配贸易'
    ) 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '国有' 
      OR Business_type = '私营' 
      OR Business_type = '集体' 
      OR Business_type = '独资' 
      OR Business_type = '外商独资企业'
    ) 
  UNION ALL 
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value, 
    CASE WHEN Trade_type = '进料加工贸易' 
    OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime, 
    CASE WHEN Business_type = '国有企业' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership,
    --CASE WHEN Business_type = '外商独资企业' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership 
  FROM 
    `China.tradedata_*` 
  WHERE 
    NOT(
      _TABLE_SUFFIX BETWEEN '2000' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '外商独资企业'
    )
) 
SELECT 
  * 
FROM 
  (
    WITH aggregate AS (
      SELECT 
        city_prod, 
        year, 
        regime, 
        soe_ownership, 
        HS6, 
        destination, 
        SUM(Quantity) as quantity
        --SUM(value) as value 
      FROM 
        merge_data 
      WHERE 
        soe_ownership = 'SOE' 
      GROUP BY 
        year, 
        regime, 
        soe_ownership, 
        HS6, 
        city_prod, 
        destination
    ) 
    SELECT 
      * 
    FROM 
      (
        WITH add_var AS (
          SELECT 
            aggregate.year, 
            geocode4_corr, 
            aggregate.regime, 
            aggregate.HS6, 
            ISO_alpha, 
            soe_ownership, 
            LAG(quantity, 1) OVER (
              PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
            ) as lag_quantity, 
            LAG(total_quantity, 1) OVER (
              PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
            ) as lag_total_quantity, 
            CASE WHEN SAFE_DIVIDE(
              LAG(quantity, 1) OVER (
                PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
              ), 
              LAG(total_quantity, 1) OVER (
                PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
              )
            ) IS NULL THEN 0 ELSE SAFE_DIVIDE(
              LAG(quantity, 1) OVER (
                PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
              ), 
              LAG(total_quantity, 1) OVER (
                PARTITION BY geocode4_corr, 
                aggregate.HS6, 
                ISO_alpha, 
                aggregate.regime 
                ORDER BY 
                  geocode4_corr, 
                  aggregate.HS6, 
                  ISO_alpha,
                  aggregate.regime,
                  aggregate.year
              )
            ) END as lag_soe_export_share_ckjr 
          FROM 
            aggregate 
            INNER JOIN (
              SELECT 
                city_prod, 
                year, 
                regime, 
                HS6, 
                destination, 
                SUM(quantity) as total_quantity
              FROM 
                merge_data 
              GROUP BY 
                year, 
                regime, 
                HS6, 
                city_prod, 
                destination
            ) as total ON aggregate.city_prod = total.city_prod 
            AND aggregate.year = total.year 
            AND aggregate.regime = total.regime 
            AND aggregate.HS6 = total.HS6 
            AND aggregate.destination = total.destination 
            INNER JOIN (
              SELECT 
                DISTINCT(citycn) as citycn, 
                cityen, 
                geocode4_corr 
              FROM 
                China.city_cn_en
            ) AS city_cn_en ON city_cn_en.citycn = aggregate.city_prod 
            INNER JOIN China.country_cn_en ON country_cn_en.Country_cn = aggregate.destination
        ) 
        SELECT -- Remove duplicates
          add_var.year, 
          add_var.geocode4_corr, 
          add_var.regime, 
          add_var.HS6, 
          add_var.ISO_alpha, 
          lag_soe_export_share_ckjr 
        FROM 
          add_var 
          INNER JOIN (
            SELECT 
              year, 
              geocode4_corr, 
              regime, 
              HS6, 
              ISO_alpha, 
              COUNT(*) as cnt 
            FROM 
              add_var 
            GROUP BY 
              year, 
              geocode4_corr, 
              regime, 
              HS6, 
              ISO_alpha 
            HAVING 
              COUNT(*) = 1 
              AND ISO_alpha IS NOT NULL
          ) as no_dup ON add_var.year = no_dup.year 
          AND add_var.geocode4_corr = no_dup.geocode4_corr 
          AND add_var.regime = no_dup.regime 
          AND add_var.HS6 = no_dup.HS6 
          AND add_var.ISO_alpha = no_dup.ISO_alpha
      )
  )


"""
```

### Step 6: Lag SOE export share by city, product, regime

- Table name: lag_soe_export_ckr
- Observation: 364,314

```python
query = """
WITH merge_data AS (
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value, 
    CASE WHEN Trade_type = '进料加工贸易' 
    OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime, 
    CASE WHEN Business_type = '国有企业' 
    OR Business_type = '国有' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership, 
    --CASE WHEN Business_type = '外商独资企业' 
    --OR Business_type = '独资' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership 
  FROM 
    `China.tradedata_*` 
  WHERE 
    (
      _TABLE_SUFFIX BETWEEN '2002' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND (
      Trade_type = '进料加工贸易' 
      OR Trade_type = '一般贸易' 
      OR Trade_type = '来料加工装配贸易'
    ) 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '国有' 
      OR Business_type = '私营' 
      OR Business_type = '集体' 
      OR Business_type = '独资' 
      OR Business_type = '外商独资企业'
    ) 
  UNION ALL 
  SELECT 
    CAST(Date AS STRING) as year, 
    ID, 
    Trade_type, 
    Business_type, 
    CASE WHEN length(
      CAST(HS AS STRING)
    ) < 5 THEN CONCAT(
      "0", 
      CAST(HS AS STRING)
    ) ELSE CAST(HS AS STRING) END as HS6, 
    city_prod, 
    Origin_or_destination as destination, 
    Quantity, 
    value, 
    CASE WHEN Trade_type = '进料加工贸易' 
    OR Trade_type = '一般贸易' THEN 'Eligible' ELSE 'Not_Eligible' END as regime, 
    CASE WHEN Business_type = '国有企业' THEN 'SOE' ELSE 'NO_SOE' END as SOE_ownership, 
    --CASE WHEN Business_type = '外商独资企业' THEN 'FOREIGN' ELSE 'NO_FOREIGN' END as foreign_ownership 
  FROM 
    `China.tradedata_*` 
  WHERE 
    NOT(
      _TABLE_SUFFIX BETWEEN '2000' 
      AND '2006'
    ) 
    AND imp_exp = '出口' 
    AND intermediate = 'No' 
    AND (
      Business_type = '国有企业' 
      OR Business_type = '私营企业' 
      OR Business_type = '集体企业' 
      OR Business_type = '外商独资企业'
    )
) 
SELECT 
  * 
FROM 
  (
    WITH share_soe_quantity AS(
      SELECT 
        soe_total.city_prod, 
        soe_total.year, 
        CAST(
          CAST(soe_total.year AS int64) -1 AS string
        ) as year_lag, 
        soe_total.regime, 
        soe_total.soe_ownership, 
        soe_total.HS6, 
        quantity, 
        total_quantity, 
        SAFE_DIVIDE(quantity, total_quantity) as share_soe 
      FROM 
        (
          SELECT 
            city_prod, 
            year, 
            regime, 
            soe_ownership, 
            HS6, 
            --destination, 
            SUM(Quantity) as quantity 
          FROM 
            merge_data 
          WHERE 
            soe_ownership = 'SOE' 
          GROUP BY 
            year, 
            regime, 
            soe_ownership, 
            HS6, 
            city_prod
        ) as soe_total 
        INNER JOIN (
          SELECT 
            city_prod, 
            year, 
            regime, 
            HS6, 
            --destination, 
            SUM(Quantity) as total_quantity 
          FROM 
            merge_data 
          GROUP BY 
            year, 
            regime, 
            HS6, 
            city_prod
        ) as total ON soe_total.city_prod = total.city_prod 
        AND soe_total.year = total.year 
        AND soe_total.regime = total.regime 
        AND soe_total.HS6 = total.HS6
    ) 
    SELECT 
      cityen, 
      geocode4_corr,
      share_soe_quantity.year, 
      share_soe_quantity.regime, 
      share_soe_quantity.HS6, 
      share_soe_quantity.share_soe as soe_export_share_ckr, 
      share_soe_quantity_lag.share_soe as lag_soe_export_share_ckr 
    FROM 
      share_soe_quantity 
      INNER JOIN (
        SELECT 
          city_prod, 
          year, 
          regime, 
          soe_ownership, 
          HS6, 
          share_soe 
        FROM 
          share_soe_quantity
      ) as share_soe_quantity_lag ON share_soe_quantity_lag.city_prod = share_soe_quantity.city_prod 
      AND share_soe_quantity_lag.year = share_soe_quantity.year_lag 
      AND share_soe_quantity_lag.regime = share_soe_quantity.regime 
      AND share_soe_quantity_lag.soe_ownership = share_soe_quantity.soe_ownership 
      AND share_soe_quantity_lag.HS6 = share_soe_quantity.HS6
      INNER JOIN (
              SELECT 
                DISTINCT(citycn) as citycn, 
                cityen, 
                geocode4_corr 
              FROM 
                China.city_cn_en
            ) AS city_cn_en ON city_cn_en.citycn = share_soe_quantity.city_prod 
  ) -- 944951, 413965, 364314

"""
```

Distribution

```python
query = """
select
  percentiles[offset(10)] as p10,
  percentiles[offset(25)] as p25,
  percentiles[offset(50)] as p50,
  percentiles[offset(60)] as p60,
  percentiles[offset(70)] as p70,
  percentiles[offset(80)] as p80,
  percentiles[offset(90)] as p90,
  percentiles[offset(95)] as p95
from (
  select approx_quantiles(lag_soe_export_share_ckr, 100) percentiles
 FROM `temporary.lag_soe_export_ckr`
)
"""
```

### Step 7: Merge all tables

Merge all tables with `quality_vat_export_2003_2010` 

Not, we need to cast the following variables because it's int in `quality_vat_export_2003_2010`:

- geocode4_corr
- year
- HS6

- the new table name is `quality_vat_export_covariate_2003_2010`

```python
query = """
WITH change_format AS(
SELECT cityen, CAST(geocode4_corr AS string) AS geocode4_corr, CAST(year AS string) AS year, regime, CAST(HS6 AS string) AS HS6, HS4, HS3, Country_en, ISO_alpha, Quantity, value, unit_price, kandhelwal_quality, price_adjusted_quality, lag_tax_rebate, ln_lag_tax_rebate, lag_import_tax, ln_lag_import_tax, sigma, sigma_price, y, prediction, residual, FE_ck, FE_cst, FE_ckr, FE_csrt, FE_kt, FE_pj, FE_jt, FE_ct
FROM `China.quality_vat_export_2003_2010`
)
SELECT change_format.cityen, change_format.geocode4_corr, change_format.year, change_format.regime, change_format.HS6, HS4, HS3, change_format.Country_en, change_format.ISO_alpha, Quantity, change_format.value, unit_price, kandhelwal_quality, price_adjusted_quality, lag_tax_rebate, ln_lag_tax_rebate, lag_import_tax, ln_lag_import_tax, sigma, sigma_price, y, prediction, residual, FE_ck, FE_cst, FE_ckr, FE_csrt, FE_kt, FE_pj, FE_jt, FE_ct, 
CASE WHEN growth_export_ckt_1 IS NULL THEN 0 ELSE growth_export_ckt_1 END AS growth_export_ckt_1,
CASE WHEN growth_export_ckjt_1 IS NULL THEN 0 ELSE growth_export_ckjt_1 END AS growth_export_ckjt_1,
CASE WHEN lag_foreign_export_share_ckr IS NULL THEN 0 ELSE lag_foreign_export_share_ckr END AS lag_foreign_export_share_ckr,
CASE WHEN lag_soe_export_share_ckr IS NULL THEN 0 ELSE lag_soe_export_share_ckr END AS lag_soe_export_share_ckr,
CASE WHEN lag_foreign_export_share_ckjr IS NULL THEN 0 ELSE lag_foreign_export_share_ckjr END AS lag_foreign_export_share_ckjr,
CASE WHEN lag_soe_export_share_ckjr IS NULL THEN 0 ELSE lag_soe_export_share_ckjr END AS lag_soe_export_share_ckjr

FROM change_format
LEFT JOIN `temporary.lag_export_growth_ck` 
ON change_format.geocode4_corr = `temporary.lag_export_growth_ck`.geocode4_corr AND
change_format.year = `temporary.lag_export_growth_ck`.year AND
change_format.HS6 = `temporary.lag_export_growth_ck`.HS6

LEFT JOIN `temporary.lag_export_growth_ckj` 
ON change_format.geocode4_corr = `temporary.lag_export_growth_ckj`.geocode4_corr AND
change_format.year = `temporary.lag_export_growth_ckj`.year AND
change_format.HS6 = `temporary.lag_export_growth_ckj`.HS6 AND
change_format.ISO_alpha = `temporary.lag_export_growth_ckj`.ISO_alpha

LEFT JOIN `temporary.lag_foreign_export_ckr` 
ON change_format.geocode4_corr = `temporary.lag_foreign_export_ckr`.geocode4_corr AND
change_format.year = `temporary.lag_foreign_export_ckr`.year AND
change_format.HS6 = `temporary.lag_foreign_export_ckr`.HS6 AND
change_format.regime = `temporary.lag_foreign_export_ckr`.regime

LEFT JOIN `temporary.lag_soe_export_ckr` 
ON change_format.geocode4_corr = `temporary.lag_soe_export_ckr`.geocode4_corr AND
change_format.year = `temporary.lag_soe_export_ckr`.year AND
change_format.HS6 = `temporary.lag_soe_export_ckr`.HS6 AND
change_format.regime = `temporary.lag_soe_export_ckr`.regime

LEFT JOIN `temporary.lag_foreign_export_ckjr` 
ON change_format.geocode4_corr = `temporary.lag_foreign_export_ckjr`.geocode4_corr AND
change_format.year = `temporary.lag_foreign_export_ckjr`.year AND
change_format.HS6 = `temporary.lag_foreign_export_ckjr`.HS6 AND
change_format.regime = `temporary.lag_foreign_export_ckjr`.regime AND
change_format.ISO_alpha = `temporary.lag_foreign_export_ckjr`.ISO_alpha

LEFT JOIN `temporary.lag_soe_export_ckjr` 
ON change_format.geocode4_corr = `temporary.lag_soe_export_ckjr`.geocode4_corr AND
change_format.year = `temporary.lag_soe_export_ckjr`.year AND
change_format.HS6 = `temporary.lag_soe_export_ckjr`.HS6 AND
change_format.regime = `temporary.lag_soe_export_ckjr`.regime AND
change_format.ISO_alpha = `temporary.lag_soe_export_ckjr`.ISO_alpha
"""
```

```python
df_quality.shape
```

## Brief Description

In the query below, we compute the distribution of the SOE and foreign share

- https://popsql.com/learn-sql/bigquery/how-to-calculate-percentiles-in-bigquery
- https://cloud.google.com/dataprep/docs/html/PERCENTILE-Function_156863254

```python
query = """
select
  percentiles[offset(10)] as p10,
  percentiles[offset(25)] as p25,
  percentiles[offset(50)] as p50,
  percentiles[offset(60)] as p60,
  percentiles[offset(70)] as p70,
  percentiles[offset(80)] as p80,
  percentiles[offset(90)] as p90,
  percentiles[offset(95)] as p95
from (
  select approx_quantiles(lag_foreign_export_share_ckr, 100) percentiles
 FROM `China.quality_vat_export_covariate_2003_2010`
)
"""
```

```python
query = """
select
  percentiles[offset(10)] as p10,
  percentiles[offset(25)] as p25,
  percentiles[offset(50)] as p50,
  percentiles[offset(60)] as p60,
  percentiles[offset(70)] as p70,
  percentiles[offset(80)] as p80,
  percentiles[offset(90)] as p90,
  percentiles[offset(95)] as p95
from (
  select approx_quantiles(lag_soe_export_share_ckr, 100) percentiles
 FROM `China.quality_vat_export_covariate_2003_2010`
)
"""
```

# Upload to cloud

The dataset is ready to be shared with your colleagues. 

## Output 

- Filename: quality_vat_export_2003_2010
- Link: https://console.cloud.google.com/bigquery?project=valid-pagoda-132423&p=valid-pagoda-132423&d=China&t=quality_vat_export_2003_2010&page=table
- Cloud Storage: 
- Type:  Table


```python
df_quality.to_csv('quality_vat_export_2003_2010.csv', index = False)
```

```python
bucket_name = 'chinese_data'
destination_blob_name = 'paper_project/Processed'
source_file_name = 'quality_vat_export_2003_2010.csv'
```

```python
gcp.delete_blob(bucket_name, 'paper_project/Processed/quality_vat_export_2003_2010.csv')
```

```python
gcp.upload_blob(bucket_name, destination_blob_name, source_file_name)
```

```python
gcp.delete_table(dataset_name = 'China', name_table = 'quality_vat_export_2003_2010')
```

```python
bucket_gcs ='chinese_data/paper_project/Processed/quality_vat_export_2003_2010.csv'
gcp.move_to_bq_autodetect(dataset_name= 'China',
							 name_table= 'quality_vat_export_2003_2010',
							 bucket_gcs=bucket_gcs)
```

```python
import shutil
shutil.move('quality_vat_export_2003_2010.csv',
            '../00_Data_catalogue/temporary_local_data/quality_vat_export_2003_2010.csv')
```

# Add data to catalogue

Now that the dataset is ready, you need to add the underlying information to the data catalogue. The data catalogue is stored in [Coda](https://coda.io/d/MasterFile-Database_dvfMWDBnHh8/MetaDatabase_suYFO#_ludIZ), more precisely, in the table named `DataSource`. 

The cells below helps you to push the information directly to the table using Coda API.

The columns are as follow:

- `Storage`: Define the location of the table
    - GBQ, GS, MongoDB
- `Theme`: Define a theme attached to the table
    - Accountancy, Complexity, Correspondance, Customer_prediction, Distance, Environment, Finance, Macro, Production, Productivity, Survey, Trade
- `Database`: Name of the dataset. Use only for GBQ or MongoDB (collection)
    - Business, China, Steamforged, Trade
- `Path`:A URL with the path of the location of the dataset
- `Filename`: Name of the table
- `Description`: Description of the table. Be very specific. 
- `Source_data`: A list of the data sources used to construct the table.
- `Link_methodology`: URL linked to the notebook
- `Dataset_documentation`: Github repository attached to the table
- `Status`: Status of the table. 
    - `Closed` if the table won't be altered in the future
    - `Active` if the table will be altered in the future
- `Profiling`: Specify if the user created a Pandas profiling
    - `True` if the profiling has been created
    - `False` otherwise
- `Profiling_URL`: Profiling URL (link to Github). Always located in `Data_catalogue/table_profiling`
- `JupyterStudio`: Specify if the user created a notebook to open the studio
    - `True` if the notebook has been created
    - `False` otherwise
- `JupyterStudio_launcher`: Notebook URL (link to Github). Always located in `Notebooks_Ready_to_use_studio`
- `Nb_projects`: Number of projects using this dataset. A Coda formula. Do not update this row
- `Created on`: Date of creation. A Coda formula. Do not update this row

Remember to commit in GitHub to activate the URL link for the profiling and Studio

```python
Storage = 'GBQ'
Theme = 'Trade' 
Database = 'China'
Description = "The table is related to the paper that studies the effect of industrial policy in China, the VAT export tax, on the quality upgrading. We use Chinese transaction data for 2002-2006 to isolate the causal impact of the exogenous variation of VAT refund tax and within firm-product change in HS6 exported quality products."
Filename = 'quality_vat_export_2003_2010'
Status = 'Active'
```

The next cell pushes the information to [Coda](https://coda.io/d/MasterFile-Database_dvfMWDBnHh8/Test-API_suDBp#API_tuDK4)

```python
regex = r"(.*)/(.*)"
path = os.getcwd()
parent_path = Path(path).parent
test_str = str(parent_path)
matches = re.search(regex, test_str)
github_repo = matches.group(2)
Source_data = ['VAT_export_2002_2010', 'Sigmas_3digit_China', 'city_cn_en']

Profiling = True
if Profiling:
    Profiling_URL = 'http://htmlpreview.github.io/?https://github.com/' \
    'thomaspernet/{}/blob/master/Data_catalogue/table_profiling/{}.html'.format(github_repo,
                                                                               Filename)
else:
    Profiling_URL = ''
JupyterStudio = False
if JupyterStudio:
    JupyterStudio_URL = '"https://mybinder.org/v2/gh/thomaspernet/{0}/' \
    'master?filepath=Notebooks_Ready_to_use_studio%2F{1}_studio.ipynb'.format(github_repo, Filename)
else:
    JupyterStudio_URL = ''
### BigQuery only 
path_url = 'https://console.cloud.google.com/bigquery?project=valid-pagoda-132423' \
'&p=valid-pagoda-132423&d=China&t={}&page=table'.format(Filename)

Link_methodology = 'https://nbviewer.jupyter.org/github/thomaspernet/' \
    '{0}/blob/master/Data_preprocessing/' \
    '{1}.ipynb'.format(github_repo,
    Filename)

Dataset_documentation = 'https://github.com/thomaspernet/{}'.format(github_repo)

to_add = {
    'Storage': Storage,
    'Theme': Theme,
    'Database': Database,
    'Path_url': path_url,
    'Filename': Filename,
    'Description': Description,
    'Source_data': Source_data,
    'Link_methodology': Link_methodology,
    'Dataset_documentation': Dataset_documentation,
    'Status': Status,
    'Profiling_URL': Profiling_URL,
    'JupyterStudio_launcher': JupyterStudio_URL

}
cols= []
for key, value in to_add.items():
    coda = {
    'column': key,
    'value':value
    }
    cols.append(coda)
    
###load token coda
with open('token_coda.json') as json_file:
    data = json.load(json_file)
    
token = data[0]['token'] 

uri = f'https://coda.io/apis/v1beta1/docs/vfMWDBnHh8/tables/grid-HgpAnIEhpP/rows'
headers = {'Authorization': 'Bearer {}'.format(token)}
payload = {
  'rows': [
    {
      'cells': cols,
    },
  ],
}
req = requests.post(uri, headers=headers, json=payload)
req.raise_for_status() # Throw if there was an error.
res = req.json()
```

```python
req
```