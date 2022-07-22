---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.8.0
  kernel_info:
    name: python3
  kernelspec:
    display_name: SoS
    language: sos
    name: sos
---

<!-- #region kernel="SoS" -->
# US Name
Estimate kandhelwal_quality as a function of  ln_lag_tax_rebate and others variables


# Description

- Change sign for story
- Check sign price adjusted
- Estimate table 1
- Estimate table 2
- Estimate table 4
- Estimate table 5

## Variables
### Target

kandhelwal_quality

### Features

- ln_lag_tax_rebate
- regime
- ln_lag_import_tax

## Complementary information



# Metadata

- Key: 198_VAT_rebate_quality
- Epic: Models
- US: Baseline table
- Task tag: #data-analysis
- Analytics reports: 

# Input Cloud Storage

## Table/file

**Name**

- china_vat_quality

**Github**

- https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/04_baseline_vat_quantity_covariates.md


<!-- #endregion -->

<!-- #region kernel="SoS" -->
# Connexion server
<!-- #endregion -->

```sos kernel="SoS"
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
#import seaborn as sns
import os, shutil, json
import sys
import janitor

path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-2'
bucket = 'datalake-london'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```sos kernel="SoS"
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False)
glue = service_glue.connect_glue(client = client) 
```

```sos kernel="SoS"
pandas_setting = True
if pandas_setting:
    #cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
os.environ['KMP_DUPLICATE_LIB_OK']='True'

```

```sos kernel="R"
change_target <- function(table){
    ## Regime
    check_target <- grep("regimeELIGIBLE:ln_lag_import_tax", rownames(table$coef))
    check_target_ntm <- grep("regimeELIGIBLE:c_lag_stock_ntm_w", rownames(table$coef))
    if (length(check_target) !=0) {
    ## SOE
    rownames(table$coefficients)[check_target] <- 'ln_lag_import_tax:regimeELIGIBLE'
    rownames(table$beta)[check_target] <- 'ln_lag_import_tax:regimeELIGIBLE'
    } 
    if (length(check_target_ntm) !=0) {
    ## SOE
    rownames(table$coefficients)[check_target_ntm] <- 'c_lag_stock_ntm_w:regimeELIGIBLE'
    rownames(table$beta)[check_target_ntm] <- 'c_lag_stock_ntm_w:regimeELIGIBLE'
    } 
    return (table)
}
```

<!-- #region kernel="SoS" -->
# Load tables

Since we load the data as a Pandas DataFrame, we want to pass the `dtypes`. We load the schema from Glue to guess the types
<!-- #endregion -->

```sos kernel="SoS"
db = 'chinese_trade'
table = 'china_vat_quality'
```

```sos kernel="SoS"
dtypes = {}
schema = (glue.get_table_information(database = db,
                           table = table)
          ['Table']['StorageDescriptor']['Columns']
         )
for key, value in enumerate(schema):
    if value['Type'] in ['varchar(12)',
                         'varchar(3)',
                        'varchar(14)',
                         'varchar(11)',
                        'array<bigint>',
                        'map<double,boolean>']:
        format_ = 'string'
    elif value['Type'] in ['decimal(21,5)', 'double', 'bigint', 'int', 'float']:
        format_ = 'float'
    else:
        format_ = value['Type'] 
    dtypes.update(
        {value['Name']:format_}
    )
```

```sos kernel="SoS"
download_data = True
filename = 'df_{}'.format(table)
full_path_filename = 'SQL_OUTPUT_ATHENA/CSV/{}.csv'.format(filename)
path_local = os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalog/temporary_local_data")
df_path = os.path.join(path_local, filename + '.csv')
if download_data:
    
    s3 = service_s3.connect_S3(client = client,
                          bucket = bucket, verbose = False)
    query = """
    SELECT 
  * 
FROM 
  chinese_trade.china_vat_quality 
  LEFT JOIN (
    SELECT 
      * 
    FROM 
      (
        SELECT 
          year, 
          hs6, 
          geocode4_corr, 
          country_en, 
          COUNT(
            DISTINCT(regime)
          ) as cn 
        FROM 
          chinese_trade.china_vat_quality 
        GROUP BY 
          year, 
          hs6, 
          geocode4_corr, 
          country_en
      )
  ) as cn on china_vat_quality.year = cn.year 
  and china_vat_quality.hs6 = cn.hs6
  and china_vat_quality.geocode4_corr = cn.geocode4_corr
  and china_vat_quality.country_en = cn.country_en 
  WHERE cn = 2
    """.format(db, table)
    try:
        df = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
                )
    except:
        pass
    #s3.download_file(
    #    key = full_path_filename
    #)
    #shutil.move(
    #    filename + '.csv',
    #    os.path.join(path_local, filename + '.csv')
    #)
    #s3.remove_file(full_path_filename)
    
```

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
pd.DataFrame(schema)
```

```sos kernel="SoS"
query = """
SELECT * FROM "trade"."wto_wiiw_ntm"
"""
df_ntm = (s3.run_query(
            query=query,
            database="trade",
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
          .assign(
        year_initiation = lambda x: x['year_initiation'].astype('Int64'),#.astype(str),
        year_withdrawal = lambda x: x['year_withdrawal'].astype('Int64').astype(str)
    )
          .rename(columns = {"imp_iso3":"iso_alpha", "year_initiation":'year'})
                )
df_ntm.head(2)
```

```sos kernel="SoS"
df_ntm.to_csv('ntm.csv')
```

<!-- #region kernel="SoS" -->
# Compute NTM and quality variables
<!-- #endregion -->
<!-- #region kernel="SoS" -->
## NTM 
<!-- #endregion -->

<!-- #region kernel="SoS" -->
### China import and world
<!-- #endregion -->

```sos kernel="SoS"
df.shape
```

```sos kernel="SoS"
df_ntm.dropna(subset = ['year']).shape
```

```sos kernel="SoS"
df_ntm_world = (
    df_ntm
    .loc[lambda x: x['ntm'].isin([
        'TBT',
        #'SPS',
        'ADP' ### 
    ])]
    .reindex(columns=["iso_alpha", "id", "year", "hs_combined"])
    .dropna(subset = ['year'])
    .assign(hs6=lambda x: x["hs_combined"].str.split("|"))
    .explode("hs6")
    .groupby(["iso_alpha", "year", "hs6"])
    .agg({"id": "nunique"})
    .rename(columns={"id": "count"})
    .sort_values(by=["iso_alpha", "year", "hs6"])
    .reset_index()
    
    .pivot_table(
        values="count",
        index=["iso_alpha","hs6"],
        columns="year",
        aggfunc="sum",
        fill_value=0,
    )
    .stack()
    .reset_index()
    .rename(columns={0: "count_ntm"})
    .assign(
        stock_ntm=lambda x: x.groupby(["iso_alpha","hs6"])[
            "count_ntm"
        ].transform("cumsum"),
        new_ntm=lambda x: np.where(x["count_ntm"] > 0, 'TRUE', 'FALSE'),
        active_ntm=lambda x: np.where(x["stock_ntm"] > 0, 'TRUE', 'FALSE'),
    )
    #.loc[lambda x: x['hs6'].isin(['400270'])]

)
df_ntm_world[["count_ntm",'stock_ntm','new_ntm','active_ntm']].describe()
```

```sos kernel="SoS"
df_ntm_world[['new_ntm']].value_counts()
```

```sos kernel="SoS"
df_ntm_world[['active_ntm']].value_counts()
```

```sos kernel="SoS"
df_ntm_china =(
    df_ntm_world
    .loc[lambda x: x['iso_alpha'].isin(['CHN'])]
    .rename(
    columns = {
        'count_ntm':'count_ntm_china',
        'stock_ntm':'stock_ntm_china',
        'new_ntm':'new_ntm_china',
        'active_ntm':'active_ntm_china',
    })
)
df_ntm_china.head()
```

<!-- #region kernel="SoS" -->
- 1) ADP - Antidumping # CHINA
- 2) CVD - Countervailing duties
- 3) EXS - Export subsidies
- 4) QRS - Quantitative Restrictions
- 5) SFG - Safeguards
- 6) SPS - Sanitary and Phytosanitary Measures # CHINA
- 7) SSG - Special Safeguards (agriculture)
- 8) STE - State trading enterprises
- 9) TBT - Technical barriers to trade # CHINA
- 10) TRQ - Tariff-rate quotas

----- 

- count_ntm_china:  Number of new policy per year
- stock_ntm_china:  Number of active policy 
- new_ntm_china: Equal to 1 if a new policy is added
- active_ntm_china: Equal to 1 if the policy is active
<!-- #endregion -->

```sos kernel="SoS"
#(
#    df_ntm
#    .loc[lambda x: x["iso_alpha"].isin(["CHN"])]
#    .loc[lambda x: x['ntm'].isin(['TBT'])]
#)
```

```sos kernel="SoS"
list_ntm = []
for i in [["TBT"], ["SPS"], ["TBT", "SPS"], ["ADP"]]:
    df_temp = (
        df_ntm.loc[lambda x: x["iso_alpha"].isin(["CHN"])]
        .loc[lambda x: x["ntm"].isin(i)]
        .reindex(columns=["iso_alpha", "id", "year", "hs_combined"])
        .dropna(subset=["year"])
        .assign(hs6=lambda x: x["hs_combined"].str.split("|"))
        .explode("hs6")
        .groupby(["iso_alpha", "year", "hs6"])
        .agg({"id": "nunique"})
        .rename(columns={"id": "count"})
        .sort_values(by=["iso_alpha", "year", "hs6"])
        .reset_index()
        .pivot_table(
            values="count",
            index=["iso_alpha", "hs6"],
            columns="year",
            aggfunc="sum",
            fill_value=0,
        )
        .stack()
        .reset_index()
        .rename(columns={0: "count_ntm_china"})
        .assign(
            stock_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["count_ntm_china"].transform(
                "cumsum"
            ),
            new_ntm_china=lambda x: np.where(x["count_ntm_china"] > 0, "TRUE", "FALSE"),
            active_ntm_china=lambda x: np.where(x["stock_ntm_china"] > 0, "TRUE", "FALSE"),
        )
        .assign(
            lag_count_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["count_ntm_china"]
            .transform("shift")
            .fillna(0),
            lag_stock_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["stock_ntm_china"]
            .transform("shift")
            .fillna(0),
            lag_new_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["new_ntm_china"]
            .transform("shift")
            .fillna("FALSE"),
            lag_active_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["active_ntm_china"]
            .transform("shift")
            .fillna("FALSE"),
        )
        .reset_index()
        .rename(
            columns={
                "count_ntm_china": "c_count_ntm_{}_china".format("_".join(i)),
                "lag_count_ntm_china": "c_lag_count_ntm_{}_china".format("_".join(i)),
                "stock_ntm_china": "c_stock_ntm_{}_china".format("_".join(i)),
                "lag_stock_ntm_china": "c_lag_stock_ntm_{}_china".format("_".join(i)),
                "new_ntm_china": "new_ntm_{}_china".format("_".join(i)),
                "lag_new_ntm_china": "lag_new_ntm_{}_china".format("_".join(i)),
                "active_ntm_china": "active_ntm_{}_china".format("_".join(i)),
                "lag_active_ntm_china": "lag_active_ntm_{}_china".format("_".join(i)),
            }
        )
        .drop(columns = ['index'])
    )
    list_ntm.append(df_temp)
len(list_ntm)
```

```sos kernel="SoS"
list_ntm[1]
```

```sos kernel="SoS"
dvl_econ = [
    'United States',
    'European Union',
    'Japan',
    'South Korea',
    'Canada',
    'Australia',
    'Switzerland',
    'Germany',
    'New Zealand',
    'Netherlands',
    'United Kingdom',
    'Norway',
    'Finland',
    'France',
    'Italy',
    'Belgium',
    'Israel',
    'Sweden',
    'Denmark',
    'Austria'
]

df_country = (
    df_ntm
    .loc[lambda x: x["iso_alpha"].isin(["CHN"])]
    .loc[lambda x: x['aff'].isin(dvl_econ)]
    .reindex(columns=["iso_alpha", "id", "year", "hs_combined"])
        .dropna(subset=["year"])
        .assign(hs6=lambda x: x["hs_combined"].str.split("|"))
        .explode("hs6")
        .groupby(["iso_alpha", "year", "hs6"])
        .agg({"id": "nunique"})
        .rename(columns={"id": "count"})
        .sort_values(by=["iso_alpha", "year", "hs6"])
        .reset_index()
        .pivot_table(
            values="count",
            index=["iso_alpha", "hs6"],
            columns="year",
            aggfunc="sum",
            fill_value=0,
        )
        .stack()
        .reset_index()
        .rename(columns={0: "count_ntm_china"})
        .assign(
            stock_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["count_ntm_china"].transform(
                "cumsum"
            ),
            new_ntm_china=lambda x: np.where(x["count_ntm_china"] > 0, "TRUE", "FALSE"),
            active_ntm_china=lambda x: np.where(x["stock_ntm_china"] > 0, "TRUE", "FALSE"),
        )
        .assign(
            lag_count_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["count_ntm_china"]
            .transform("shift")
            .fillna(0),
            lag_stock_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["stock_ntm_china"]
            .transform("shift")
            .fillna(0),
            lag_new_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["new_ntm_china"]
            .transform("shift")
            .fillna("FALSE"),
            lag_active_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["active_ntm_china"]
            .transform("shift")
            .fillna("FALSE"),
        )
        .reset_index()
        .rename(
            columns={
                "count_ntm_china": "c_count_ntm_dvp_china",
                "lag_count_ntm_china": "c_lag_count_ntm_dvp_china",
                "stock_ntm_china": "c_stock_ntm_dvp_china",
                "lag_stock_ntm_china": "c_lag_stock_ntm_dvp_china",
                "new_ntm_china": "new_ntm_dvp_china",
                "lag_new_ntm_china": "lag_new_ntm_dvp_china",
                "active_ntm_china": "active_ntm_dvp_china",
                "lag_active_ntm_china": "lag_active_ntm_dvp_china"
            }
        )
    .drop(columns = ['index'])
    )
df_country.head(1)
```

```sos kernel="SoS"
#list_ntm_c[1]
```

```sos kernel="SoS"
list_env = [
    "Protection of the environment",
#"Protection of the environment Quality requirements",
#"Protection of the environment Safety",
#"Protection of the environment Trade facilitation"
]

df_ntm_env = (
    df_ntm
    .loc[lambda x: x["iso_alpha"].isin(["CHN"])]
    .loc[lambda x: x['keywords'].isin(list_env)]
    .reindex(columns=["iso_alpha", "id", "year", "hs_combined"])
        .dropna(subset=["year"])
        .assign(hs6=lambda x: x["hs_combined"].str.split("|"))
        .explode("hs6")
        .groupby(["iso_alpha", "year", "hs6"])
        .agg({"id": "nunique"})
        .rename(columns={"id": "count"})
        .sort_values(by=["iso_alpha", "year", "hs6"])
        .reset_index()
        .pivot_table(
            values="count",
            index=["iso_alpha", "hs6"],
            columns="year",
            aggfunc="sum",
            fill_value=0,
        )
        .stack()
        .reset_index()
        .rename(columns={0: "count_ntm_china"})
        .assign(
            stock_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["count_ntm_china"].transform(
                "cumsum"
            ),
            new_ntm_china=lambda x: np.where(x["count_ntm_china"] > 0, "TRUE", "FALSE"),
            active_ntm_china=lambda x: np.where(x["stock_ntm_china"] > 0, "TRUE", "FALSE"),
        )
        .assign(
            lag_count_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["count_ntm_china"]
            .transform("shift")
            .fillna(0),
            lag_stock_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["stock_ntm_china"]
            .transform("shift")
            .fillna(0),
            lag_new_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["new_ntm_china"]
            .transform("shift")
            .fillna("FALSE"),
            lag_active_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])["active_ntm_china"]
            .transform("shift")
            .fillna("FALSE"),
        )
        .reset_index()
        .rename(
            columns={
                "count_ntm_china": "c_count_ntm_env_china",
                "lag_count_ntm_china": "c_lag_count_ntm_env_china",
                "stock_ntm_china": "c_stock_ntm_env_china",
                "lag_stock_ntm_china": "c_lag_stock_ntm_env_china",
                "new_ntm_china": "new_ntm_env_china",
                "lag_new_ntm_china": "lag_new_ntm_env_china",
                "active_ntm_china": "active_ntm_env_china",
                "lag_active_ntm_china": "lag_active_ntm_env_china"
            }
        )
    .drop(columns = ['index'])
    )
df_ntm_env.head(1)
```

<!-- #region kernel="SoS" -->
Compute frequency, coverage and prevalence ratio:

- D is a dummy variable reflecting the presence of one or more NTMs
- M indicates whether there are imports of good i (also a dummy variable)
- V is the value of imports in product i.
- N is the number of NTMs on product i

To make things simpler, we create different queries
<!-- #endregion -->

```sos kernel="SoS"
query = """
SELECT DISTINCT(hs6), t as year
FROM baci_2002_2020
WHERE 
    t IN (
      '2002', '2003', '2004', '2005', '2006', 
      '2007', '2008', '2009', '2010', '2011'
    ) 
GROUP BY t, hs6
"""
df_hs_baci = (s3.run_query(
            query=query,
            database="trade",
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
)
              .assign(
              year = lambda x: x['year'].astype('Int64'),
              hs6 = lambda x: x['hs6'].astype('Int64').astype(str).str.zfill(6),
          )
       )

```

```sos kernel="SoS"
query = """
SELECT 
    DISTINCT(hs6) as hs6,
    t as year, 
    iso_3digit_alpha_d
    
  FROM 
    "trade"."baci_2002_2020" 
  WHERE 
    t IN (
      '2002', '2003', '2004', '2005', '2006', 
      '2007', '2008', '2009', '2010', '2011'
    ) 
  GROUP BY 
    t, 
    iso_3digit_alpha_d,hs6
"""
df_M = (s3.run_query(
            query=query,
            database="trade",
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
        .dropna(subset = ['year', 'iso_3digit_alpha_d'])
        .rename(columns={"iso_3digit_alpha_d": "iso_alpha"})
        .assign(
              year = lambda x: x['year'].astype('Int64'),
              hs6 = lambda x: x['hs6'].astype('Int64').astype(str).str.zfill(6),
          )
       )
df_M.head()
```

```sos kernel="SoS"
query = """
SELECT 
    t as year, 
    iso_3digit_alpha_d, 
    hs6,
    SUM(v) as v
  FROM 
    "trade"."baci_2002_2020" 
  WHERE 
    t IN (
      '2002', '2003', '2004', '2005', '2006', 
      '2007', '2008', '2009', '2010', '2011'
    ) 
  GROUP BY 
    t, 
    iso_3digit_alpha_d,
    hs6
"""
df_V = (s3.run_query(
            query=query,
            database="trade",
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
)
              .assign(
              year = lambda x: x['year'].astype('Int64'),
              hs6 = lambda x: x['hs6'].astype('Int64').astype(str).str.zfill(6),
          )
        .rename(columns={"iso_3digit_alpha_d": "iso_alpha"})
       )
df_V.head()
```

<!-- #region kernel="SoS" -->
Frequency
<!-- #endregion -->

```sos kernel="SoS"
df_freq =(
    df_ntm_world
    .loc[lambda x: ~x["iso_alpha"].isin(["CHN"])]
    .loc[lambda x: x["active_ntm"].isin(["TRUE"])]
    .merge(df_M)
    .groupby(['year', 'iso_alpha'])
    .agg({'hs6':'nunique'})
    .rename(columns = {'hs6':'DM'})
    .merge((df_M.groupby(['year', 'iso_alpha'])).agg({'hs6':'count'}).rename(columns = {'hs6':'M'}),
           left_index = True, right_index = True,how = 'right')
    .assign(DM=lambda x: x["DM"].fillna(0), F=lambda x: x["DM"] / x["M"])
    .reset_index()
    .sort_values(by=["iso_alpha", "year"])
    .assign(
        lag_DM=lambda x: x.groupby(["iso_alpha",])["DM"].transform("shift"),
        lag_F=lambda x: x.groupby(["iso_alpha",])["F"].transform("shift")
    )
)
df_freq[['DM','M','F','lag_DM','lag_F']].describe()
```

<!-- #region kernel="SoS" -->
Coverage
<!-- #endregion -->

```sos kernel="SoS"
df_cov = (
    df_ntm_world
    .loc[lambda x: ~x["iso_alpha"].isin(["CHN"])]
    .loc[lambda x: x["active_ntm"].isin(["TRUE"])]
    .merge(df_V)
    .groupby(['year', 'iso_alpha'])
    .agg({'v':'sum'})
    .rename(columns = {'v':'DV'})
    .merge((df_V.groupby(['year', 'iso_alpha'])).agg({'v':'sum'}).rename(columns = {'v':'V'}),
           left_index = True, right_index = True,how = 'right')
    .assign(DV=lambda x: x["DV"].fillna(0), C=lambda x: x["DV"] / x["V"])
    .reset_index()
    .sort_values(by=["iso_alpha", "year"])
    .assign(
        lag_DV=lambda x: x.groupby(["iso_alpha",])["DV"].transform("shift"),
        lag_C=lambda x: x.groupby(["iso_alpha",])["C"].transform("shift")
    )
)
df_cov[['DV','V','C','lag_DV','lag_C']].describe()
```

<!-- #region kernel="SoS" -->
Prevalence
<!-- #endregion -->

```sos kernel="SoS"
df_prev = (
    df_ntm_world
    .loc[lambda x: ~x["iso_alpha"].isin(["CHN"])]
    .loc[lambda x: x["active_ntm"].isin(["TRUE"])]
    .merge(df_M)
    .groupby(['year', 'iso_alpha'])
    .agg({'stock_ntm':'sum'})
    .rename(columns = {'stock_ntm':'NM'})
    .merge((df_M.groupby(['year', 'iso_alpha'])).agg({'hs6':'count'}).rename(columns = {'hs6':'M'}),
           left_index = True, right_index = True,how = 'right')
    .assign(NM=lambda x: x["NM"].fillna(0), P=lambda x: x["NM"] / x["M"])
    .reset_index()
    .sort_values(by=["iso_alpha", "year"])
    .assign(
        lag_NM=lambda x: x.groupby(["iso_alpha",])["NM"].transform("shift"),
        lag_P=lambda x: x.groupby(["iso_alpha",])["P"].transform("shift")
    )
    .drop(columns = ['M'])
)
df_prev[['NM','P','lag_NM','lag_P']].describe()
```

```sos kernel="SoS"
df_prev
```

<!-- #region kernel="SoS" -->
### Merge all NTM
<!-- #endregion -->

```sos kernel="SoS"
df_final = (
    df.drop(columns=["year.1", "hs6.1", "geocode4_corr.1", "country_en.1"])
    .assign(hs6=lambda x: x["hs6"].astype(str).str.zfill(6))
    .merge(df_ntm_world, on=["iso_alpha", "year", "hs6"], how="left")
    .merge(
        (
            df_ntm_china.assign(
                c_lag_count_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])[
                    "count_ntm_china"
                ]
                .transform("shift")
                .fillna(0),
                c_lag_stock_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])[
                    "stock_ntm_china"
                ]
                .transform("shift")
                .fillna(0),
                lag_new_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])[
                    "new_ntm_china"
                ]
                .transform("shift")
                .fillna("FALSE"),
                lag_active_ntm_china=lambda x: x.groupby(["iso_alpha", "hs6"])[
                    "active_ntm_china"
                ]
                .transform("shift")
                .fillna("FALSE"),
            )
            .rename(
                columns={
                    "count_ntm_china": "c_count_ntm_china",
                    "stock_ntm_china": "c_stock_ntm_china",
                }
            )
            .drop(columns=["iso_alpha"])
        ),
        on=["year", "hs6"],
        how="left",
    )
    .merge(df_ntm_env.drop(columns=["iso_alpha"]), on=["year", "hs6"], how="left")
    .merge(list_ntm[0].drop(columns=["iso_alpha"]), on=["year", "hs6"], how="left")
    .merge(list_ntm[1].drop(columns=["iso_alpha"]), on=["year", "hs6"], how="left")
    .merge(list_ntm[2].drop(columns=["iso_alpha"]), on=["year", "hs6"], how="left")
    .merge(list_ntm[3].drop(columns=["iso_alpha"]), on=["year", "hs6"], how="left")
    .merge(df_country.drop(columns=["iso_alpha"]), on=["year", "hs6"], how="left")
    .merge(df_freq, on=["year", "iso_alpha"], how="left",)
    .merge(df_prev, on=["year", "iso_alpha"], how="left",)
    .merge(df_cov, on=["year", "iso_alpha"], how="left",)
    .merge(
    (
            df_ntm_world.assign(
                c_lag_count_ntm_w=lambda x: x.groupby(["iso_alpha", "hs6"])[
                    "count_ntm"
                ]
                .transform("shift")
                .fillna(0),
                c_lag_stock_ntm_w=lambda x: x.groupby(["iso_alpha", "hs6"])[
                    "stock_ntm"
                ]
                .transform("shift")
                .fillna(0),
                lag_new_ntm_w=lambda x: x.groupby(["iso_alpha", "hs6"])[
                    "new_ntm"
                ]
                .transform("shift")
                .fillna("FALSE"),
                lag_active_ntm_w=lambda x: x.groupby(["iso_alpha", "hs6"])[
                    "active_ntm"
                ]
                .transform("shift")
                .fillna("FALSE"),
            )
            .rename(
                columns={
                    "count_ntm": "c_count_ntm_w",
                    "stock_ntm": "c_stock_ntm_w",
                    "new_ntm": "new_ntm_w",
                    "active_ntm": "active_ntm_w",
                }
            )
        ), on=["year", "iso_alpha", "hs6"], how="left"
    )
)
df_final = df_final.assign(
    **{
        "{}".format(i): df_final[i].fillna(0)
        for i in list(df_final.filter(regex="^c_").columns)
        + [
            "DM",
            "M",
            "F",
            "lag_DM",
            "lag_F",
            "NM",
            "P",
            "lag_NM",
            "lag_P",
            "DV",
            "V",
            "C",
            "lag_DV",
            "lag_C",
        ]
    },
    **{
        "{}".format(i): df_final[i].fillna("FALSE")
        for i in list(df_final.filter(regex="new|active").columns)
    },
)
df_final.head(1)
```

```sos kernel="SoS"
#df_ntm.loc[lambda x: x["iso_alpha"].isin(["CHN"])]['aff'].value_counts()
```

<!-- #region kernel="SoS" -->
## Quality variable: different sigma

- Use different values of sigma:
    -  Set sigma to the elasticity as 5 and 10
        - [Credit Constraints, Quality, and Export Prices: Theory and Evidence from China](https://drive.google.com/file/d/1FnIIq2kwWdcIgg6jm-ydIvzj9f-UApYR/view?usp=sharing)
        - [Credit constraints, quality, and export prices: Theory and evidence from China](https://drive.google.com/file/d/1FnIIq2kwWdcIgg6jm-ydIvzj9f-UApYR/view?usp=sharing)
        
-> code from [01_preparation_quality](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/01_preparation_quality.md#steps)
<!-- #endregion -->
```sos kernel="SoS"
query = """
SELECT geocode4_corr,country_en,year,regime,hs6,iso_alpha,unit_price,quantity
FROM chinese_trade.china_export_tariff_tax
""".format(db, table)
df_vat = (
    s3.run_query(
    query=query,
    database="chinese_trade",
    s3_output="SQL_OUTPUT_ATHENA",
    filename="trade_vat",  # Add filename to print dataframe
    destination_key=None,  # Add destination key if need to copy output
)
    .assign(
              year = lambda x: x['year'].astype('Int64'),
              hs6 = lambda x: x['hs6'].astype('Int64').astype(str).str.zfill(6),
          )
)
df_quality = (
    df_vat.assign(
    hs2 = lambda x: x['hs6'].str[:2],
    hs3 = lambda x: x['hs6'].str[:3],
    hs4 = lambda x: x['hs6'].str[:4],
    sigma_3 = 3,
    sigma_5 = 5,
    sigma_10 = 10

)
    .assign(
        sigma_price3 = lambda x: x['sigma_3'].astype('float') * np.log(x['unit_price']),
        sigma_price5 = lambda x: x['sigma_5'].astype('float') * np.log(x['unit_price']),
        sigma_price10 = lambda x: x['sigma_10'].astype('float') * np.log(x['unit_price']),
        y3 = lambda x : np.log(x['quantity']) + x['sigma_price3'],
        y5 = lambda x : np.log(x['quantity']) + x['sigma_price5'],
        y10 = lambda x : np.log(x['quantity']) + x['sigma_price10']
    )
)
df_quality["FE_ct"] = pd.factorize(df_quality["year"].astype('string') + 
                                       df_quality["country_en"])[0]
compute_quality = False
if compute_quality:
    from sklearn.pipeline import make_pipeline
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import OneHotEncoder
    from sklearn.compose import make_column_transformer
    
    cat_proc = make_pipeline(
        OneHotEncoder()
    )
    preprocessor = make_column_transformer(
        (cat_proc, tuple(['hs6', 'FE_ct']))
    )
    clf = make_pipeline(preprocessor,
                        LinearRegression(fit_intercept=True, normalize=False))
    #%%time
    print('begin model 3')
    MODEL3 = clf.fit(df_quality[['hs6', 'FE_ct']], df_quality['y3']) 
    print('begin model 5')
    MODEL5 = clf.fit(df_quality[['hs6', 'FE_ct']], df_quality['y5']) 
    print('begin model 10')
    MODEL10 = clf.fit(df_quality[['hs6', 'FE_ct']], df_quality['y10']) 
    from joblib import dump, load
    dump(MODEL3, 'filename3.joblib') 
    dump(MODEL5, 'filename5.joblib') 
    dump(MODEL10, 'filename10.joblib') 
```

```sos kernel="SoS"
from joblib import dump, load
df_quality = df_quality.assign(
    prediction3 = lambda x: load('filename3.joblib') .predict(x[['hs6', 'FE_ct']]),
    residual3 = lambda x: x['y3'] - x['prediction3'],
    kandhelwal_quality3 = lambda x: x['residual3'] / (x['sigma_3'].astype('float') -1),
    
    prediction5 = lambda x: load('filename5.joblib') .predict(x[['hs6', 'FE_ct']]),
    residual5 = lambda x: x['y5'] - x['prediction5'],
    kandhelwal_quality5 = lambda x: x['residual5'] / (x['sigma_5'].astype('float') -1),
    
    prediction10 = lambda x: load('filename10.joblib') .predict(x[['hs6', 'FE_ct']]),
    residual10 = lambda x: x['y10'] - x['prediction10'],
    kandhelwal_quality10 = lambda x: x['residual10'] / (x['sigma_10'].astype('float') -1),
)  
```

```sos kernel="SoS"
df_final = (
    df_final
    .merge(
    df_quality.reindex(
    columns = ['geocode4_corr','year','regime','hs6','iso_alpha',
              'kandhelwal_quality3','kandhelwal_quality5','kandhelwal_quality10']
    )
    )
    #.to_csv(os.path.join(path_local, 'df_final_robusntess' + '.csv'), index = False)
)
df_final.head()
```

```sos kernel="SoS"
df_final.shape
```

```sos kernel="SoS"
manova = "credit_constraint_cic - tabula-Manova et al. 2015 - Firm Exports and Multinational Activity Under Credit Constraints.csv"
jarreau = "credit_constraint_cic - Poncet_ISIC_industry.csv"
df_final = df_final.merge(
    (
        pd.read_csv(
            "concordance_sic_hs.csv", dtype={"hs6": "str", "concordance_hs": "str"}
        )
        .merge(
            pd.read_csv(manova, dtype={"ISIC": "str"},).rename(
                columns={"ISIC": "concordance_hs"}
            ),
            how="inner",
        )
        .rename(
            columns={
                "ext_finance": "ext_finance_m",
                "inventory_ratio": "inventory_ratio_m",
                "asset_tangibility": "asset_tangibility_m",
                "trade_credit_intensity": "trade_credit_intensity_m",
                "liquidity_needs": "liquidity_needs_m",
            }
        )
        .drop(columns=["Unnamed: 0"])
    ),
    how="inner",
).merge(
    (
        pd.read_csv(
            "concordance_sic_hs.csv", dtype={"hs6": "str", "concordance_hs": "str"}
        )
        .merge(
            pd.read_csv(jarreau, dtype={"ISIC": "str"},).rename(
                columns={"ISIC": "concordance_hs"}
            ),
            how="inner",
        )
        .rename(
            columns={
                "External dependence": "ext_finance_j",
                "R&D intensity": "rd_intensity_j",
                "Liquidity needs": "liquidity_needs_j",
            }
        )
        .drop(
            columns=[
                "Unnamed: 0",
                "Name of sector",
                "CIC",
                "Hum. cap. intensity",
                "Phys. cap. intensity",
                "Foreign share",
                "rank",
                "concordance_hs",
            ]
        )
    ),
    how="inner",
)

df_final = df_final.loc[lambda x: 
                        (x['kandhelwal_quality'] > -15)
                        &
                        (x['kandhelwal_quality'] < 15)
                       ]
```

<!-- #region kernel="SoS" -->
Make the dataset squared
<!-- #endregion -->

```sos kernel="SoS"
df_final = (
    df_final
    .assign(
        count = lambda x: x.groupby(['year', 'geocode4_corr', 'hs6', 'iso_alpha'])['hs6'].transform('count')
    )
    .loc[lambda x: x['count'] ==2]
    
)
df_final = (
    df_final
    .loc[lambda x: x['regime'].isin(['NOT_ELIGIBLE'])]
    .reindex(columns = ['year', 'geocode4_corr', 'hs6', 'iso_alpha'])
    .drop_duplicates()
    .merge(df_final)
    
)
```

```sos kernel="SoS"
df_final.shape
```

<!-- #region kernel="SoS" -->
### Summary statistics Quality

<!-- #endregion -->

```sos kernel="SoS"
query = """
SELECT *, CASE WHEN income_group = 'Low income' 
      OR income_group = 'Low Lower middle income' 
      OR (
        income_group IS NULL 
        AND ISO_alpha != 'HKG'
      ) then 'LDC' ELSE 'DC' END AS income_group_ldc_dc
FROM (
SELECT * FROM "chinese_trade"."china_product_quality"
LEFT JOIN (
SELECT iso_alpha03 , year, income_group
FROM world_bank.world_bank_gdp_per_capita ) as world_bank_gdp_per_capita
ON china_product_quality.iso_alpha = world_bank_gdp_per_capita.iso_alpha03
    AND china_product_quality.year = world_bank_gdp_per_capita.year 
    LEFT JOIN industry.hs6_homogeneous
    ON china_product_quality.hs6 = hs6_homogeneous.hs6
    )
"""
df_quality_stat = (
    s3.run_query(
    query=query,
    database="chinese_trade",
    s3_output="SQL_OUTPUT_ATHENA",
    filename="trade_vat",  # Add filename to print dataframe
    destination_key=None,  # Add destination key if need to copy output
)
    .assign(
              year = lambda x: x['year'].astype('Int64'),
              hs6 = lambda x: x['hs6'].astype('Int64').astype(str).str.zfill(6),
          )
)
```

```sos kernel="SoS"
(
    df_final
    ['kandhelwal_quality'].describe()
)
```

<!-- #region kernel="SoS" -->
- Average quality
- Average quality per dvp/dvpg
- Average quality per product type
<!-- #endregion -->

<!-- #region kernel="SoS" -->
Construct table for the product: Homogenous vs heterogeneous
<!-- #endregion -->

```sos kernel="SoS"
t_all_product = pd.concat(
    [
        #### ALL QUALITY
        (
            pd.concat(
                [
                    (
                        df_final
                        .replace({'homogeneous':{'HETEREGENEOUS':'Heterogenous','HOMOGENEOUS':'Homogeneous'}})
                        .assign(homogeneous = lambda x: x['homogeneous'].fillna('Homogeneous'))
                        .groupby(["homogeneous"])
                        .agg({"kandhelwal_quality": "mean"})
                        .rename_axis(index={"homogeneous": ""})
                        .rename(columns = {'kandhelwal_quality':'Estimated quality'})
                        .T
                    )
                ],
                axis=1,
                keys=["all"],
            ).swaplevel(axis=1)
        ),
        #### ALL REBATE
        (
            pd.concat(
                [
                    (
                        df_final
                        .replace({'homogeneous':{'HETEREGENEOUS':'Heterogenous','HOMOGENEOUS':'Homogeneous'}})
                        .assign(
                            rebate=lambda x: x["lag_vat_reb_m"] / x["lag_vat_m"]
                        )
                        .assign(homogeneous = lambda x: x['homogeneous'].fillna('Homogeneous'))
                        .groupby(["homogeneous"])
                        .agg({"rebate": "mean"})
                        .rename_axis(index={"homogeneous": ""})
                        .rename(columns = {'kandhelwal_quality':'Estimated quality'})
                        .T
                    )
                ],
                axis=1,
                keys=["all"],
            ).swaplevel(axis=1)
        ),
        #### NB OF OBS
        (
            pd.concat(
                [
                    (
                        df_final
                        .replace({'homogeneous':{'HETEREGENEOUS':'Heterogenous','HOMOGENEOUS':'Homogeneous'}})
                        .assign(
                            rebate=lambda x: x["lag_vat_reb_m"] / x["lag_vat_m"]
                        )
                        .assign(homogeneous = lambda x: x['homogeneous'].fillna('Homogeneous'))
                        .groupby(["homogeneous"])
                        .agg({"rebate": "count"})
                        .rename(columns={"rebate": "nb of obs"})
                        .rename_axis(index={"homogeneous": ""})
                        .rename(columns = {'kandhelwal_quality':'Estimated quality'})
                        .T
                    )
                ],
                axis=1,
                keys=["all"],
            ).swaplevel(axis=1)
        ),
         (
            pd.concat(
                [
                    (
                        df_final
                        .replace({'homogeneous':{'HETEREGENEOUS':'Heterogenous','HOMOGENEOUS':'Homogeneous'}})
                        .assign(
                            rebate=lambda x: x["lag_vat_reb_m"] / x["lag_vat_m"]
                        )
                        .assign(homogeneous = lambda x: x['homogeneous'].fillna('Homogeneous'))
                        .groupby(["homogeneous"])
                        .agg({"hs6": "nunique"})
                        .rename(columns={"rebate": "nb of obs"})
                        .rename_axis(index={"homogeneous": ""})
                        .rename(columns = {'hs6':'nb_product_country'})
                        .T
                    )
                ],
                axis=1,
                keys=["all"],
            ).swaplevel(axis=1)
        )
    ]
)

t_regime_product = pd.concat(
    [
        #### QUALITY REGIME
        (
            df_final
            .replace({'homogeneous':{'HETEREGENEOUS':'Heterogenous','HOMOGENEOUS':'Homogeneous'}})
            .replace({'regime':{'ELIGIBLE':'Eligible','NOT_ELIGIBLE':'Not eligible'}})
            .assign(homogeneous = lambda x: x['homogeneous'].fillna('Homogeneous'))
            .groupby(["homogeneous", "regime"])
            .agg({"kandhelwal_quality": "mean"})
            .rename_axis(index={"homogeneous": ""})
            .rename(columns = {'kandhelwal_quality':'Estimated quality'})
            .T
        ),
        #### REBATE REGIME
        (
            df_final
            .replace({'homogeneous':{'HETEREGENEOUS':'Heterogenous','HOMOGENEOUS':'Homogeneous'}})
            .replace({'regime':{'ELIGIBLE':'Eligible','NOT_ELIGIBLE':'Not eligible'}})
            .assign(homogeneous = lambda x: x['homogeneous'].fillna('Homogeneous'))
            .assign(rebate=lambda x: x["lag_vat_reb_m"] / x["lag_vat_m"])
            .groupby(["homogeneous", "regime"])
            .agg({"rebate": "mean"})
            .rename_axis(index={"homogeneous": ""})
            .rename(columns = {'kandhelwal_quality':'Estimated quality'})
            .T
        ),
        #### NB OBS REGIME
        (
            df_final
            .replace({'homogeneous':{'HETEREGENEOUS':'Heterogenous','HOMOGENEOUS':'Homogeneous'}})
            .replace({'regime':{'ELIGIBLE':'Eligible','NOT_ELIGIBLE':'Not eligible'}})
            .assign(homogeneous = lambda x: x['homogeneous'].fillna('Homogeneous'))
            .assign(rebate=lambda x: x["lag_vat_reb_m"] / x["lag_vat_m"])
            .groupby(["homogeneous", "regime"])
            .agg({"rebate": "count"})
            .rename(columns={"rebate": "nb of obs"})
            .rename_axis(index={"homogeneous": ""})
            .rename(columns = {'kandhelwal_quality':'Estimated quality'})
            .T
        ),
    ]
)
```

<!-- #region kernel="SoS" -->
Construct table for the countryes: LDC vs DC
<!-- #endregion -->

```sos kernel="SoS"
t_all_country = pd.concat(
    [
        #### ALL QUALITY
        (
            pd.concat(
                [
                    (
                        df_final.groupby(["income_group_ldc_dc"])
                        .agg({"kandhelwal_quality": "mean"})
                        .rename_axis(index={"income_group_ldc_dc": ""})
                        .rename(columns = {'kandhelwal_quality':'Estimated quality'})
                        .T
                    )
                ],
                axis=1,
                keys=["all"],
            ).swaplevel(axis=1)
        ),
        #### ALL REBATE
        (
            pd.concat(
                [
                    (
                        df_final.assign(
                            rebate=lambda x: x["lag_vat_reb_m"] / x["lag_vat_m"]
                        )
                        .groupby(["income_group_ldc_dc"])
                        .agg({"rebate": "mean"})
                        .rename_axis(index={"income_group_ldc_dc": ""})
                        .rename(columns = {'kandhelwal_quality':'Estimated quality'})
                        .T
                    )
                ],
                axis=1,
                keys=["all"],
            ).swaplevel(axis=1)
        ),
        #### NB OF OBS
        (
            pd.concat(
                [
                    (
                        df_final.assign(
                            rebate=lambda x: x["lag_vat_reb_m"] / x["lag_vat_m"]
                        )
                        .groupby(["income_group_ldc_dc"])
                        .agg({"rebate": "count"})
                        .rename(columns={"rebate": "nb of obs"})
                        .rename_axis(index={"income_group_ldc_dc": ""})
                        .rename(columns = {'kandhelwal_quality':'Estimated quality'})
                        .T
                    )
                ],
                axis=1,
                keys=["all"],
            ).swaplevel(axis=1)
        ),
        (
            pd.concat(
                [
                    (
                        df_final.groupby(["income_group_ldc_dc"])
                        .agg({"country_en": "nunique"})
                        .rename_axis(index={"income_group_ldc_dc": ""})
                        .rename(columns = {'country_en':'nb_product_country'})
                        .T
                    )
                ],
                axis=1,
                keys=["all"],
            ).swaplevel(axis=1)
        )
    ]
)
t_regime_country = pd.concat(
    [
        #### QUALITY REGIME
        (
            df_final
            .replace({'regime':{'ELIGIBLE':'Eligible','NOT_ELIGIBLE':'Not eligible'}})
            .groupby(["income_group_ldc_dc", "regime"])
            .agg({"kandhelwal_quality": "mean"})
            .rename_axis(index={"income_group_ldc_dc": ""})
            .rename(columns = {'kandhelwal_quality':'Estimated quality'})
            .T
        ),
        #### REBATE REGIME
        (
            df_final
            .replace({'regime':{'ELIGIBLE':'Eligible','NOT_ELIGIBLE':'Not eligible'}})
            .assign(rebate=lambda x: x["lag_vat_reb_m"] / x["lag_vat_m"])
            .groupby(["income_group_ldc_dc", "regime"])
            .agg({"rebate": "mean"})
            .rename_axis(index={"income_group_ldc_dc": ""})
            .rename(columns = {'kandhelwal_quality':'Estimated quality'})
            .T
        ),
        #### NB OBS REGIME
        (
            df_final
            .replace({'regime':{'ELIGIBLE':'Eligible','NOT_ELIGIBLE':'Not eligible'}})
            .assign(rebate=lambda x: x["lag_vat_reb_m"] / x["lag_vat_m"])
            .groupby(["income_group_ldc_dc", "regime"])
            .agg({"rebate": "count"})
            .rename(columns={"rebate": "nb of obs"})
            .rename_axis(index={"income_group_ldc_dc": ""})
            .rename(columns = {'kandhelwal_quality':'Estimated quality'})
            .T
        )
    ]
)
```

```sos kernel="SoS"
l = np.repeat("-", 15)
l[0::3] = "1"
l
```

```sos kernel="SoS"
t_final = (
    pd.concat(
        [
            pd.concat(
                [
                    pd.concat(
                        [
                            pd.concat(
                                [
                                    pd.concat(
                                        [
                                            df_final[["kandhelwal_quality"]]
                                            .rename(
                                                columns={
                                                    "kandhelwal_quality": "Estimated quality"
                                                }
                                            )
                                            .mean()
                                        ],
                                        keys=["all"],
                                        axis=1,
                                    ),
                                    pd.concat(
                                        [
                                            df_final.assign(
                                                rebate=lambda x: x["lag_vat_reb_m"]
                                                / x["lag_vat_m"]
                                            )[["rebate"]].mean()
                                        ],
                                        keys=["all"],
                                        axis=1,
                                    ),
                                    pd.concat(
                                        [
                                            df_final[["kandhelwal_quality"]]
                                            .count()
                                            .rename({"kandhelwal_quality": "nb of obs"})
                                        ],
                                        keys=["all"],
                                        axis=1,
                                    ),
                                    pd.concat(
                                        [
                                            df_final[["hs6"]]
                                            .nunique()
                                            .rename({"hs6": "nb_product_country"})
                                        ],
                                        keys=["all"],
                                        axis=1,
                                    )
                                ],
                            )
                        ],
                        keys=["Full"],
                        axis=1,
                    ),
                    (
                        pd.concat([t_all_country, t_regime_country], axis=1).sort_index(
                            axis=1, level=[0, 1], ascending=[True, False]
                        )
                    ),
                    (
                        pd.concat([t_all_product, t_regime_product], axis=1).sort_index(
                            axis=1, level=[0, 1], ascending=[True, False]
                        )
                    ),
                ],
                axis=1,
            ).T,
            (
                pd.DataFrame(
                    l,
                    pd.MultiIndex.from_product(
                        [
                            ["Full", "DC", "LDC", "Heterogenous", "Homogeneous"],
                            ["all", "Not eligible", "Eligible"],
                        ],
                        names=["", ""],
                    ),
                    ["condition"],
                ).loc[
                    lambda x: ~x.index.isin(
                        [("Full", "Not eligible"), ("Full", "Eligible")]
                    )
                ]
            ),
        ],
        axis=1,
    )
    #.fillna('-')
    .assign(
        nb_of_obs=lambda x: np.where(x["condition"].isin(["-"]), "-", x["nb of obs"].astype(int)),
        rebate=lambda x: np.where(x["condition"].isin(["-"]), "-", np.round(x["rebate"],3)),
        nb_product_country = lambda x: np.where(
            x['nb_product_country'].isin([np.nan]), 
             '-',
            x['nb_product_country'].astype("Int64").astype(str)
           
        )
    )
    .drop(columns=["nb of obs", "condition"])
    .rename(columns={"nb_of_obs": "nb of obs", 'nb_product_country':'nb product/country'})
    
)
t_final
```

```sos kernel="SoS"
import tex2pix
from PyPDF2 import PdfFileMerger
from wand.image import Image as WImage
```

```sos kernel="SoS"
folder = 'Figures'
table_number = 1
title = 'Distribution of unique rebate share at the HS6 product level'
tb_note = """

"""
with open('{}/table_{}.tex'.format(folder,table_number), 'w') as fout:
    for i in range(len( t_final.to_latex(index=True,float_format="{:0.3f}".format))):
        if i ==0:
            header= "\documentclass[preview]{standalone} \n\\usepackage[utf8]{inputenc}\n" \
            "\\usepackage{booktabs,caption,threeparttable, siunitx, adjustbox}\n\n" \
            "\\begin{document}"
            top =  '\n\\begin{adjustbox}{width=\\textwidth, totalheight=\\textheight-2\\baselineskip,keepaspectratio}\n'
            table_top = "\n\\begin{table}[!htbp] \centering"
            table_title = "\n\caption{%s}\n" % title
            
            fout.write(header)
            fout.write(table_top)
            fout.write(table_title)
            fout.write(top)
           
        fout.write( t_final.to_latex(index=True,float_format="{:0.3f}".format)[i])
    
    bottom =  '\n\\end{adjustbox}\n'
    tb_note_top = "\n\\begin{tablenotes}\n\small\n\item"
    table_bottom = "\n\end{table}"
    footer = "\n\n\\end{document}"
    tb_note_bottom = "\n\end{tablenotes}"
    fout.write(bottom)
    fout.write(tb_note_top)
    fout.write(tb_note)
    fout.write(tb_note_bottom)
    fout.write(table_bottom)
    fout.write(footer)
 
f = open('{}/table_{}.tex'.format(folder,table_number))
r = tex2pix.Renderer(f, runbibtex=False)
r.mkpdf('{}/table_{}.pdf'.format(folder,table_number))
img = WImage(filename='{}/table_{}.pdf'.format(folder,table_number),
resolution = 200)
display(img)
```

```sos kernel="SoS"
import seaborn as sns
import matplotlib.pyplot as plt
```

```sos kernel="SoS"
sns.set(style="whitegrid")
```

```sos kernel="SoS"
plt.figure(figsize = (15,10))
var = 'kandhelwal_quality'
temp = (
    df_quality_stat
    .replace({'regime':{'ELIGIBLE':'Eligible','NOT_ELIGIBLE':'Not eligible'}})
    .groupby(['regime'])
    .sample(frac = .5)
    .reindex(columns = ['regime',var])
    .loc[lambda x: x[var] < 15]#df_quality_stat[var].describe(percentiles = [.95])['95%']]
    .loc[lambda x: x[var] > -15]#df_quality_stat[var].describe(percentiles = [.05])['5%']]
    .rename(columns = {'kandhelwal_quality':'Estimated quality'})
    
)
sns.set(style="white")

# Without transparency
sns.kdeplot(data=temp, x='Estimated quality', hue="regime", cut=0, fill=False, common_norm=False, alpha=.5)
plt.savefig("Figures/fig_2.png",
            bbox_inches='tight',
            dpi=600)
#plt.show()
```

<!-- #region kernel="SoS" -->
Replicate previous table
<!-- #endregion -->

```sos kernel="SoS"
query = """
WITH temp AS (
SELECT distinct(hs6), year, lag_vat_reb_m/lag_vat_m as rebate
FROM {}.{}  
)
SELECT year, hs6, rebate
FROM temp
ORDER BY hs6
""".format(db, table)

df = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename='fig1',  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = {'hs6':'string'}
)
      .loc[lambda x: x['hs6'].isin(df_final['hs6'].unique())]
     )
df.head()
```

```sos kernel="SoS"
df_latex = (
    pd.concat(
    [
        (
            df
            .groupby("year")["rebate"]
            .describe()
            .assign(
                count=lambda x: x["count"].astype("int"),
                max=lambda x: x["max"].astype("int"),
            )
            .apply(lambda x: round(x, 2))
        ),
        (df
         .groupby("year").agg({"rebate": "nunique"})),
    ],
    axis=1,
    # 
)
    .rename(columns = {'rebate':'unique rebate'})
    .reindex(columns = ['count','unique rebate', 'mean', 'std', 'min', '25%', '50%', '75%', 'max',
       ])
    .to_latex()
)
```

```sos kernel="SoS"
folder = 'Figures'
table_number = 2
title = 'Distribution of unique rebate share at the HS6 product level'
tb_note = """

"""
with open('{}/table_{}.tex'.format(folder,table_number), 'w') as fout:
    for i in range(len( df_latex)):
        if i ==0:
            header= "\documentclass[preview]{standalone} \n\\usepackage[utf8]{inputenc}\n" \
            "\\usepackage{booktabs,caption,threeparttable, siunitx, adjustbox}\n\n" \
            "\\begin{document}"
            top =  '\n\\begin{adjustbox}{width=\\textwidth, totalheight=\\textheight-2\\baselineskip,keepaspectratio}\n'
            table_top = "\n\\begin{table}[!htbp] \centering"
            table_title = "\n\caption{%s}\n" % title
            
            fout.write(header)
            fout.write(table_top)
            fout.write(table_title)
            fout.write(top)
           
        fout.write( df_latex[i])
    
    bottom =  '\n\\end{adjustbox}\n'
    tb_note_top = "\n\\begin{tablenotes}\n\small\n\item"
    table_bottom = "\n\end{table}"
    footer = "\n\n\\end{document}"
    tb_note_bottom = "\n\end{tablenotes}"
    fout.write(bottom)
    fout.write(tb_note_top)
    fout.write(tb_note)
    fout.write(tb_note_bottom)
    fout.write(table_bottom)
    fout.write(footer)
 
f = open('{}/table_{}.tex'.format(folder,table_number))
r = tex2pix.Renderer(f, runbibtex=False)
r.mkpdf('{}/table_{}.pdf'.format(folder,table_number))
img = WImage(filename='{}/table_{}.pdf'.format(folder,table_number),
resolution = 200)
display(img)
```

```sos kernel="SoS"
pd.DataFrame({
    'hs6':df_final['hs6'].unique()
}).to_csv('hs6.csv', index = False)
```

<!-- #region kernel="SoS" -->
### Statistic about NTM
<!-- #endregion -->

```sos kernel="SoS"
(
    df_ntm
    .loc[lambda x: ~x["aff"].isin(["China"])]
    .loc[lambda x: ~x["year"].isin(["2010"])]
    #.merge(df_final[['iso_alpha', "hs6"]].drop_duplicates())
    .groupby(['aff'])
    .agg({'id':'nunique'})
    .sort_values(by = ['id'], ascending = False)
    .head(10)
)
```

```sos kernel="SoS"
fig_ntm = (
    df_ntm
    .loc[lambda x: x['ntm'].isin([
        'TBT',
        'SPS'
    ])]
    .groupby(['year','ntm'])
    .agg({"id": "nunique"})
    .unstack(-1)
    .collapse_levels(sep='_')
)
fig, ax1 = plt.subplots(figsize=(15,10))
lns1 = ax1.plot(fig_ntm.index, fig_ntm['id_SPS'], 'g-', label = "SPS") 
lns1 = ax1.plot(fig_ntm.index, fig_ntm['id_TBT'], 'b-', label = "TBT") 
ax1.legend(bbox_to_anchor=(1.08, 1), loc=2, borderaxespad=0.)
plt.savefig("Figures/fig_3.png",
            bbox_inches='tight',
            dpi=600)
```

<!-- #region kernel="SoS" -->
# compute fixed effect

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


Create the following fixed effect for the baseline regression:

**index**

* city: `c`
* product: `k`
* sector: `s`
* year: `t`
* Destination: `j`
* regime: `r`

**FE**

* city-product: `FE_ck`
* City-sector-year: `FE_cst`
* City-product-regime: `FE_ckr`
* City-sector-regime-year: `FE_csrt`
* Product-year: `FE_kt`
* Product-destination: `FE_pj`
* Destination-year: `FE_jt`
<!-- #endregion -->
<!-- #region kernel="SoS" -->

<!-- #endregion -->
<!-- #region kernel="SoS" -->

<!-- #endregion -->
<!-- #region kernel="SoS" -->

<!-- #endregion -->
<!-- #region kernel="SoS" -->

<!-- #endregion -->
<!-- #region kernel="SoS" -->
<!-- #endregion -->
<!-- #endregion -->

```sos kernel="SoS"
create_fe = True
if create_fe:
    df_final = df_final#.loc[lambda x: x['cn'] == 2]
    #df = pd.read_csv(df_path, dtype = dtypes)
    ### city-product
    df_final["fe_ck"] = pd.factorize(df_final["geocode4_corr"].astype('str') + 
                                        df_final["hs6"].astype('str')
                                       )[0]
    
    ### sector-year
    df_final["fe_st"] = pd.factorize(
                                        df_final["hs4"].astype('str') +
                                        df_final["year"].astype('str')
                                       )[0]

    ### sector-year
    df_final["fe_ct"] = pd.factorize(
                                        df_final["geocode4_corr"].astype('str') +
                                        df_final["year"].astype('str')
                                       )[0]
    
    ### City-sector-year
    df_final["fe_cst"] = pd.factorize(df_final["geocode4_corr"].astype('str') + 
                                        df_final["hs4"].astype('str') +
                                        df_final["year"].astype('str')
                                       )[0]

    ### City-product-regime
    df_final["fe_ckr"] = pd.factorize(df_final["geocode4_corr"].astype('str') + 
                                        df_final["hs6"].astype('str') +
                                        df_final["regime"].astype('str')
                                       )[0]

    ### City-sector-regime-year
    df_final["fe_csrt"] = pd.factorize(df_final["geocode4_corr"].astype('str') + 
                                        df_final["hs4"].astype('str') +
                                        df_final["regime"].astype('str') +
                                        df_final["year"].astype('str')
                                       )[0]

    ## Product-year
    df_final["fe_kt"] = pd.factorize(df_final["hs6"].astype('str') + 
                                        df_final["year"].astype('str')
                                       )[0]

    ## Product-destination
    df_final["fe_kj"] = pd.factorize(df_final["hs6"].astype('str') + 
                                        df_final["country_en"].astype('str')
                                       )[0]

    ## Destination-year
    df_final["fe_jt"] = pd.factorize(df_final["country_en"].astype('str') + 
                                        df_final["year"].astype('str')
                                       )[0]
    
    ## Destination-year-regime
    df_final["fe_jtr"] = pd.factorize(df_final["country_en"].astype('str') + 
                                        df_final["year"].astype('str') +
                                df_final["regime"].astype('str')
                                
                                       )[0]

    ## city-product-destination
    df_final["fe_ckj"] = pd.factorize(df_final["geocode4_corr"].astype('str') + 
                                        df_final["hs6"].astype('str') + 
                                        df_final["country_en"].astype('str')
                                       )[0]
    
    ## product destination regime 
    df_final["fe_kjr"] = pd.factorize(df_final["hs6"].astype('str') + 
                                        df_final["country_en"].astype('str') + 
                                        df_final["regime"].astype('str')
                                       )[0]
    ## Shocks
    df_final["fe_group_shock"] = pd.factorize(
        df_final["hs6"].astype('str') +
        df_final["country_en"].astype('str') + 
        df_final["year"].astype('str'))[0]
    
    df_final.to_csv(os.path.join(path_local, filename + '.csv'), index = False)
```

<!-- #region kernel="SoS" nteract={"transient": {"deleting": false}} -->
## Schema Latex table

To rename a variable, please use the following template:

```
{
    'old':'XX',
    'new':'XX_1'
    }
```

if you need to pass a latex format with `\`, you need to duplicate it for instance, `\text` becomes `\\text:

```
{
    'old':'working\_capital\_i',
    'new':'\\text{working capital}_i'
    }
```

Then add it to the key `to_rename`
<!-- #endregion -->

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
add_to_dic = True
if add_to_dic:
    if os.path.exists("schema_table.json"):
        os.remove("schema_table.json")
    data = {'to_rename':[], 'to_remove':[]}
    dic_rename = [
        {
        'old':'ln\_lag\_tax\_rebate',
        'new':'\\text{Ln VAT export tax}_{k, t-1}'
        },
        {
        'old':'ln\_rebate',
        'new':'\\text{Ln VAT rebate}_{k, t-1}'
        },
        {
        'old':'ln\_rebate\_1',
        'new':'\\text{Ln VAT rebate}_{k, t-1}'
        },
        {
        'old':'ln\_rebate\_2',
        'new':'\\text{Ln VAT rebate}_{k, t-1}'
        },
        {
        'old':'rebate',
        'new':'\\text{VAT refund}_{k, t-1}'
        },
        {
        'old':'regimeELIGIBLE',
        'new':'\\text{Regime}^R'
        },
        {
        'old':'ln\_lag\_import\_tax',
        'new':'\\text{Import tax,}_{k, t-1}'
        },
        {
        'old':'lag\_foreign\_export\_share\_ckr',
        'new':'\\text{Foreign export share}_{ck, t-1}^R'
        },
        {
        'old':'lag\_soe\_export\_share\_ckr',
        'new':'\\text{SOE export share}_{ck, t-1}^R'
        },
        
         {
        'old':'lag\_stock\_ntm',
        'new':'\\text{Stock ntm Chinese import}_{k, t-1}'
        },
        
        {
        'old':'lag\_frequency',
        'new':'\\text{lag frequency}_{ck, t-1}'
        },
        
        {
        'old':'lag\_P',
        'new':'\\text{Prevalence}_{j, t-1}'
        },
        
        {
        'old':'lag\_coverage',
        'new':'\\text{lag coverage}_{ck, t-1}'
        },
        {
        'old':'c\_lag\_stock\_ntm\_w',
        'new':'\\text{Stock ntm destination country}_{jk, t-1}'
        },
        {
        'old':'d\_credit\_needs',
        'new':'\\text{Credit needs}_{k}'
        }
    ]

    data['to_rename'].extend(dic_rename)
    with open('schema_table.json', 'w') as outfile:
        json.dump(data, outfile)
```

```sos kernel="SoS"
sys.path.append(os.path.join(parent_path, 'utils'))
import latex.latex_beautify as lb
#%load_ext autoreload
#%autoreload 2
```

```sos kernel="R"
options(warn=-1)
library(tidyverse)
#library(lfe)
#library(lazyeval)
library('progress')
path = "../../../utils/latex/table_golatex.R"
source(path)
```

```sos kernel="R"
library(lfe)
```

```sos kernel="R"
library(tidyverse)
```

```sos kernel="R"
%get df_path
df_final <- read_csv(df_path) %>%
mutate_if(is.character, as.factor) %>%
    mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(
    regime = relevel(as.factor(regime), ref='NOT_ELIGIBLE'),
    ln_rebate = ln_lag_tax_rebate * (-1),
    ln_rebate_1 = log((lag_vat_reb_m / lag_vat_m) +1),
    ln_rebate_2 = log(lag_vat_reb_m + 1),
    rebate = lag_vat_reb_m / lag_vat_m
      ) 
```

```sos kernel="R"
df_final %>% select(lag_vat_reb_m, lag_vat_m, rebate) %>% head(2)
```

```sos kernel="R"
#df_final %>% group_by(regime) %>%summarize(mean(lag_F_C))
```

```sos kernel="R"
dim(df_final)
```

```sos kernel="SoS"
df_final.head()
```

<!-- #region kernel="SoS" -->
## Table 1: baseline estimate

$$\begin{aligned} \text{Quality}_{c,k,j, t}^{R} &=\alpha \ln \text{VAT Rebate}_{k, t-1} \times \text { Eligibility }^{R} +\alpha \ln \text{Import tax} \times \text { Eligibility }^{R} +X_{c, s, t-1}^{R}+F E_{c,k}^{R}+F E_{k,t}+F E_{j,t}+\epsilon_{c,k,j, t}^{R} \end{aligned} $$

 
Use ln_rebate_1 = log((lag_vat_reb_m / lag_vat_m) +1)  → share of rebate and FE → fe_ckr  + fe_kt + fe_jtr

* Baseline estimate
* Baseline estimate with more controls
* Controlling for product-country-year trends
* cities presents all years
* Keep rebates 17%
* Exclude rebates 0% 
<!-- #endregion -->

<!-- #region kernel="SoS" -->
### Quality
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="SoS"
#pd.read_csv('NTM_HS1996.csv')
```

```sos kernel="R"
dim(df_final %>% filter(regime != 'ELIGIBLE'))
```

```sos kernel="R"
dim(df_final %>% filter(regime == 'ELIGIBLE'))
```

```sos kernel="R"
%get path table
### Quality
t_0 <- felm(kandhelwal_quality ~
            rebate + 
            ln_lag_import_tax 
            #c_lag_stock_ntm_w #+ 
            #lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ck  + hs6 + fe_jt|0|hs6, df_final %>% filter(regime != 'ELIGIBLE'),
            exactDOF = TRUE)

t_1 <- felm(kandhelwal_quality ~
            rebate + 
            ln_lag_import_tax 
            #c_lag_stock_ntm_w #+ 
            #lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ck  + hs6 + fe_jt|0, df_final %>% filter(regime == 'ELIGIBLE'),
            exactDOF = TRUE)

### focus coef -> benchmark
#t_2 <- felm(kandhelwal_quality ~rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax 
#           | fe_ckr  + fe_kt + fe_jtr |0 | hs6, df_final,
#            exactDOF = TRUE)
#t_2 <- change_target(t_2)
print('table 0 done')

t_3 <- felm(kandhelwal_quality ~
            rebate* regime + 
            regime * c_lag_stock_ntm_w + 
            ln_lag_import_tax * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final,
            exactDOF = TRUE)
#t_3 <- change_target(t_3)
print('table 1 done')

t_4 <- felm(kandhelwal_quality ~
            rebate* regime + 
            regime * c_lag_stock_ntm_w + 
            ln_lag_import_tax * regime+
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr + fe_group_shock|0 | hs6, df_final,
            exactDOF = TRUE)
#t_4 <- change_target(t_4)
print('table 2 done')

t_5 <- felm(kandhelwal_quality ~
            rebate* regime + 
            regime * c_lag_stock_ntm_w + 
            ln_lag_import_tax * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% group_by(geocode4_corr) %>%
  mutate(length = length(unique(year))) %>%
  filter(length ==8),
            exactDOF = TRUE)
#t_5 <- change_target(t_5)
print('table 3 done')

t_6 <- felm(kandhelwal_quality ~
            rebate* regime +
            regime * c_lag_stock_ntm_w + 
            ln_lag_import_tax * regime+
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% filter(lag_vat_m==17),
            exactDOF = TRUE)
#t_6 <- change_target(t_6)
print('table 4 done')

t_7 <- felm(kandhelwal_quality ~
            rebate* regime + 
            regime * c_lag_stock_ntm_w + 
            ln_lag_import_tax * regime+
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% filter(lag_vat_reb_m != 0),
            exactDOF = TRUE)
#t_7 <- change_target(t_7)
print('table 5 done')
t_8 <- felm(kandhelwal_quality3 ~
            rebate* regime + 
            regime * c_lag_stock_ntm_w + 
            ln_lag_import_tax * regime+ 
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final,
            exactDOF = TRUE)
#t_8 <- change_target(t_8)
print('table 6 done')

t_9 <- felm(kandhelwal_quality5 ~
            rebate* regime + 
            regime * c_lag_stock_ntm_w + 
            ln_lag_import_tax * regime +
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final,
            exactDOF = TRUE)
#t_9 <- change_target(t_9)
print('table 6 done')

t_10 <- felm(kandhelwal_quality10 ~
             rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr + fe_group_shock|0 , df_final,
            exactDOF = TRUE)
#t_10 <- change_target(t_10)


dep <- "Dependent variable: Product quality"
fe1 <- list(
    c("product",
      "Yes", "Yes", "No", "No", "No","No", "No","No", "No","No"
     ),
    c("City-product-regime",
      "Yes", "Yes", "Yes", "Yes", "Yes","Yes", "Yes","Yes", "Yes","Yes"
     ),
    c("Product-year",
      "No", "No", "Yes", "Yes", "Yes","Yes", "Yes","Yes", "Yes","Yes"
     ),
    c("Destination-year",
      "Yes", "Yes", "Yes", "Yes", "Yes","Yes", "Yes","Yes", "Yes","Yes"
     )
)

table_1 <- go_latex(list(
    change_target(t_0),
    change_target(t_1), #t_2,
    change_target(t_3),
    change_target(t_4),
    change_target(t_5),
    change_target(t_6),
    change_target(t_7),
    change_target(t_8),
    change_target(t_9),
    change_target(t_10)
    #, t_8, t_9, t_10, t_11
),
    title="VAT export rebate and product's quality upgrading, baseline regression",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(XX)." \
" Ln VAT rebate is the share entitled to reimboursement at the HS6 product." \
" Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group." \
" Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund." \
" Sectors are defined following the Chinese 4-digit GB/T industry." \
" classification and regroup several products." \
" Heteroskedasticity-robust standard errors." \
" clustered at the product level appear inparentheses."\
" \sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

#multicolumn ={
#    'Quality': 4,
    #'Price-adjusted': 5,
#}
multicolumn ={
    'Non eligible':1,
    'Eligible':1,
    'Baseline':1,
    'Shocks': 1,
    'Balance': 1,
    'Only 17\%': 1,
    'No zero rebate': 1,
    'Sigma 3': 1,
    'Sigma 5': 1,
    'Sigma 10': 1
}
multi_lines_dep = '(city/product/trade regime/year)'
new_r = [
    #'& Eligible', 'Non-Eligible', 
    '& All','All benchmark', 'All', 'All benchmark',
    #'Eligible', 'Non-Eligible', 'All','All benchmark', 'All', 'All benchmark',
]

reorder = {
    2:1,
    5:2,
    7:3,
    6:5
}

lb.beautify(table_number = table_nb,
            reorder_var = reorder,
            multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
            folder = folder)
```

<!-- #region kernel="SoS" -->
## Table 2: Heterogeneity effect

- LDC and DC comes from the world bank classification, and are already in the table
- The list of homogeneous goods is in the S3
- Small/large city is be computed by using:
    - the number of product exported in 2003. If the count of product is above average, then it can be consider as a large firm 
    - The average quantity
 
 
* Column 1: Estimate baseline regression subset LDC countries: `income_group_ldc_dc` -> `LDC`
* Column 2: Estimate baseline regression subset DC countries: `income_group_ldc_dc` -> `DC`
* Column 3: Estimate baseline regression subset Homogeneous goods: `classification` -> `HOMOGENEOUS`
* Column 4: Estimate baseline regression subset heterogeneous goods: `classification` -> != `HOMOGENEOUS`
* Column 5: Estimate baseline regression subset small cities: `size_product` -> == `SMALL_COUNT`
* Column 6: Estimate baseline regression subset large cities: `size_product` -> == `LARGE_COUNT`
* Column 7: Estimate baseline regression subset small cities: `size_quantity` -> == `SMALL_QUANTITY`
* Column 8: Estimate baseline regression subset large cities: `size_quantity` -> == `LARGE_QUANTITY`

Sector is defined as the GBT 4 digits
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 2
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
#for ext in ['.txt', '.tex', '.pdf']:
#    x = [a for a in os.listdir(folder) if a.endswith(ext)]
#    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
temp <- df_final %>% 
filter(year == "2003") %>%
#group_by(year, geocode4_corr, hs2) %>%
group_by(geocode4_corr, hs4) %>%
summarize(sum_quantity = sum(quantity), sum_value = sum(value)) %>%
ungroup %>%
group_by(geocode4_corr) %>%
mutate(
    national_avg_q = mean(sum_quantity),
    national_m_q = median(sum_quantity),
    national_d_q =quantile(sum_quantity,probs = .5),
    national_avg_v = mean(sum_value),
    national_m_v = median(sum_value),
    national_d_v =quantile(sum_value,probs = .5),
    size_q_a = ifelse(sum_quantity > national_avg_q, 'LARGE', 'SMALL'),
    size_q_md = ifelse(sum_quantity > national_m_q, 'LARGE', 'SMALL'),
    size_q_d = ifelse(sum_quantity > national_d_q, 'LARGE', 'SMALL'),
    size_v_a = ifelse(sum_value > national_avg_v, 'LARGE', 'SMALL'),
    size_v_md = ifelse(sum_value > national_m_v, 'LARGE', 'SMALL'),
    size_v_d = ifelse(sum_value > national_d_v, 'LARGE', 'SMALL')
) %>%
right_join(df_final, by = c(
    #'year',
    'geocode4_corr',
    #"hs2",
    "hs4"
)) %>%
mutate(
    size_q_a = replace_na(size_q_a, "SMALL"),
    size_q_md = replace_na(size_q_md, "SMALL"),
    size_q_d = replace_na(size_q_d, "SMALL"),
    size_v_a = replace_na(size_v_a, "SMALL"),
    size_v_md = replace_na(size_v_md, "SMALL"),
    size_v_d = replace_na(size_v_d, "SMALL")
)
```

```sos kernel="R"
%get path table
#### COUNTRIES
t_0 <- felm(kandhelwal_quality ~
             rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% filter(income_group_ldc_dc == 'LDC'),
            exactDOF = TRUE)
t_0 <- change_target(t_0)
print('table 0 done')

t_1 <- felm(kandhelwal_quality ~
             rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% filter(income_group_ldc_dc != 'LDC'),
            exactDOF = TRUE)
t_1 <- change_target(t_1)
print('table 1 done')
#### GOODS
t_2 <- felm(kandhelwal_quality ~
            rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% filter(is.na(homogeneous) | homogeneous == 'HOMOGENEOUS'),
            exactDOF = TRUE)
t_2 <- change_target(t_2)
print('table 2 done')

t_3 <- felm(kandhelwal_quality ~
            rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% filter(homogeneous == 'HETEREGENEOUS'),
            exactDOF = TRUE)
t_3 <- change_target(t_3)
print('table 3 done')
#### CITIES
##### city-industry
t_4 <- felm(kandhelwal_quality ~
            rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, temp %>% filter(size_q_d == 'SMALL'),
            exactDOF = TRUE)
t_4 <- change_target(t_4)
print('table 4 done')

t_5 <- felm(kandhelwal_quality ~
            rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, temp %>% filter(size_q_d == 'LARGE'),
            exactDOF = TRUE)
t_5 <- change_target(t_5)
print('table 5 done')

##### Quantity
#t_6 <- felm(kandhelwal_quality ~ln_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax
#            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% filter(size_quantity == 'SMALL_QUANTITY'),
#            exactDOF = TRUE)
#t_6 <- change_target(t_6)
#print('table 6 done')

#t_7 <- felm(kandhelwal_quality ~ln_rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax
#            | fe_ckr + fe_csrt+fe_kt|0 | hs6, df_final %>% filter(size_quantity == 'LARGE_QUANTITY'),
#            exactDOF = TRUE)
#t_7 <- change_target(t_7)
#print('table 7 done')

dep <- "Dependent variable: Product quality"
fe1 <- list(
    c("City-product-regime","Yes", "Yes", "Yes", "Yes","Yes", "Yes"),
    
    c("Product-year","Yes", "Yes", "Yes", "Yes","Yes", "Yes"),
    
    c("Destination-year", "Yes", "Yes", "Yes", "Yes","Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="VAT export tax and firm’s quality upgrading, characteristics of the destination countries, products, and cities",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = """
This table estimates eq(3). 
LDC and DC are defined according to the World Bank country classification.
Homogeneous and heterogeneous goods are defined according to the official list of goods`s classification, Rauch (1999).
Small and large are computed based on the total quantity exported by city-HS4.
When oto total export by city-HS4 above the city average, then the pair city-industry is considered as large.
Note that 'Eligible' refers to the regime entitle to VAT refund, our treatment group.
Our control group is processing trade with supplied input, 'Non-Eligible' to VAT refund.
Sectors are defined following the Chinese 4-digit GB/T industry
classification and regroup several products.
Heteroskedasticity-robust standard errors
clustered at the product level appear inparentheses.
\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."""

multicolumn ={
    'LDC': 1,
    'DC': 1,
    'Homogeneous': 1,
    'Heterogeneous': 1,
    'Small': 1,
    'Large': 1
}
multi_lines_dep = '(city/product/trade regime/year)'
reorder = {
    3:0,
    5:1,
    4:3
}
#new_r = ['& Eligible', 'Non-Eligible', 'All', 'All benchmark']
lb.beautify(table_number = table_nb,
            #multi_lines_dep = None,
            reorder_var = reorder,
            multi_lines_dep = multi_lines_dep,
            new_row= False,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 180,
           folder = folder)
```

<!-- #region kernel="SoS" -->
## Table 3: Industry characteristicts

* Column 1 excludes rare earth products with the main fixed effect:
* Column 2 excludes energy intensive industries with the main fixed effect:
* Column 3 excludes high tech industries with the main fixed effect:
* Column 4 excludes RD oriented indusrtries with the main fixed effect:
* Column 5 excludes High skilled oriented with the main fixed effect:
  
Sector is defined as the GBT 4 digits
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 3
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
#for ext in ['.txt', '.tex', '.pdf']:
#    x = [a for a in os.listdir(folder) if a.endswith(ext)]
#    [os.remove(os.path.join(folder, i)) for i in x]
```

<!-- #region kernel="SoS" -->
### Download polluted sectors
<!-- #endregion -->

```sos kernel="SoS"
db = 'environment'
query = """
WITH temp AS (
SELECT ind2, SUM(tso2) as sum_tso2, SUM(tso2) / SUM(ttoutput) as so2_intensity 
FROM environment.china_city_sector_pollution  
WHERE year = '2002'
GROUP BY ind2
ORDER BY sum_tso2
  )
  SELECT *
  FROM temp
  LEFT JOIN (
    SELECT cic as ind2, short
    FROM chinese_lookup.ind_cic_2_name
    ) as ind_name
    ON temp.ind2 = ind_name.ind2
"""
list_polluted = s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename='polluted',  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
        )
list_polluted
```

```sos kernel="SoS"
(
    list_polluted.assign(
        polluted = lambda x: x['sum_tso2'] >=np.quantile(x['sum_tso2'], 0.5),
        polluted_int = lambda x: x['so2_intensity'] >=np.quantile(x['so2_intensity'], 0.5),
    )
    .reindex(columns = ['ind2', 'short', 'polluted', 'polluted_int'])
    .rename(columns = {'indus_code':'hs4','ind2':'hs2'})
    .to_csv('polluted_vs_no_polluted.csv', index = False)
)
```

```sos kernel="R"
df_final_polluted <-  df_final %>% left_join(
    read_csv('polluted_vs_no_polluted.csv',
             col_types = cols(hs2 = col_double()))
) %>%
mutate_at(c('polluted', 'polluted_int'), ~replace(., is.na(.), FALSE))
```

```sos kernel="R"
table(df_final_polluted$polluted)
```

```sos kernel="R"
table(df_final_polluted$polluted_int)
```

<!-- #region kernel="SoS" -->
### Download high tech sectors

- We use the economic complexity: https://atlas.cid.harvard.edu/rankings/product and saved in Google Drive: [Product Complexity Rankings 1995 - 2019](https://docs.google.com/spreadsheets/d/1Jzef1jfTT-vMn80cQz1pVgF85QAIxKVIKZhbMUvYxT8/edit?usp=sharing)
- see paper: https://www.atlantis-press.com/article/55913101.pdf

![image.png](attachment:dcd45d20-44f0-47f3-bab3-ffe32026149f.png)
<!-- #endregion -->

```sos kernel="SoS"
#!pip install git+https://github.com/thomaspernet/GoogleDrive-python.git
```

```sos kernel="python3"
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
import os
import numpy as np
import pandas as pd
```

```sos kernel="SoS"
try:
    os.mkdir("creds")
except:
    pass
```

```sos kernel="SoS"
s3.download_file(key = "CREDS/Financial_dependency_pollution/creds/token.pickle", path_local = "creds")
```

```sos kernel="python3"
auth = authorization_service.get_authorization(
    #path_credential_gcp=os.path.join(parent_path, "creds", "service.json"),
    path_credential_drive=os.path.join(os.getcwd(), "creds"),
    verbose=False,
    scope=['https://www.googleapis.com/auth/spreadsheets.readonly',
           "https://www.googleapis.com/auth/drive"]
)
gd_auth = auth.authorization_drive(path_secret=os.path.join(
    os.getcwd(), "creds"))
drive = connect_drive.drive_operations(gd_auth)
```

```sos kernel="python3"
FILENAME_SPREADSHEET = "Product Complexity Rankings 1995 - 2019"
spreadsheet_id = drive.find_file_id(FILENAME_SPREADSHEET, to_print=False)
df_complexity = (
    drive.download_data_from_spreadsheet(
        sheetID=spreadsheet_id,
        sheetName="Product Complexity Rankings 1995 - 2019.csv",
        to_dataframe=True,
    )
    .rename(columns={"PCI 2002": "PCI_2002"})
    .assign(PCI_2002=lambda x: pd.to_numeric(x["PCI_2002"]))
    .assign(
        hs4=lambda x: np.where(
            x["HS4 Code"].astype("str").str.len() == 3,
            "0" + x["HS4 Code"].astype("str"),
            x["HS4 Code"].astype("str"),
        ),
        rank_2002=lambda x: pd.qcut(
            x["PCI_2002"], 4, labels=["Low-tech Complexity", 
                                      "Lower-middle-tech",
                                      "Upper-middle-tech",
                                     "High-tech"]
        ),
        dummy_tech_1 = lambda x: x['rank_2002'].isin(["Upper-middle-tech",
                                     "High-tech"]), # 50%
        dummy_tech_2 = lambda x: x['rank_2002'].isin(["High-tech"]), #75%
        dummy_tech_3 = lambda x:  x['PCI_2002'] >=0.910580, #80%
        dummy_tech_4 = lambda x:  x['PCI_2002'] >=1.271060, #90%
        dummy_tech_5 = lambda x:  x['PCI_2002'] >=1.535260 #95%
    )
)
df_complexity.to_csv('rank_complexity.csv', index = False)
```

```sos kernel="python3"
df_complexity.head(2)
```

```sos kernel="python3"
df_complexity['PCI_2002'].describe(percentiles= [.75, .80, .90, .95])
```

```sos kernel="python3"
df_complexity.groupby('rank_2002')['PCI_2002'].describe()
```

```sos kernel="python3"
df_complexity.loc[lambda x: x['rank_2002'].isin(['High-tech'])].sort_values(by = ['PCI_2002']).tail()['Product']
```

```sos kernel="R"
df_final_1 <- df_final %>% 
left_join(read_csv('rank_complexity.csv',
                                           col_types = cols(
                                               hs4 = col_double()
                                           )
                                          )%>%select(Product,
                                           hs4,
                                           rank_2002,
                                           dummy_tech_1,
                                           dummy_tech_2,
                                           dummy_tech_3,
                                                     dummy_tech_4,
                                                     dummy_tech_5
                                                    )) %>%
mutate_at(c('rank_2002'), ~replace(., is.na(.), 'Low-tech Complexity'))%>%
mutate_at(c('dummy_tech_1',
            'dummy_tech_2',
            'dummy_tech_3',
            'dummy_tech_4',
            'dummy_tech_5'
           ), ~replace(., is.na(.), FALSE),
         )

```

```sos kernel="R"
table(df_final_1$dummy_tech_1)
```

```sos kernel="R"
table(df_final_1$dummy_tech_2)
```

```sos kernel="R"
table(df_final_1$dummy_tech_3)
```

```sos kernel="R"
table(df_final_1$dummy_tech_4)
```

```sos kernel="R"
table(df_final_1$dummy_tech_5)
```

<!-- #region kernel="R" -->
### Download RD oriented
<!-- #endregion -->

```sos kernel="SoS"
query = """
WITH test AS (
SELECT SUBSTRING(cic, 1, 2) as hs2,
SUM(rdfee) as rdfee,
SUM(total_asset) as total_asset,
CAST(
          SUM(rdfee) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS rd_total_asset,
CAST(
          SUM(rdfee) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(output) AS DECIMAL(16, 5)
          ), 
          0
        ) AS rd_output
FROM (
SELECT 
cic, rdfee,output,  CASE WHEN (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) - (c95 + c97 + c99) > 0 THEN (c95 + c97 + c99) + ABS(
      (
        c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
      ) - (c95 + c97 + c99)
    ) ELSE (c95 + c97 + c99) END AS total_right, 
    CASE WHEN (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) - (c95 + c97 + c99) < 0 THEN (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) + ABS(
      (
        c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
      ) - (c95 + c97 + c99)
    ) ELSE (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) END AS total_asset
    FROM firms_survey.asif_firms_prepared  
WHERE year = '2005'
)
GROUP BY SUBSTRING(cic, 1, 2)
ORDER BY rd_total_asset
) 
SELECT *
FROM test
LEFT JOIN (
    SELECT cic as ind2, short
    FROM chinese_lookup.ind_cic_2_name
    ) as ind_name
    ON test.hs2 = ind_name.ind2
"""
list_rd = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename='rd',  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
        )
           .assign(
        rd_asset = lambda x: x['rd_total_asset'] >=np.quantile(x['rd_total_asset'], 0.75),
        rd_output = lambda x: x['rd_output'] >=np.quantile(x['rd_output'], 0.75)
    )
    .reindex(columns = [
        #'hs4',
        'hs2', 'rd_asset', 'rd_output', 'short'
    ])
           .rename(columns = {'hs2':'cic2'})
          )
(
    list_rd
    .to_csv('rd_vs_no_rd.csv', index = False)
)
list_rd
```

```sos kernel="SoS"
db = 'industry'
query = """
SELECT * FROM "industry"."industry_hs_gbt"
"""
df_condordance = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename='polluted',  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
        )
                  .assign(
        hs96=lambda x: np.where(x['hs96'].isin([np.nan]),
                                np.nan,
                                x['hs96'].astype('Int64').astype(
                                    'str').str.zfill(6)
                                ),
        hs2002=lambda x: np.where(x['hs2002'].isin([np.nan]),
                                  np.nan,
                                  x['hs2002'].astype('Int64').astype(
                                      'str').str.zfill(6)
                                  ),
        cic_adj=lambda x: np.where(x['cic_adj'].isin([np.nan]),
                                   np.nan,
                                   x['cic_adj'].astype('Int64').astype(
                                       'str').str.zfill(4)
                                   ),
        cic03=lambda x: np.where(x['cic03'].isin([np.nan]),
                                 np.nan,
                                 x['cic03'].astype('Int64').astype(
                                     'str').str.zfill(4)
                                 ),
        cic02=lambda x: np.where(x['cic02'].isin([np.nan]),
                                 np.nan,
                                 x['cic02'].astype('Int64').astype(
                                     'str').str.zfill(4)
                                 )
    )
                  .drop(columns = ['hs96'])
                  .dropna()
                  .drop_duplicates(subset = ['hs2002'])
                  .assign(cic2 = lambda x: x['cic_adj'].str.slice(stop =2))
                  .rename(columns = {'hs2002':'hs6'})
                 )
(
    df_condordance.to_csv('concordance.csv', index = False)
)
df_condordance
```

```sos kernel="R"
list_polluting = list(
    6,8,10, 
    #14
    15, 17, 19,
    #22,
    25,26,
    #27,
    31
    #,32, 33, 44
)
```

```sos kernel="R"
df_final_rd_pol <- df_final %>% left_join(
    read.csv(
        'concordance.csv',
        colClasses=c(
            "hs6"="character",
            "cic_adj"="character",
            "cic03"="character",
            "cic02"="character",
            "cic2"="character"
        )
    ) %>% 
    mutate(hs6 = as.numeric(hs6))) %>%
    left_join(
    read_csv('polluted_vs_no_polluted.csv',
             col_types = cols(hs2 = col_character()) 
            )%>%
            rename(cic2 = hs2)
) %>%
mutate_at(c('polluted', 'polluted_int'), ~replace(., is.na(.), FALSE))%>% left_join(
    read_csv('rd_vs_no_rd.csv',
             col_types = cols(
                 cic2 = col_character()
             ))
) %>%
mutate_at(c('rd_asset','rd_output'), ~replace(., is.na(.), FALSE))
dim(df_final_rd_pol)
```

```sos kernel="R"
dim(df_final_rd_pol %>% filter(rd_output == TRUE))
```

```sos kernel="R"
summary(felm(kandhelwal_quality ~
            rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final_rd_pol %>% filter(rd_output == TRUE),
            exactDOF = TRUE))
```

```sos kernel="R"
summary(felm(kandhelwal_quality ~
            rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final_rd_pol %>% filter(rd_output != TRUE),
            exactDOF = TRUE))
```

<!-- #region kernel="SoS" -->
### subset
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 4
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
```

```sos kernel="R"
%get path table
#### RARE HEARTH
#t_0 <- felm(kandhelwal_quality ~rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax+
#            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
#            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% filter(hs6 != 850511),
#            exactDOF = TRUE)
#t_0 <- change_target(t_0)
#print('table 0 done')

#t_1 <- felm(kandhelwal_quality ~rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax+
#            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
#            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% filter(hs6 == 850511),
#            exactDOF = TRUE)
#t_1 <- change_target(t_1)
#print('table 0 done')

#### LARGE POLLUTED INDUSTRY
t_1 <- felm(kandhelwal_quality ~
             rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final_polluted %>%filter(cic2 %in% list_polluting),
            exactDOF = TRUE)
t_1 <- change_target(t_1)
print('table 1 done')

#### NO LARGE POLLUTED INDUSTRY
t_2 <- felm(kandhelwal_quality ~
             rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final_polluted %>%filter(!cic2 %in% list_polluting),
            exactDOF = TRUE)
t_2 <- change_target(t_2)
print('table 1 done')

#### LARGEST COMPLEXITY ->ONE WITH BEST POTENTIAL
t_3 <- felm(kandhelwal_quality ~
            rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final_1 %>% filter(dummy_tech_2 == TRUE),
            exactDOF = TRUE)
t_3 <- change_target(t_3)
print('table 2 done')

t_4 <- felm(kandhelwal_quality ~
            rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final_1 %>% filter(dummy_tech_2 == FALSE),
            exactDOF = TRUE)
t_4 <- change_target(t_4)
print('table 2 done')
#### SKILLED
#t_6 <- felm(kandhelwal_quality ~rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax+
#            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
#            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% filter(is.na(skilled)),
#            exactDOF = TRUE)
#t_6 <- change_target(t_6)
#print('table 3 done')

#t_7 <- felm(kandhelwal_quality ~rebate* regime + ln_lag_import_tax * regime+ ln_lag_import_tax+
#            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
#            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final %>% filter(!is.na(skilled)),
#            exactDOF = TRUE)
#t_7 <- change_target(t_7)
#print('table 3 done')
##### RD
t_5 <- felm(kandhelwal_quality ~
            rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final_rd_pol %>% filter(rd_output == TRUE),
            exactDOF = TRUE)
t_5 <- change_target(t_5)
print('table 3 done')

t_6 <- felm(kandhelwal_quality ~
            rebate* regime + 
             regime * c_lag_stock_ntm_w + 
             ln_lag_import_tax * regime+
             lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 | hs6, df_final_rd_pol %>% filter(rd_output == FALSE),
            exactDOF = TRUE)
t_6 <- change_target(t_6)
print('table 3 done')

dep <- "Dependent variable: Product quality"
fe1 <- list(
    c("City-product-regime","Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("Product-year","Yes", "Yes", "Yes", "Yes","Yes","Yes"),
    
    c("Destination-year", "Yes", "Yes", "Yes", "Yes","Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_1, t_2, t_3, t_4, t_5, t_6
),
    title="VAT export tax and firm’s quality upgrading, characteristics of sensible sectors",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
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
    #'Rare-earth': 2,
    'Polluted intensive': 2,
    'High tech': 2,
    'RD oriented': 2,
    #1'High skilled oriented': 2
}
reorder = {
    3:2,
    #5:1,
    #4:3
}
multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Yes', 'No', 'Yes', 'No', 'Yes', 'No']
lb.beautify(table_number = table_nb,
            #multi_lines_dep = None,
            reorder_var = reorder,
            multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 180,
            folder = folder)
```

<!-- #region kernel="SoS" -->
# Credit Constraint
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 5
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
```

```sos kernel="SoS"
pd.read_csv(manova,
                    dtype={"ISIC": "str"},
                ).describe()
```

```sos kernel="SoS"
pd.read_csv(jarreau,
                    dtype={"ISIC": "str"},
                ).describe()
```

```sos kernel="SoS"
#df_final[['ext_finance_m','ext_finance_j','rd_intensity_j', 'liquidity_needs_m', 'asset_tangibility_m', "trade_credit_intensity_m"]].describe()
```

```sos kernel="R"
summary(felm(kandhelwal_quality ~
    d_credit_needs+ 
    
    #regime * c_lag_stock_ntm_w + 
    #ln_lag_import_tax * regime+
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_kt + fe_jt|0 , df_final %>%
     mutate(d_credit_needs = ifelse(ext_finance_j >0.220000, FALSE, TRUE)),
            exactDOF = TRUE)
       )

```

```sos kernel="R"
%get path table
#### External finance J: OK
t_0 <- felm(kandhelwal_quality ~
     rebate* regime * d_credit_needs+ 
    regime * c_lag_stock_ntm_w + 
     ln_lag_import_tax * regime+
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 , df_final %>%
     mutate(d_credit_needs = ifelse(ext_finance_j >0.220000, FALSE, TRUE)),
            exactDOF = TRUE)

### liquidity: OK
t_1 <- felm(kandhelwal_quality ~
     rebate* regime * d_credit_needs+ 
    regime * c_lag_stock_ntm_w + 
     ln_lag_import_tax * regime+
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 , df_final %>%
     mutate(d_credit_needs = ifelse(liquidity_needs_j >0.200000, TRUE, FALSE)),
            exactDOF = TRUE)

### Trade credit 
t_2 <- felm(kandhelwal_quality ~
     rebate* regime * d_credit_needs+ 
    regime * c_lag_stock_ntm_w + 
     ln_lag_import_tax * regime+
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 , df_final %>%
     mutate(d_credit_needs = ifelse(trade_credit_intensity_m >0.080000, FALSE, TRUE)),
            exactDOF = TRUE)

### Asset tangibility
t_3 <- felm(kandhelwal_quality ~
     rebate* regime * d_credit_needs+ 
    regime * c_lag_stock_ntm_w + 
     ln_lag_import_tax * regime+
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 , df_final %>%
     mutate(d_credit_needs = ifelse(asset_tangibility_m >0.220000, FALSE, TRUE)),
            exactDOF = TRUE)

### RD intensity
t_4 <- felm(kandhelwal_quality ~
     rebate* regime * d_credit_needs+ 
     regime * c_lag_stock_ntm_w + 
     ln_lag_import_tax * regime+
            lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr|0 , df_final %>%
     mutate(d_credit_needs = ifelse(rd_intensity_j >0.010000, FALSE, TRUE)),
            exactDOF = TRUE)
dep <- "Dependent variable: Product quality"
fe1 <- list(
    c("City-product-regime","Yes", "Yes", "Yes", "Yes","Yes","Yes","Yes","Yes"),
    
    c("Product-year","Yes", "Yes", "Yes", "Yes","Yes","Yes","Yes","Yes"),
    
    c("Destination-year", "Yes", "Yes", "Yes", "Yes","Yes", "Yes","Yes","Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, 
    #t_2,
    t_3, t_4#, t_5#, t_6
),
    title="VAT export tax and firm’s quality upgrading NTM anaysis",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
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
    'Baseline': 1,
    'Environnement': 1,
    'classification': 3,
    'Developped': 1,
    #'Destination': 1
}
reorder = {
    3:1,
    #5:1
}
multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Ext. Fin','Liquidity needs',
         #'Trade Credit', 
         'Asset Tang.', 'RD intensity']
lb.beautify(table_number = table_nb,
            #multi_lines_dep = None,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 180,
            folder = folder)
```

<!-- #region kernel="SoS" -->
# Non Tariff Measure

- Model is located here: [checkpoint-2562](https://drive.google.com/drive/folders/13rpnm5X5UG-MgLAFb8r1Pu2F-hvGA443?usp=sharing)
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 6
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')

#for ext in ['.txt', '.tex', '.pdf']:
#    x = [a for a in os.listdir(folder) if a.endswith(ext)]
#    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
t_0 <- felm(
    kandhelwal_quality ~
    rebate * regime * lag_stock_ntm + 
    rebate * regime * c_lag_stock_ntm_w + 
    rebate * regime * lag_P + 
    ln_lag_import_tax * regime +
    lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr |0 , df_final %>% 
    rename(lag_stock_ntm = c_lag_stock_ntm_china),
            exactDOF = TRUE)
   
#### ENV
t_1 <- felm(
    kandhelwal_quality ~
    rebate * regime * lag_stock_ntm + 
    rebate * regime * c_lag_stock_ntm_w + 
    rebate * regime * lag_P + 
    ln_lag_import_tax * regime  +
    lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr |0 , df_final %>%
    rename(lag_stock_ntm = c_lag_stock_ntm_env_china),
            exactDOF = TRUE)

### TBT
t_2 <- felm(
    kandhelwal_quality ~
    rebate * regime * lag_stock_ntm + 
    rebate * regime * c_lag_stock_ntm_w + 
    rebate * regime * lag_P + 
    ln_lag_import_tax * regime  +
    lag_foreign_export_share_ckr + lag_soe_export_share_ckr
            | fe_ckr  + fe_kt + fe_jtr |0 , df_final %>%
    rename(lag_stock_ntm = c_lag_stock_ntm_TBT_SPS_china),
            exactDOF = TRUE)

dep <- "Dependent variable: Product quality"
fe1 <- list(
    c("City-product-regime","Yes", "Yes", "Yes", "Yes","Yes","Yes","Yes","Yes"),
    
    c("Product-year","Yes", "Yes", "Yes", "Yes","Yes","Yes","Yes","Yes"),
    
    c("Destination-year", "Yes", "Yes", "Yes", "Yes","Yes", "Yes","Yes","Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2#, t_3, t_4, t_5#, t_6
),
    title="VAT export tax and firm’s quality upgrading NTM anaysis",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
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
    'Baseline': 1,
    'Environnement': 1,
    'classification': 3,
    'Developped': 1,
    #'Destination': 1
}
reorder = {
    12:2,
    #5:1
}
multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& baseline', 'environment', 'TBT/SPS']
lb.beautify(table_number = table_nb,
            #multi_lines_dep = None,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 180,
            folder = folder)
```

<!-- #region kernel="SoS" nteract={"transient": {"deleting": false}} -->
# Generate reports
<!-- #endregion -->

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
import sys
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
sys.path.append(os.path.join(parent_path, 'utils'))
import make_toc
```

```sos kernel="python3"
name_json = 'parameters_ETL_VAT_rebate_quality_china.json'
path_json = os.path.join(str(Path(path).parent.parent), 'utils',name_json)
```

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
def create_report(extension = "html", keep_code = False, notebookname = None):
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
            notebookname = notebookname  
    
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

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
create_report(extension = "html", keep_code = True, notebookname = "00_baseline_vat_quality.ipynb")
```

```sos kernel="python3"
### Update TOC in Github
for p in [parent_path,
          str(Path(path).parent),
          #os.path.join(str(Path(path).parent), "00_download_data_from"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis", "00_statistical_exploration"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis", "01_model_estimation"),
         ]:
    try:
        os.remove(os.path.join(p, 'README.md'))
    except:
        pass
    path_parameter = os.path.join(parent_path,'utils', name_json)
    md_lines =  make_toc.create_index(cwd = p, path_parameter = path_parameter)
    md_out_fn = os.path.join(p,'README.md')
    
    if p == parent_path:
    
        make_toc.replace_index(md_out_fn, md_lines, Header = os.path.basename(p).replace('_', ' '), add_description = True, path_parameter = path_parameter)
    else:
        make_toc.replace_index(md_out_fn, md_lines, Header = os.path.basename(p).replace('_', ' '), add_description = False)
```
