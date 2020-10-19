
# Data Catalogue



## Table of Content

    
- [base_hs6_VAT_2002_2012](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-base_hs6_VAT_2002_2012)
- [applied_mfn_tariffs_hs02_china_2002_2010](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-applied_mfn_tariffs_hs02_china_2002_2010)
- [sigma_industry](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-sigma_industry)
- [country_cn_en](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-country_cn_en)
- [city_cn_en](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-city_cn_en)
- [import_export](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-import_export)
- [quality_vat_export_2003_2010](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-quality_vat_export_2003_2010)
- [VAT_export_2003_2010](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-VAT_export_2003_2010)
- [lag_foreign_export_ckjr](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-lag_foreign_export_ckjr)
- [lag_foreign_export_ckr](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-lag_foreign_export_ckr)
- [lag_soe_export_ckjr](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-lag_soe_export_ckjr)
- [lag_soe_export_ckr](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-lag_soe_export_ckr)
- [quality_vat_export_covariate_2003_2010](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-quality_vat_export_covariate_2003_2010)

    

## Table base_hs6_vat_2002_2012

- Owner: hadoop 
- Database: chinese_trade
- Filename: base_hs6_vat_2002_2012
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/TAX_DATA/TRANSFORMED/VAT_REBATE
- S3uri: `s3://chinese-data/TAX_DATA/TRANSFORMED/VAT_REBATE`


|    | Name            | Type   | Comment              |
|---:|:----------------|:-------|:---------------------|
|  0 | hs6             | string |                      |
|  1 | year            | string |                      |
|  2 | tax_rebate      | float  |                      |
|  3 | ln_vat_rebate   | float  | log (1 + tax_rebate) |
|  4 | vat_m           | float  |                      |
|  5 | vat_reb_m       | float  |                      |
|  6 | ln_vat_rebate_m | float  | log (1 + vat_reb_m)  |

    

## Table applied_mfn_tariffs_hs02_china_2002_2010

- Owner: hadoop 
- Database: chinese_trade
- Filename: applied_mfn_tariffs_hs02_china_2002_2010
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/TAX_DATA/TRANSFORMED/APPLIED_MFN_TARIFFS
- S3uri: `s3://chinese-data/TAX_DATA/TRANSFORMED/APPLIED_MFN_TARIFFS`


|    | Name       | Type   | Comment   |
|---:|:-----------|:-------|:----------|
|  0 | reporter   | string |           |
|  1 | year       | string |           |
|  2 | import_tax | float  |           |
|  3 | hs02       | string | nan       |

    

## Table sigma_industry

- Owner: hadoop 
- Database: chinese_trade
- Filename: sigma_industry
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/ADDITIONAL_DATA/SIGMAS_HS3
- S3uri: `s3://chinese-data/ADDITIONAL_DATA/SIGMAS_HS3`


|    | Name   | Type   | Comment      |
|---:|:-------|:-------|:-------------|
|  0 | ccode  | string | Country code |
|  1 | cname  | string | countr name  |
|  2 | sigma  | float  | sigma        |
|  3 | hs3    | string | nan          |

    

## Table country_cn_en

- Owner: hadoop 
- Database: chinese_lookup
- Filename: country_cn_en
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/LOOKUP_DATA/COUNTRY_NAME
- S3uri: `s3://chinese-data/LOOKUP_DATA/COUNTRY_NAME`


|    | Name       | Type   | Comment                 |
|---:|:-----------|:-------|:------------------------|
|  0 | country_cn | string | Country name in Chinese |
|  1 | country_en | string | Country name in English |
|  2 | iso_alpha  | string | Country code            |
|  3 | code_2     | string | Country code WB         |

    

## Table city_cn_en

- Owner: hadoop 
- Database: chinese_lookup
- Filename: city_cn_en
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/LOOKUP_DATA/CITY_NAME
- S3uri: `s3://chinese-data/LOOKUP_DATA/CITY_NAME`


|    | Name          | Type   | Comment                  |
|---:|:--------------|:-------|:-------------------------|
|  0 | extra_code    | string | Correspondence code      |
|  1 | geocode4_corr | string | Official code            |
|  2 | citycn        | string | City name in Chinese     |
|  3 | cityen        | string | City name in English     |
|  4 | province_cn   | string | Province name in Chinese |
|  5 | province_en   | string | Province name in English |

    

## Table import_export

- Owner: hadoop 
- Database: chinese_trade
- Filename: import_export
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/TRADE_DATA/TRANSFORMED
- S3uri: `s3://chinese-data/TRADE_DATA/TRANSFORMED`


|    | Name                  | Type   | Comment   |
|---:|:----------------------|:-------|:----------|
|  0 | date                  | string |           |
|  1 | id                    | string | nan       |
|  2 | business_type         | string |           |
|  3 | intermediate          | string |           |
|  4 | trade_type            | string |           |
|  5 | province              | string |           |
|  6 | city_prod             | string |           |
|  7 | matching_city         | string |           |
|  8 | imp_exp               | string |           |
|  9 | hs                    | string |           |
| 10 | origin_or_destination | string |           |
| 11 | value                 | int    |           |
| 12 | quantities            | int    |           |

    

## Table quality_vat_export_2003_2010

- Owner: hadoop 
- Database: chinese_trade
- Filename: quality_vat_export_2003_2010
- Location: https://s3.console.aws.amazon.com/s3/buckets/vat-rebate-quality/DATA/TRANSFORMED/QUALITY/BASELINE
- S3uri: `s3://vat-rebate-quality/DATA/TRANSFORMED/QUALITY/BASELINE`


|    | Name                   | Type   | Comment                                                                                                                                                              |
|---:|:-----------------------|:-------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | cityen                 | string |                                                                                                                                                                      |
|  1 | geocode4_corr          | string |                                                                                                                                                                      |
|  2 | year                   | string |                                                                                                                                                                      |
|  3 | regime                 | string | Eligible to the rebate or not                                                                                                                                        |
|  4 | hs6                    | string |                                                                                                                                                                      |
|  5 | hs4                    | string |                                                                                                                                                                      |
|  6 | hs3                    | string |                                                                                                                                                                      |
|  7 | country_en             | string |                                                                                                                                                                      |
|  8 | iso_alpha              | string |                                                                                                                                                                      |
|  9 | quantity               | int    |                                                                                                                                                                      |
| 10 | value                  | int    |                                                                                                                                                                      |
| 11 | unit_price             | float  |                                                                                                                                                                      |
| 12 | kandhelwal_quality     | float  | kandhelwal quality. cf https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/01_preparation_quality.md  |
| 13 | price_adjusted_quality | float  | price adjusted quality. https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/01_preparation_quality.md |
| 14 | lag_tax_rebate         | float  |                                                                                                                                                                      |
| 15 | ln_lag_tax_rebate      | float  |                                                                                                                                                                      |
| 16 | lag_vat_reb_m          | float  |                                                                                                                                                                      |
| 17 | ln_lag_vat_reb_m       | float  |                                                                                                                                                                      |
| 18 | lag_import_tax         | float  |                                                                                                                                                                      |
| 19 | ln_lag_import_tax      | float  |                                                                                                                                                                      |
| 20 | sigma                  | float  |                                                                                                                                                                      |
| 21 | sigma_price            | float  |                                                                                                                                                                      |
| 22 | y                      | float  | log quantity plus sigma                                                                                                                                              |
| 23 | prediction             | float  |                                                                                                                                                                      |
| 24 | residual               | float  |                                                                                                                                                                      |
| 25 | fe_ck                  | string | nan                                                                                                                                                                  |
| 26 | fe_cst                 | string | nan                                                                                                                                                                  |
| 27 | fe_ckr                 | string | nan                                                                                                                                                                  |
| 28 | fe_csrt                | string | nan                                                                                                                                                                  |
| 29 | fe_kt                  | string | nan                                                                                                                                                                  |
| 30 | fe_kj                  | string | nan                                                                                                                                                                  |
| 31 | fe_jt                  | string | nan                                                                                                                                                                  |
| 32 | fe_ckj                 | string | nan                                                                                                                                                                  |

    

## Table vat_export_2003_2010

- Owner: 468786073381 
- Database: chinese_trade
- Filename: vat_export_2003_2010
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/ATHENA/MAIN/tables/2e1926c3-cb90-41f6-b149-295185a69253/
- S3uri: `s3://chinese-data/ATHENA/MAIN/tables/2e1926c3-cb90-41f6-b149-295185a69253/`


|    | Name              | Type          | Comment   |
|---:|:------------------|:--------------|:----------|
|  0 | cityen            | string        |           |
|  1 | geocode4_corr     | string        |           |
|  2 | year              | string        |           |
|  3 | regime            | varchar(12)   |           |
|  4 | hs6               | string        |           |
|  5 | country_en        | string        |           |
|  6 | iso_alpha         | string        |           |
|  7 | quantity          | bigint        |           |
|  8 | value             | bigint        |           |
|  9 | unit_price        | decimal(21,5) |           |
| 10 | lag_tax_rebate    | float         |           |
| 11 | ln_lag_tax_rebate | double        |           |
| 12 | lag_vat_reb_m     | float         |           |
| 13 | ln_lag_vat_reb_m  | double        |           |
| 14 | lag_import_tax    | float         |           |
| 15 | ln_lag_import_tax | double        |           |

    

## Table lag_foreign_export_ckjr

- Owner: 468786073381 
- Database: chinese_trade
- Filename: lag_foreign_export_ckjr
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/ATHENA/MAIN/tables/85fdd28e-e03b-437a-8ae3-2c46376c175f/
- S3uri: `s3://chinese-data/ATHENA/MAIN/tables/85fdd28e-e03b-437a-8ae3-2c46376c175f/`


|    | Name                          | Type          | Comment      |
|---:|:------------------------------|:--------------|:-------------|
|  0 | year                          | string        |              |
|  1 | year_lag                      | string        |              |
|  2 | regime                        | varchar(12)   |              |
|  3 | foreign_ownership             | varchar(10)   | Only FOREIGN |
|  4 | geocode4_corr                 | string        |              |
|  5 | iso_alpha                     | string        |              |
|  6 | hs6                           | string        |              |
|  7 | quantities                    | bigint        |              |
|  8 | quantities_lag                | bigint        |              |
|  9 | total_quantities_lag          | bigint        |              |
| 10 | lag_foreign_export_share_ckjr | decimal(21,5) |              |

    

## Table lag_foreign_export_ckr

- Owner: 468786073381 
- Database: chinese_trade
- Filename: lag_foreign_export_ckr
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/ATHENA/MAIN/tables/e1337fa3-3766-4a2a-a0b0-755d7a570fd2/
- S3uri: `s3://chinese-data/ATHENA/MAIN/tables/e1337fa3-3766-4a2a-a0b0-755d7a570fd2/`


|    | Name                         | Type          | Comment   |
|---:|:-----------------------------|:--------------|:----------|
|  0 | year                         | string        |           |
|  1 | year_lag                     | string        |           |
|  2 | regime                       | varchar(12)   |           |
|  3 | foreign_ownership            | varchar(10)   |           |
|  4 | geocode4_corr                | string        |           |
|  5 | hs6                          | string        |           |
|  6 | quantities                   | bigint        |           |
|  7 | quantities_lag               | bigint        |           |
|  8 | total_quantities_lag         | bigint        |           |
|  9 | lag_foreign_export_share_ckr | decimal(21,5) |           |

    

## Table lag_soe_export_ckjr

- Owner: 468786073381 
- Database: chinese_trade
- Filename: lag_soe_export_ckjr
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/ATHENA/MAIN/tables/ed3ec47f-831a-4508-bb28-970ceebe84a2/
- S3uri: `s3://chinese-data/ATHENA/MAIN/tables/ed3ec47f-831a-4508-bb28-970ceebe84a2/`


|    | Name                      | Type          | Comment   |
|---:|:--------------------------|:--------------|:----------|
|  0 | year                      | string        |           |
|  1 | year_lag                  | string        |           |
|  2 | regime                    | varchar(12)   |           |
|  3 | soe_ownership             | varchar(6)    |           |
|  4 | geocode4_corr             | string        |           |
|  5 | iso_alpha                 | string        |           |
|  6 | hs6                       | string        |           |
|  7 | quantities                | bigint        |           |
|  8 | quantities_lag            | bigint        |           |
|  9 | total_quantities_lag      | bigint        |           |
| 10 | lag_soe_export_share_ckjr | decimal(21,5) |           |

    

## Table lag_soe_export_ckr

- Owner: 468786073381 
- Database: chinese_trade
- Filename: lag_soe_export_ckr
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/ATHENA/MAIN/tables/96119386-12f3-465c-ab9d-134e8bfb0f64/
- S3uri: `s3://chinese-data/ATHENA/MAIN/tables/96119386-12f3-465c-ab9d-134e8bfb0f64/`


|    | Name                     | Type          | Comment   |
|---:|:-------------------------|:--------------|:----------|
|  0 | year                     | string        |           |
|  1 | year_lag                 | string        |           |
|  2 | regime                   | varchar(12)   |           |
|  3 | soe_ownership            | varchar(6)    |           |
|  4 | geocode4_corr            | string        |           |
|  5 | hs6                      | string        |           |
|  6 | quantities               | bigint        |           |
|  7 | quantities_lag           | bigint        |           |
|  8 | total_quantities_lag     | bigint        |           |
|  9 | lag_soe_export_share_ckr | decimal(21,5) |           |

    

## Table quality_vat_export_covariate_2003_2010

- Owner: 468786073381 
- Database: chinese_trade
- Filename: quality_vat_export_covariate_2003_2010
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/ATHENA/MAIN/tables/04661edc-a7bc-4da8-a412-1a67d7073904/
- S3uri: `s3://chinese-data/ATHENA/MAIN/tables/04661edc-a7bc-4da8-a412-1a67d7073904/`


|    | Name                          | Type          | Comment   |
|---:|:------------------------------|:--------------|:----------|
|  0 | cityen                        | string        |           |
|  1 | geocode4_corr                 | string        |           |
|  2 | year                          | string        |           |
|  3 | regime                        | string        |           |
|  4 | hs6                           | string        |           |
|  5 | hs4                           | string        |           |
|  6 | hs3                           | string        |           |
|  7 | country_en                    | string        |           |
|  8 | iso_alpha                     | string        |           |
|  9 | gni_per_capita                | float         |           |
| 10 | gpd_per_capita                | float         |           |
| 11 | income_group                  | string        |           |
| 12 | quantity                      | int           |           |
| 13 | value                         | int           |           |
| 14 | unit_price                    | float         |           |
| 15 | kandhelwal_quality            | float         |           |
| 16 | price_adjusted_quality        | float         |           |
| 17 | lag_tax_rebate                | float         |           |
| 18 | ln_lag_tax_rebate             | float         |           |
| 19 | lag_vat_reb_m                 | float         |           |
| 20 | ln_lag_vat_reb_m              | float         |           |
| 21 | lag_import_tax                | float         |           |
| 22 | ln_lag_import_tax             | float         |           |
| 23 | lag_soe_export_share_ckr      | decimal(21,5) |           |
| 24 | lag_foreign_export_share_ckr  | decimal(21,5) |           |
| 25 | lag_soe_export_share_ckjr     | decimal(21,5) |           |
| 26 | lag_foreign_export_share_ckjr | decimal(21,5) |           |
| 27 | sigma                         | float         |           |
| 28 | sigma_price                   | float         |           |
| 29 | y                             | float         |           |
| 30 | prediction                    | float         |           |
| 31 | residual                      | float         |           |
| 32 | fe_ck                         | string        |           |
| 33 | fe_cst                        | string        |           |
| 34 | fe_ckr                        | string        |           |
| 35 | fe_csrt                       | string        |           |
| 36 | fe_kt                         | string        |           |
| 37 | fe_kj                         | string        |           |
| 38 | fe_jt                         | string        |           |
| 39 | fe_ckj                        | string        |           |

    