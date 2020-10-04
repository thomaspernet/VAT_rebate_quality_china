
# Data Catalogue



## Table of Content

    
- [base_hs6_VAT_2002_2012](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-base_hs6_VAT_2002_2012)
- [applied_mfn_tariffs_hs02_china_2002_2010](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-applied_mfn_tariffs_hs02_china_2002_2010)
- [sigma_industry](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-sigma_industry)
- [country_cn_en](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-country_cn_en)
- [city_cn_en](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-city_cn_en)
- [import_export](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-import_export)
- [quality_vat_export_2003_2010](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-quality_vat_export_2003_2010)
- [world_gdp_per_capita](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-world_gdp_per_capita)
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


|    | Name          | Type   | Comment   |
|---:|:--------------|:-------|:----------|
|  0 | hs6           | string |           |
|  1 | year          | string |           |
|  2 | tax_rebate    | float  |           |
|  3 | ln_vat_rebate | float  |           |
|  4 | vat_m         | float  |           |
|  5 | vat_reb_m     | float  |           |

    

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
- Location: https://s3.console.aws.amazon.com/s3/buckets/vat-rebate-quality/DATA/TRANSFORMED
- S3uri: `s3://vat-rebate-quality/DATA/TRANSFORMED`


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
| 16 | lag_import_tax         | float  |                                                                                                                                                                      |
| 17 | ln_lag_import_tax      | float  |                                                                                                                                                                      |
| 18 | sigma                  | float  |                                                                                                                                                                      |
| 19 | sigma_price            | float  |                                                                                                                                                                      |
| 20 | y                      | float  | log quantity plus sigma                                                                                                                                              |
| 21 | prediction             | float  |                                                                                                                                                                      |
| 22 | residual               | float  |                                                                                                                                                                      |
| 23 | fe_ck                  | string | nan                                                                                                                                                                  |
| 24 | fe_cst                 | string | nan                                                                                                                                                                  |
| 25 | fe_ckr                 | string | nan                                                                                                                                                                  |
| 26 | fe_csrt                | string | nan                                                                                                                                                                  |
| 27 | fe_kt                  | string | nan                                                                                                                                                                  |
| 28 | fe_pj                  | string | nan                                                                                                                                                                  |
| 29 | fe_jt                  | string | nan                                                                                                                                                                  |
| 30 | fe_ct                  | string | nan                                                                                                                                                                  |

    

## Table world_gdp_per_capita

- Owner: hadoop 
- Database: world_bank
- Filename: world_gdp_per_capita
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/TRADE_DATA/RAW/WORLD_BANK/NY.GNP.PCAP.CD_NY.GDP.PCAP.KD
- S3uri: `s3://chinese-data/TRADE_DATA/RAW/WORLD_BANK/NY.GNP.PCAP.CD_NY.GDP.PCAP.KD`


|    | Name           | Type   | Comment                                                                                                                                                               |
|---:|:---------------|:-------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | country        | string | Country name                                                                                                                                                          |
|  1 | iso_alpha      | string | Country code                                                                                                                                                          |
|  2 | iso_alpha03    | string | Country code, iso 03                                                                                                                                                  |
|  3 | year           | string | Year                                                                                                                                                                  |
|  4 | gni_per_capita | float  | GDP per capita is gross domestic product divided by midyear population                                                                                                |
|  5 | gpd_per_capita | float  | GNI per capita (formerly GNP per capita) is the gross national income, converted to U.S. dollars using the World Bank Atlas method, divided by the midyear population |
|  6 | income_group   | string | One of 'Others', 'Low income', 'Upper middle income','High income: nonOECD', 'Lower middle income', 'High income: OECD'                                               |

    

## Table vat_export_2003_2010

- Owner: 468786073381 
- Database: chinese_trade
- Filename: vat_export_2003_2010
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/SQL_OUTPUT_ATHENA/tables/afd99920-592b-4910-b280-f84b68eb7d3f/
- S3uri: `s3://chinese-data/SQL_OUTPUT_ATHENA/tables/afd99920-592b-4910-b280-f84b68eb7d3f/`


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
| 12 | lag_import_tax    | float         |           |
| 13 | ln_lag_import_tax | double        |           |

    

## Table lag_foreign_export_ckjr

- Owner: 468786073381 
- Database: chinese_trade
- Filename: lag_foreign_export_ckjr
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/SQL_OUTPUT_ATHENA/tables/12175656-527f-41f2-a378-978b2000bcb8/
- S3uri: `s3://chinese-data/SQL_OUTPUT_ATHENA/tables/12175656-527f-41f2-a378-978b2000bcb8/`


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
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/SQL_OUTPUT_ATHENA/tables/a12f488d-78df-4a70-9f94-a9f10c153698/
- S3uri: `s3://chinese-data/SQL_OUTPUT_ATHENA/tables/a12f488d-78df-4a70-9f94-a9f10c153698/`


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
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/SQL_OUTPUT_ATHENA/tables/6c8101c9-2713-4843-8fcc-7e093240714d/
- S3uri: `s3://chinese-data/SQL_OUTPUT_ATHENA/tables/6c8101c9-2713-4843-8fcc-7e093240714d/`


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
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/SQL_OUTPUT_ATHENA/tables/b354a1ec-6c1e-4e95-838e-67b4cba3ac63/
- S3uri: `s3://chinese-data/SQL_OUTPUT_ATHENA/tables/b354a1ec-6c1e-4e95-838e-67b4cba3ac63/`


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
- Location: https://s3.console.aws.amazon.com/s3/buckets/chinese-data/SQL_OUTPUT_ATHENA/tables/f6d8ca3a-4726-4d65-b6b1-09afe033041d/
- S3uri: `s3://chinese-data/SQL_OUTPUT_ATHENA/tables/f6d8ca3a-4726-4d65-b6b1-09afe033041d/`


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
| 19 | lag_import_tax                | float         |           |
| 20 | ln_lag_import_tax             | float         |           |
| 21 | lag_soe_export_share_ckr      | decimal(21,5) |           |
| 22 | lag_foreign_export_share_ckr  | decimal(21,5) |           |
| 23 | lag_soe_export_share_ckjr     | decimal(21,5) |           |
| 24 | lag_foreign_export_share_ckjr | decimal(21,5) |           |
| 25 | sigma                         | float         |           |
| 26 | sigma_price                   | float         |           |
| 27 | y                             | float         |           |
| 28 | prediction                    | float         |           |
| 29 | residual                      | float         |           |
| 30 | fe_ck                         | string        |           |
| 31 | fe_cst                        | string        |           |
| 32 | fe_ckr                        | string        |           |
| 33 | fe_csrt                       | string        |           |
| 34 | fe_kt                         | string        |           |
| 35 | fe_pj                         | string        |           |
| 36 | fe_jt                         | string        |           |
| 37 | fe_ct                         | string        |           |

    