
# Data Catalogue



## Table of Content

    
- [base_hs6_VAT_2002_2012](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-base_hs6_VAT_2002_2012)
- [applied_mfn_tariffs_hs02_china_2002_2010](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-applied_mfn_tariffs_hs02_china_2002_2010)
- [sigma_industry](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-sigma_industry)
- [country_cn_en](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-country_cn_en)
- [city_cn_en](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-city_cn_en)
- [import_export](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-import_export)
- [quality_vat_export_2003_2010](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-quality_vat_export_2003_2010)
- [quality_vat_export_2003_2010](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-quality_vat_export_2003_2010)
- [world_gdp_per_capita](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-world_gdp_per_capita)
- [VAT_export_2003_2010](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalogue#table-VAT_export_2003_2010)

    

## Table base_hs6_vat_2002_2012

- Owner: hadoop 
- Database: chinese_trade
- Filename: base_hs6_vat_2002_2012
- Location: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience/DATA/ECON/TAX_DATA/TRANSFORMED/CHINA_VAT_REBATE
- S3uri: `s3://datalake-datascience/DATA/ECON/TAX_DATA/TRANSFORMED/CHINA_VAT_REBATE`


|    | Name       | Type   | Comment                                |
|---:|:-----------|:-------|:---------------------------------------|
|  0 | hs6        | string | 6 digits product line                  |
|  1 | year       | string |                                        |
|  2 | vat_m      | float  | 6 digits product line tax              |
|  3 | vat_reb_m  | float  | Percentage of tax refunded             |
|  4 | tax_rebate | float  | Effective tax level: vat_m - vat_reb_m |

    

## Table applied_mfn_tariffs_hs02_china_2002_2010

- Owner: hadoop 
- Database: chinese_trade
- Filename: applied_mfn_tariffs_hs02_china_2002_2010
- Location: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience/DATA/ECON/TAX_DATA/TRANSFORMED/APPLIED_MFN_TARIFFS_HS2
- S3uri: `s3://datalake-datascience/DATA/ECON/TAX_DATA/TRANSFORMED/APPLIED_MFN_TARIFFS_HS2`


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
- Location: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience/DATA/ECON/INDUSTRY/ADDITIONAL_DATA/SIGMAS_HS3
- S3uri: `s3://datalake-datascience/DATA/ECON/INDUSTRY/ADDITIONAL_DATA/SIGMAS_HS3`


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
- Location: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience/DATA/ECON/LOOKUP_DATA/COUNTRY_NAME_CHINESE
- S3uri: `s3://datalake-datascience/DATA/ECON/LOOKUP_DATA/COUNTRY_NAME_CHINESE`


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
- Location: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience/DATA/ECON/LOOKUP_DATA/CITY_NAME_CHINESE
- S3uri: `s3://datalake-datascience/DATA/ECON/LOOKUP_DATA/CITY_NAME_CHINESE`


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
- Location: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/TRANSFORMED/IMPORT_EXPORT
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/TRANSFORMED/IMPORT_EXPORT`


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
- Location: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/TRANSFORMED/QUALITY_EXPORT_TARIFF_TAX
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/TRANSFORMED/QUALITY_EXPORT_TARIFF_TAX`


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
| 12 | price_adjusted_quality | float  | price adjusted quality. https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/01_preparation_quality.md |
| 13 | kandhelwal_quality     | float  | Kandhelwal quality. cf https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/01_preparation_quality.md  |
| 14 | lag_vat_m              | float  | Lag Percentage of tax                                                                                                                                                |
| 15 | lag_vat_reb_m          | float  | Lag Percentage of tax refunded                                                                                                                                       |
| 16 | lag_tax_rebate         | float  | Lag effective tax rebate (vat_m-vat_reb_m). Larger values imply higher tax                                                                                           |
| 17 | ln_lag_vat_m           | float  |                                                                                                                                                                      |
| 18 | ln_lag_vat_reb_m       | float  |                                                                                                                                                                      |
| 19 | ln_lag_tax_rebate      | float  |                                                                                                                                                                      |
| 20 | lag_import_tax         | float  |                                                                                                                                                                      |
| 21 | ln_lag_import_tax      | float  |                                                                                                                                                                      |
| 22 | sigma                  | float  |                                                                                                                                                                      |
| 23 | sigma_price            | float  |                                                                                                                                                                      |
| 24 | y                      | float  | log quantity plus sigma                                                                                                                                              |
| 25 | prediction             | float  |                                                                                                                                                                      |
| 26 | residual               | float  |                                                                                                                                                                      |
| 27 | fe_ct                  | string | nan                                                                                                                                                                  |
| 28 | fe_ck                  | string | nan                                                                                                                                                                  |
| 29 | fe_cst                 | string | nan                                                                                                                                                                  |
| 30 | fe_ckr                 | string | nan                                                                                                                                                                  |
| 31 | fe_csrt                | string | nan                                                                                                                                                                  |
| 32 | fe_kt                  | string | nan                                                                                                                                                                  |
| 33 | fe_kj                  | string | nan                                                                                                                                                                  |
| 34 | fe_jt                  | string | nan                                                                                                                                                                  |
| 35 | fe_ckj                 | string | nan                                                                                                                                                                  |

    

## Table quality_vat_export_2003_2010

- Owner: hadoop 
- Database: chinese_trade
- Filename: quality_vat_export_2003_2010
- Location: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/TRANSFORMED/QUALITY_EXPORT_TARIFF_TAX
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/TRANSFORMED/QUALITY_EXPORT_TARIFF_TAX`


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
| 12 | price_adjusted_quality | float  | price adjusted quality. https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/01_preparation_quality.md |
| 13 | kandhelwal_quality     | float  | Kandhelwal quality. cf https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_prepare_tables_model/01_preparation_quality.md  |
| 14 | lag_vat_m              | float  | Lag Percentage of tax                                                                                                                                                |
| 15 | lag_vat_reb_m          | float  | Lag Percentage of tax refunded                                                                                                                                       |
| 16 | lag_tax_rebate         | float  | Lag effective tax rebate (vat_m-vat_reb_m). Larger values imply higher tax                                                                                           |
| 17 | ln_lag_vat_m           | float  |                                                                                                                                                                      |
| 18 | ln_lag_vat_reb_m       | float  |                                                                                                                                                                      |
| 19 | ln_lag_tax_rebate      | float  |                                                                                                                                                                      |
| 20 | lag_import_tax         | float  |                                                                                                                                                                      |
| 21 | ln_lag_import_tax      | float  |                                                                                                                                                                      |
| 22 | sigma                  | float  |                                                                                                                                                                      |
| 23 | sigma_price            | float  |                                                                                                                                                                      |
| 24 | y                      | float  | log quantity plus sigma                                                                                                                                              |
| 25 | prediction             | float  |                                                                                                                                                                      |
| 26 | residual               | float  |                                                                                                                                                                      |
| 27 | fe_ct                  | string | nan                                                                                                                                                                  |
| 28 | fe_ck                  | string | nan                                                                                                                                                                  |
| 29 | fe_cst                 | string | nan                                                                                                                                                                  |
| 30 | fe_ckr                 | string | nan                                                                                                                                                                  |
| 31 | fe_csrt                | string | nan                                                                                                                                                                  |
| 32 | fe_kt                  | string | nan                                                                                                                                                                  |
| 33 | fe_kj                  | string | nan                                                                                                                                                                  |
| 34 | fe_jt                  | string | nan                                                                                                                                                                  |
| 35 | fe_ckj                 | string | nan                                                                                                                                                                  |

    

## Table world_gdp_per_capita

- Owner: hadoop 
- Database: world_bank
- Filename: world_gdp_per_capita
- Location: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience/DATA/ECON/TRADE_DATA/RAW/WORLD_BANK/NY.GNP.PCAP.CD_NY.GDP.PCAP.KD
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/RAW/WORLD_BANK/NY.GNP.PCAP.CD_NY.GDP.PCAP.KD`


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
- Location: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/TRANSFORMED/EXPORT_TARIFF_TAX/tables/c8e9613e-3f72-4e70-aee0-1466ffb9dd1a
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/TRANSFORMED/EXPORT_TARIFF_TAX/tables/c8e9613e-3f72-4e70-aee0-1466ffb9dd1a`


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
| 10 | lag_vat_m         | float         |           |
| 11 | ln_lag_vat_m      | double        |           |
| 12 | lag_vat_reb_m     | float         |           |
| 13 | ln_lag_vat_reb_m  | double        |           |
| 14 | lag_tax_rebate    | float         |           |
| 15 | ln_lag_tax_rebate | double        |           |
| 16 | lag_import_tax    | float         |           |
| 17 | ln_lag_import_tax | double        |           |

    