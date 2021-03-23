
# Data Catalogue



## Table of Content

    
- [world_bank_gdp_per_capita](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-world_bank_gdp_per_capita)
- [hs6_homogeneous](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-hs6_homogeneous)
- [china_density](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_density)
- [baci_export](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-baci_export)
- [hs6_china_vat_rebate](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-hs6_china_vat_rebate)
- [china_import_export](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_import_export)
- [china_country_name](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_country_name)
- [china_applied_mfn_tariffs_hs2](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_applied_mfn_tariffs_hs2)
- [china_sigmas_hs3](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_sigmas_hs3)
- [china_export_tariff_tax](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_export_tariff_tax)
- [china_product_quality](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_product_quality)
- [export_foreign_city_product](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-export_foreign_city_product)
- [export_soe_city_product](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-export_soe_city_product)

    

## Table world_bank_gdp_per_capita

- Database: world_bank
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/RAW/WORLD_BANK/GDP_PER_CAPITA`
- Partitition: []
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/WORLD_BANK/gdp_per_capita.py

|    | Name           | Type   | Comment                                                                                                                                                               |
|---:|:---------------|:-------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | country        | string | Country name                                                                                                                                                          |
|  1 | iso_alpha      | string | Country code                                                                                                                                                          |
|  2 | iso_alpha03    | string | Country code, iso 03                                                                                                                                                  |
|  3 | year           | string | Year                                                                                                                                                                  |
|  4 | gni_per_capita | float  | GDP per capita is gross domestic product divided by midyear population                                                                                                |
|  5 | gpd_per_capita | float  | GNI per capita (formerly GNP per capita) is the gross national income, converted to U.S. dollars using the World Bank Atlas method, divided by the midyear population |
|  6 | income_group   | string | One of 'Others', 'Low income', 'Upper middle income','High income: nonOECD', 'Lower middle income', 'High income: OECD'                                               |

    

## Table hs6_homogeneous

- Database: industry
- S3uri: `s3://datalake-datascience/TRADE_DATA/RAW/GOODS_CLASSIFICATION/HOMOGENEOUS`
- Partitition: []
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/HOMOGENEOUS_GOODS/hs_homogeneous_goods.py

|    | Name        | Type   | Comment                                |
|---:|:------------|:-------|:---------------------------------------|
|  0 | hs6         | string | HS6 6 digits                           |
|  1 | goods       | string | 0 is homoegeous and 1 is heterogeneous |
|  2 | homogeneous | string | Goods label                            |

    

## Table china_density

- Database: chinese_trade
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/DENSITY`
- Partitition: []
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/DENSITY/density.py

|    | Name                | Type   | Comment                    |
|---:|:--------------------|:-------|:---------------------------|
|  0 | hs6                 | string | HS6 6 digits               |
|  1 | density_china_ville | float  | Density china city-product |
|  2 | geocode4_corr       | string | Chinese city code          |

    

## Table baci_export

- Database: trade
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/BACI/EXPORT`
- Partitition: []
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/BACI/baci_92_2002.py

|    | Name   | Type   | Comment                |
|---:|:-------|:-------|:-----------------------|
|  0 | t      | string | year                   |
|  1 | hs6    | string | HS6 6 digits           |
|  2 | i      | string | origin country ID      |
|  3 | j      | string | Destination country ID |
|  4 | v      | float  | Export value           |
|  5 | q      | float  | Export quantity        |

    

## Table hs6_china_vat_rebate

- Database: chinese_trade
- S3uri: `s3://datalake-datascience/DATA/ECON/TAX_DATA/CHINA_VAT_REBATE`
- Partitition: []
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/VAT_REBATE/vat_rebate.py

|    | Name       | Type   | Comment                                |
|---:|:-----------|:-------|:---------------------------------------|
|  0 | hs6        | string | 6 digits product line                  |
|  1 | year       | string |                                        |
|  2 | vat_m      | float  | 6 digits product line tax              |
|  3 | vat_reb_m  | float  | Percentage of tax refunded             |
|  4 | tax_rebate | float  | Effective tax level: vat_m - vat_reb_m |

    

## Table china_import_export

- Database: chinese_trade
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/IMPORT_EXPORT`
- Partitition: []
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/TRADE_CHINA/import_export.py

|    | Name                  | Type   | Comment                                                                                                           |
|---:|:----------------------|:-------|:------------------------------------------------------------------------------------------------------------------|
|  0 | date                  | string | year trade recorded                                                                                               |
|  1 | ID                    | string | firm ID                                                                                                           |
|  2 | business_type         | string | business trade type                                                                                               |
|  3 | intermediate          | string | company name includes the following 进出口, 贸易, 外贸,外经, 经贸, 工贸, 科贸, then there are intermediates firms |
|  4 | trade_type            | string | Trade type including ordianry or processing                                                                       |
|  5 | province              | string | Province name                                                                                                     |
|  6 | city_prod             | string | production site city name                                                                                         |
|  7 | matching_city         | string | matching name                                                                                                     |
|  8 | imp_exp               | string | import or export                                                                                                  |
|  9 | hs                    | string | HS6 6 digit                                                                                                       |
| 10 | origin_or_destination | string | source or destination country                                                                                     |
| 11 | value                 | int    | import or export value                                                                                            |
| 12 | quantities            | int    | import or export quantity                                                                                         |

    

## Table china_country_name

- Database: chinese_lookup
- S3uri: `s3://datalake-datascience/DATA/ECON/LOOKUP_DATA/COUNTRY_NAME`
- Partitition: []
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/COUNTRY_NAME/chinese_country_name.py

|    | Name       | Type   | Comment              |
|---:|:-----------|:-------|:---------------------|
|  0 | country_cn | string | Chinese country name |
|  1 | country_en | string | English country name |
|  2 | iso_alpha  | string | country ID 1         |
|  3 | code       | string | country ID 2         |
|  4 | index      | int    | index                |

    

## Table china_applied_mfn_tariffs_hs2

- Database: chinese_trade
- S3uri: `s3://datalake-datascience/DATA/ECON/TAX_DATA/CHINA/APPLIED_MFN_TARIFFS_HS2`
- Partitition: []
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/APPLIED_MFN_TARIFFS/mnf_tariff.py

|    | Name       | Type   | Comment   |
|---:|:-----------|:-------|:----------|
|  0 | reporter   | string |           |
|  1 | year       | string |           |
|  2 | import_tax | float  |           |
|  3 | hs6        | string |           |

    

## Table china_sigmas_hs3

- Database: chinese_trade
- S3uri: `s3://datalake-datascience/DATA/ECON/INDUSTRY/ADDITIONAL_DATA/SIGMAS_HS3`
- Partitition: []
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/SIGMA/sigma.py

|    | Name   | Type   | Comment       |
|---:|:-------|:-------|:--------------|
|  0 | ccode  | string | Country code  |
|  1 | cname  | string | countr name   |
|  2 | sigma  | float  | sigma         |
|  3 | hs3    | string | industry code |

    

## Table china_export_tariff_tax

- Database: chinese_trade
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/EXPORT_TARIFF_TAX`
- Partitition: ['geocode4_corr', 'year', 'regime', 'hs6', 'country_en']
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/00_export_vat.md

|    | Name              | Type          | Comment                               |
|---:|:------------------|:--------------|:--------------------------------------|
|  0 | cityen            | string        | English city name                     |
|  1 | geocode4_corr     | string        | Chinese city code                     |
|  2 | year              | string        | year                                  |
|  3 | regime            | varchar(12)   | eligible or no eligible to vat rebate |
|  4 | hs6               | string        | HS6 6 digit                           |
|  5 | country_en        | string        | English country name                  |
|  6 | iso_alpha         | string        |                                       |
|  7 | quantity          | bigint        | Export quantity                       |
|  8 | value             | bigint        | Export value                          |
|  9 | unit_price        | decimal(21,5) | product unit price                    |
| 10 | lag_vat_m         | float         | lag vat tax                           |
| 11 | ln_lag_vat_m      | double        | log lag vat tax                       |
| 12 | lag_vat_reb_m     | float         | lag vat rebate                        |
| 13 | ln_lag_vat_reb_m  | double        | log lag vat rebate                    |
| 14 | lag_tax_rebate    | float         | lag vat rebate to pay                 |
| 15 | ln_lag_tax_rebate | double        | log lag vat rebate to pay             |
| 16 | lag_import_tax    | float         | lag import tax                        |
| 17 | ln_lag_import_tax | double        | log lag import tax                    |
| 18 | cnt               | bigint        | remove duplicate                      |

    

## Table china_product_quality

- Database: chinese_trade
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/PRODUCT_QUALITY`
- Partitition: ['geocode4_corr', 'year', 'regime', 'hs6', 'country_en']
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/01_preparation_quality.md

|    | Name                   | Type   | Comment                |
|---:|:-----------------------|:-------|:-----------------------|
|  0 | ccode                  | string | country code           |
|  1 | cname                  | string | country name           |
|  2 | country_en             | string | country name english   |
|  3 | iso_alpha              | string |                        |
|  4 | geocode4_corr          | string | city code              |
|  5 | year                   | string | year                   |
|  6 | regime                 | string | eligible to vat rebate |
|  7 | hs6                    | string | HS6 6 digits           |
|  8 | hs3                    | string | HS3 3 digits           |
|  9 | hs4                    | string | HS4 4 digits           |
| 10 | quantity               | float  | Export quantity        |
| 11 | value                  | float  | Export value           |
| 12 | unit_price             | float  | Export unit price      |
| 13 | sigma                  | float  | sigma                  |
| 14 | sigma_price            | float  |                        |
| 15 | y                      | float  |                        |
| 16 | FE_ct                  | int    | city year fixed effect |
| 17 | prediction             | float  | prediction OLS         |
| 18 | residual               | float  | residual OLS           |
| 19 | price_adjusted_quality | float  | price adjusted         |
| 20 | kandhelwal_quality     | float  | Kandhelwal quality     |

    

## Table export_foreign_city_product

- Database: chinese_trade
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/EXPORT_SHARE/FOREIGN_CITY_PRODUCT`
- Partitition: ['year_lag', 'regime', 'geocode4_corr', 'hs6']
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/02_ownership_export_share_ckr.md

|    | Name                         | Type          | Comment                                    |
|---:|:-----------------------------|:--------------|:-------------------------------------------|
|  0 | year                         | string        | year                                       |
|  1 | year_lag                     | string        | previous year                              |
|  2 | regime                       | varchar(12)   | eligible or not eligible to vat refund     |
|  3 | foreign_ownership            | varchar(10)   | foreign or not foreign ownership           |
|  4 | geocode4_corr                | string        | city id                                    |
|  5 | hs6                          | string        | HS6 6 digits                               |
|  6 | quantities                   | bigint        | export quantity by ownership               |
|  7 | quantities_lag               | bigint        | lag export quantity by ownership           |
|  8 | total_quantities_lag         | bigint        | total export lag quantity by city industry |
|  9 | lag_foreign_export_share_ckr | decimal(21,5) | lag export share city industry regime      |

    

## Table export_soe_city_product

- Database: chinese_trade
- S3uri: `s3://datalake-datascience/DATA/ECON/TRADE_DATA/CHINA/EXPORT_SHARE/SOE_CITY_PRODUCT`
- Partitition: ['year_lag', 'regime', 'geocode4_corr', 'hs6']
- Script: https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/03_ownership_soe_export_share_ckr.md

|    | Name                     | Type          | Comment                                    |
|---:|:-------------------------|:--------------|:-------------------------------------------|
|  0 | year                     | string        | year                                       |
|  1 | year_lag                 | string        | previous year                              |
|  2 | regime                   | varchar(12)   | eligible or not eligible to vat refund     |
|  3 | soe_ownership            | varchar(10)   | foreign or not foreign ownership           |
|  4 | geocode4_corr            | string        | city id                                    |
|  5 | hs6                      | string        | HS6 6 digits                               |
|  6 | quantities               | bigint        | export quantity by ownership               |
|  7 | quantities_lag           | bigint        | lag export quantity by ownership           |
|  8 | total_quantities_lag     | bigint        | total export lag quantity by city industry |
|  9 | lag_soe_export_share_ckr | decimal(21,5) | lag export share city industry regime      |

    