
# Data Catalog



## Table of Content

    
- [china_sigmas_hs3](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_sigmas_hs3)
- [world_bank_gdp_per_capita](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-world_bank_gdp_per_capita)
- [hs6_homogeneous](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-hs6_homogeneous)
- [china_density](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_density)
- [baci_export](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-baci_export)
- [hs6_china_vat_rebate](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-hs6_china_vat_rebate)
- [china_applied_mfn_tariffs_hs2](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_applied_mfn_tariffs_hs2)
- [china_import_export](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_import_export)
- [china_country_name](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog#table-china_country_name)

    

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
|  3 | HS3    | string | industry code |

    

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
|  3 | HS02       | string |           |

    

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

    