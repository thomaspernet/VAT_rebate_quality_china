# CHINA VAT QUALITY

Merge quality city-product, ownership lag export value and country gdp per capita

* **[china_vat_quality](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/04_baseline_vat_quantity_covariates.md)**: 
Merge quality city-product, ownership lag export value and country gdp per capita

    * TRANSFORMATION
        * [china_export_tariff_tax](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/00_export_vat.md): 
Filter trade data, aggregate quantity and values and construct unit price

            * CREATION
                * [china_import_export](https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/TRADE_CHINA/import_export.py): Create import export china
            * CREATION
                * [city_cn_en](None): None
            * CREATION
                * [china_country_name](https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/COUNTRY_NAME/chinese_country_name.py): Download chinese country name (initialy in big query)
            * CREATION
                * [china_applied_mfn_tariffs_hs2](https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/APPLIED_MFN_TARIFFS/mnf_tariff.py): Download trade tariff china
            * CREATION
                * [hs6_china_vat_rebate](https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/VAT_REBATE/vat_rebate.py): Download vat rebate
    * TRANSFORMATION
        * [china_product_quality](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/01_preparation_quality.md): 
Compute Kandhelwal quality. The quality is computed at the city product-destination-level for each year in our sample

            * CREATION
                * [](None): None
            * CREATION
                * [](None): None
    * TRANSFORMATION
        * [export_foreign_city_product](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/02_ownership_export_share_ckr.md): 
Create lag foreign export quantity at the city, product level

            * CREATION
                * [china_import_export](https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/TRADE_CHINA/import_export.py): Create import export china
            * CREATION
                * [city_cn_en](None): None
    * TRANSFORMATION
        * [export_soe_city_product](https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/03_ownership_soe_export_share_ckr.md): 
Create lag soe export quantity at the city, product level

            * CREATION
                * [china_import_export](https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/TRADE_CHINA/import_export.py): Create import export china
            * CREATION
                * [city_cn_en](None): None
    * CREATION
        * [world_bank_gdp_per_capita](https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/WORLD_BANK/gdp_per_capita.py): Download GDP per capita from the World Bank Website
        * [hs6_homogeneous](https://github.com/thomaspernet/VAT_rebate_quality_china/01_data_preprocessing/00_download_data/HOMOGENEOUS_GOODS/hs_homogeneous_goods.py): Download homogeneous goods from R library concordance

### ETL diagrams



![](https://raw.githubusercontent.com/thomaspernet/VAT_rebate_quality_china/master/utils/IMAGES/china_vat_quality.jpg)

