
# VAT rebate quality china



This paper studies the effect of industrial policy in China, the VAT export tax, on the quality upgrading. We use Chinese transaction data for 2002-2006 to isolate the causal impact of the exogenous variation of VAT refund tax and within firm-product change in HS6 exported quality products.

## Table of Content

 - **00_data_catalog/**
   - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog/README.md)
   - **HTML_ANALYSIS/**
     - [CHINA_VAT_QUALITY.html](https://htmlpreview.github.io/?https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/00_data_catalog/HTML_ANALYSIS/CHINA_VAT_QUALITY.html)
     - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog/HTML_ANALYSIS/README.md)
   - **temporary_local_data/**
     - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/00_data_catalog/temporary_local_data/README.md)
 - **01_data_preprocessing/**
   - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/README.md)
   - **00_download_data/**
     - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/README.md)
     - **ADDITIONAL_VARIABLES/**
       - [energy.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/ADDITIONAL_VARIABLES/energy.py)
       - [high_tech.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/ADDITIONAL_VARIABLES/high_tech.py)
       - [rd_oriented.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/ADDITIONAL_VARIABLES/rd_oriented.py)
       - [skilled_oriented.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/ADDITIONAL_VARIABLES/skilled_oriented.py)
     - **APPLIED_MFN_TARIFFS/**
       - [mnf_tariff.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/APPLIED_MFN_TARIFFS/mnf_tariff.py)
     - **BACI/**
       - [baci_92_2002.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/BACI/baci_92_2002.py)
     - **COUNTRY_NAME/**
       - [chinese_country_name.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/COUNTRY_NAME/chinese_country_name.py)
     - **DENSITY/**
       - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/DENSITY/README.md)
       - [density.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/DENSITY/density.py)
     - **HOMOGENEOUS_GOODS/**
       - [hs_homogeneous_goods.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/HOMOGENEOUS_GOODS/hs_homogeneous_goods.py)
     - **SIGMA/**
       - [sigma.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/SIGMA/sigma.py)
     - **TRADE_CHINA/**
       - [import_export.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/TRADE_CHINA/import_export.py)
     - **VAT_REBATE/**
       - [vat_rebate.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/VAT_REBATE/vat_rebate.py)
     - **WORLD_BANK/**
       - [gdp_per_capita.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/00_download_data/WORLD_BANK/gdp_per_capita.py)
   - **02_transform_tables/**
     - [00_export_vat.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_transform_tables/00_export_vat.md)
     - [01_preparation_quality.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_transform_tables/01_preparation_quality.md)
     - [02_ownership_export_share_ckr.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_transform_tables/02_ownership_export_share_ckr.md)
     - [03_ownership_soe_export_share_ckr.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_transform_tables/03_ownership_soe_export_share_ckr.md)
     - [04_baseline_vat_quantity_covariates.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_transform_tables/04_baseline_vat_quantity_covariates.md)
     - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_transform_tables/README.md)
     - **Reports/**
       - [00_export_vat.html](https://htmlpreview.github.io/?https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/Reports/00_export_vat.html)
       - [01_preparation_quality.html](https://htmlpreview.github.io/?https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/Reports/01_preparation_quality.html)
       - [02_ownership_export_share_ckr.html](https://htmlpreview.github.io/?https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/Reports/02_ownership_export_share_ckr.html)
       - [03_ownership_soe_export_share_ckr.html](https://htmlpreview.github.io/?https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/Reports/03_ownership_soe_export_share_ckr.html)
       - [04_baseline_vat_quantity_covariates.html](https://htmlpreview.github.io/?https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/01_data_preprocessing/02_transform_tables/Reports/04_baseline_vat_quantity_covariates.html)
       - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/01_data_preprocessing/02_transform_tables/Reports/README.md)
   - **01_model_estimation/**
     - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/01_model_estimation/README.md)
     - **00_replicate_previous/**
       - [00_previous_tables.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/01_model_estimation/00_replicate_previous/00_previous_tables.md)
       - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/01_model_estimation/00_replicate_previous/README.md)
       - **Reports/**
         - [00_previous_tables.html](https://htmlpreview.github.io/?https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/02_data_analysis/01_model_estimation/00_replicate_previous/Reports/00_previous_tables.html)
     - **01_baseline_tables/**
       - [00_baseline_vat_quality.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/01_model_estimation/01_baseline_tables/00_baseline_vat_quality.md)
       - **Reports/**
         - [00_baseline_vat_quality.html](https://htmlpreview.github.io/?https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/02_data_analysis/01_model_estimation/01_baseline_tables/Reports/00_baseline_vat_quality.html)
   - **02_statistical_tables/**
     - [00_final_publishable_tables_figures.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/02_statistical_tables/00_final_publishable_tables_figures.md)
     - [README.md](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/02_data_analysis/02_statistical_tables/README.md)
     - **Reports/**
       - [00_final_publishable_tables_figures.html](https://htmlpreview.github.io/?https://github.com/thomaspernet/VAT_rebate_quality_china/blob/master/02_data_analysis/02_statistical_tables/Reports/00_final_publishable_tables_figures.html)
 - **utils/**
   - [create_report.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/utils/create_report.py)
   - [create_schema.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/utils/create_schema.py)
   - [make_toc.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/utils/make_toc.py)
   - [prepare_catalog.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/utils/prepare_catalog.py)
   - **IMAGES/**
     - [script_diagram_china_vat_quality.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/utils/IMAGES/script_diagram_china_vat_quality.py)
   - **latex/**
     - [latex_beautify.py](https://github.com/thomaspernet/VAT_rebate_quality_china/tree/master/utils/latex/latex_beautify.py)
