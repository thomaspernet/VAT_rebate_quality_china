
from diagrams import Cluster, Diagram
from diagrams.aws.compute import ECS
from diagrams.aws.database import Redshift, RDS
from diagrams.aws.integration import SQS
from diagrams.aws.storage import S3

with Diagram("CHINA VAT QUALITY", show=False, filename="/home/ec2-user/VAT_rebate_quality_china/utils/IMAGES/china_vat_quality", outformat="jpg"):

     temp_1 = S3('world_bank_gdp_per_capita')
     temp_2 = S3('hs6_homogeneous')
     input_china_import_export = S3("china_import_export")
     input_city_cn_en = S3("city_cn_en")
     input_china_country_name = S3("china_country_name")
     input_china_applied_mfn_tariffs_hs2 = S3("china_applied_mfn_tariffs_hs2")
     input_hs6_china_vat_rebate = S3("hs6_china_vat_rebate")
     input_ = S3("")
     temp_3 = SQS('china_export_tariff_tax')
     temp_4 = SQS('china_product_quality')
     temp_5 = SQS('export_foreign_city_product')
     temp_6 = SQS('export_soe_city_product')

     with Cluster("FINAL"):

         temp_final_0 = Redshift('china_vat_quality')


     temp_final_0 << temp_1
     temp_final_0 << temp_2
     input_china_import_export >> temp_3
     input_city_cn_en >> temp_3
     input_china_country_name >> temp_3
     input_china_applied_mfn_tariffs_hs2 >> temp_3
     input_hs6_china_vat_rebate >> temp_3 >> temp_final_0
     input_ >> temp_4
     input_china_import_export >> temp_5
     input_city_cn_en >> temp_5 >> temp_final_0
     input_china_import_export >> temp_6
     input_city_cn_en >> temp_6 >> temp_final_0
