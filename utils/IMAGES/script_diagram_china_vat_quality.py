
from diagrams import Cluster, Diagram
from diagrams.aws.compute import ECS
from diagrams.aws.database import Redshift, RDS
from diagrams.aws.integration import SQS
from diagrams.aws.storage import S3

with Diagram("CHINA VAT QUALITY", show=False, filename="/Users/thomas/Google Drive/PROJECT/GITHUB/REPOSITORIES/VAT_rebate_quality_china/utils/IMAGES/china_vat_quality", outformat="jpg"):

     temp_1 = S3('world_bank_gdp_per_capita')
     temp_2 = SQS('china_export_tariff_tax')
     temp_3 = SQS('china_product_quality')
     temp_5 = SQS('export_foreign_city_product')
     temp_6 = SQS('export_soe_city_product')

     with Cluster("FINAL"):

         temp_final_0 = Redshift('china_vat_quality')


     temp_final_0 << temp_1
     temp_2 >> temp_final_0
     temp_2 >>temp_3 >> temp_final_0
     temp_2 >> temp_final_0
     temp_5 >> temp_final_0
     temp_6 >> temp_final_0
