from src.layers.apigit_to_bronze.task import ApiGitToBronze
from src.layers.apigit_to_bronze.schemas import Payments, Orders
from src.spark.spark_session_manager import SparkSessionManager

spark= SparkSessionManager()
spark_session = spark.create_session()



payments = Payments()

job_payments_to_bronze = ApiGitToBronze(
    input_path='/fiap-dep-payments-pipeline/datasets/payments', 
    schema=payments.schema(),
    spark = spark.get_session()
    )
    
orders = Orders()
    
job_orders_to_bronze = ApiGitToBronze(
    input_path='/fiap-dep-payments-pipeline/datasets/orders', 
    schema=orders.schema(),
    spark = spark.get_session()
    )