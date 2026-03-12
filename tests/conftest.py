import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DecimalType, BooleanType, StructField, StructType

@pytest.fixture(scope="session", autouse=True)
def spark_session():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pyspark-testing")
        .getOrCreate()
    )
    
    yield spark
    
    spark.stop()
    

    
    
@pytest.fixture    
def payments_schema():
    return StructType([
    StructField(
        "avaliacao_fraude",
        StructType([
            StructField("fraude", StringType(), True),
            StructField("score", StringType(), True),
        ]),
        True
    ),
    StructField("data_processamento", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("id_pedido", StringType(), True),
    StructField("status", StringType(), True),
    StructField("valor_pagamento", StringType(), True),
])
    
    
    
@pytest.fixture
def orders_schema(): 
    return StructType([
    StructField("ID_PEDIDO", StringType(), True),
    StructField("PRODUTO", StringType(), True),
    StructField("VALOR_UNITARIO", StringType(), True),
    StructField("QUANTIDADE", StringType(), True),
    StructField("DATA_CRIACAO", StringType(), True),
    StructField("UF", StringType(), True),
    StructField("ID_CLIENTE", StringType(), True),
])