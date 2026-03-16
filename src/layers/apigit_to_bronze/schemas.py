from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

class Schemas:
    """Definição explícita de schemas para cumprir o Critério 1 do trabalho."""
    
    # Dataset de Pedidos (CSV)
    PEDIDOS = StructType([
        StructField("id_pedido", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("valor_total", DoubleType(), True),
        StructField("data_pedido", TimestampType(), True)
    ])

    # Dataset de Pagamentos (JSON)
    PAGAMENTOS = StructType([
        StructField("id_pedido", StringType(), True),
        StructField("pagamento_recusado", BooleanType(), True),
        StructField("avaliacao_fraude", BooleanType(), True)
    ])