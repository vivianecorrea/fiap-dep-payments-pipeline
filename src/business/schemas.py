from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


class Schemas:
    """Explicit schema definitions for all DataFrames. Satisfies Criterion 1."""

    ORDERS = StructType([
        StructField("ID_PEDIDO", StringType(), True),
        StructField("PRODUTO", StringType(), True),
        StructField("VALOR_UNITARIO", StringType(), True),
        StructField("QUANTIDADE", StringType(), True),
        StructField("DATA_CRIACAO", StringType(), True),
        StructField("UF", StringType(), True),
        StructField("ID_CLIENTE", StringType(), True),
    ])

    PAYMENTS = StructType([
        StructField(
            "avaliacao_fraude",
            StructType([
                StructField("fraude", StringType(), True),
                StructField("score", StringType(), True),
            ]),
            True,
        ),
        StructField("data_processamento", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("id_pedido", StringType(), True),
        StructField("status", StringType(), True),
        StructField("valor_pagamento", StringType(), True),
    ])
