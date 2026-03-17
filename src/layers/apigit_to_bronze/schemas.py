from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    DecimalType
)


class Payments:
    AVALIACAO_FRAUDE = "avaliacao_fraude"
    FRAUDE = "fraude"
    SCORE = "score"
    DATA_PROCESSAMENTO = "data_processamento"
    FORMA_PAGAMENTO = "forma_pagamento"
    ID_PEDIDO = "id_pedido"
    STATUS = "status"
    VALOR_PAGAMENTO = "valor_pagamento"

    def schema(self) -> StructType:
        return StructType([
            StructField(
                self.AVALIACAO_FRAUDE,
                StructType([
                    StructField(self.FRAUDE, BooleanType(), True),
                    StructField(self.SCORE, StringType(), True),
                ]),
                True,
            ),
            StructField(self.DATA_PROCESSAMENTO, StringType(), True),
            StructField(self.FORMA_PAGAMENTO, StringType(), True),
            StructField(self.ID_PEDIDO, StringType(), True),
            StructField(self.STATUS, StringType(), True),
            StructField(self.VALOR_PAGAMENTO, DecimalType(10, 2), True),
        ])

class Orders:
    ID_PEDIDO = "ID_PEDIDO"
    PRODUTO = "PRODUTO"
    VALOR_UNITARIO = "VALOR_UNITARIO"
    QUANTIDADE = "QUANTIDADE"
    DATA_CRIACAO = "DATA_CRIACAO"
    UF = "UF"
    ID_CLIENTE = "ID_CLIENTE"

    def schema(self) -> StructType:
        return StructType([
            StructField(self.ID_PEDIDO, StringType(), True),
            StructField(self.PRODUTO, StringType(), True),
            StructField(self.VALOR_UNITARIO, DecimalType(10, 2), True),
            StructField(self.QUANTIDADE, StringType(), True),
            StructField(self.DATA_CRIACAO, StringType(), True),
            StructField(self.UF, StringType(), True),
            StructField(self.ID_CLIENTE, StringType(), True),
        ])