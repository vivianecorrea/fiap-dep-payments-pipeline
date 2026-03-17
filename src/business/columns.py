class OrderInputColumns:
    ID = "ID_PEDIDO"
    PRODUCT = "PRODUTO"
    UNIT_PRICE = "VALOR_UNITARIO"
    QUANTITY = "QUANTIDADE"
    CREATED_AT = "DATA_CRIACAO"
    STATE = "UF"
    CUSTOMER_ID = "ID_CLIENTE"


class PaymentInputColumns:
    FRAUD_EVAL = "avaliacao_fraude"
    FRAUD_FLAG = "avaliacao_fraude.fraude"
    PROCESSING_DATE = "data_processamento"
    PAYMENT_METHOD = "forma_pagamento"
    ORDER_ID = "id_pedido"
    STATUS = "status"
    PAYMENT_VALUE = "valor_pagamento"


class ReportColumns:
    ORDER_ID = "order_id"
    STATE = "state"
    PAYMENT_METHOD = "payment_method"
    ORDER_TOTAL = "order_total"
    ORDER_DATE = "order_date"
    PAYMENT_STATUS = "payment_status"
    FRAUD_FLAG = "fraud_flag"
    PROCESSING_DATE = "processing_date"


class OutputColumns:
    ORDER_ID = "id_pedido"
    STATE = "UF"
    PAYMENT_METHOD = "forma_pagamento"
    ORDER_TOTAL = "valor_total"
    ORDER_DATE = "data_pedido"
