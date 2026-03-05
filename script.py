import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType,
    TimestampType, DoubleType
)

ORDERS_PATH = "./datasets-csv-pedidos/data/pedidos/"
PAYMENTS_PATH = "./dataset-json-pagamentos/data/pagamentos/"
OUTPUT_PATH = "./output/sales_report_2025"

orders_schema = StructType([
    StructField("ID_PEDIDO", StringType(), True),
    StructField("PRODUTO", StringType(), True),
    StructField("VALOR_UNITARIO", StringType(), True),
    StructField("QUANTIDADE", StringType(), True),
    StructField("DATA_CRIACAO", StringType(), True),
    StructField("UF", StringType(), True),
    StructField("ID_CLIENTE", StringType(), True),
])

payments_schema = StructType([
    StructField(
        "avaliacao_fraude",
        StructType([
            StructField("fraude", BooleanType(), True),
            StructField("score", DoubleType(), True),
        ]),
        True
    ),
    StructField("data_processamento", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("id_pedido", StringType(), True),
    StructField("status", BooleanType(), True),
    StructField("valor_pagamento", DoubleType(), True),
])

def main():
    spark = (
        SparkSession.builder
        .appName("dataeng-spark-sql-mvp")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df_orders = (
        spark.read
        .format("csv")
        .option("compression", "gzip")
        .option("sep", ";")
        .option("header", True)
        .schema(orders_schema)
        .load(ORDERS_PATH)
    )

    df_payments = (
        spark.read
        .format("json")
        .option("compression", "gzip")
        .schema(payments_schema)
        .load(PAYMENTS_PATH)
    )

    df_orders_silver = (
        df_orders
        .select(
            F.col("ID_PEDIDO").alias("order_id"),
            F.col("UF").alias("state"),
            F.to_timestamp("DATA_CRIACAO", "yyyy-MM-dd'T'HH:mm:ss").alias("order_date"),
            (
                F.col("VALOR_UNITARIO").cast("double") *
                F.col("QUANTIDADE").cast("int")
            ).alias("order_total")
        )
    )

    df_payments_silver = (
        df_payments
        .select(
            F.col("id_pedido").alias("order_id"),
            F.col("forma_pagamento").alias("payment_method"),
            F.col("status"),
            F.col("avaliacao_fraude.fraude").alias("fraud_flag"),
            F.to_timestamp("data_processamento", "yyyy-MM-dd'T'HH:mm:ss").alias("processing_date")
        )
    )

    df_report = (
        df_orders_silver
        .filter(F.year("order_date") == 2025)
        .join(
            df_payments_silver.filter(
                (F.col("status") == F.lit(False)) &
                (F.col("fraud_flag") == F.lit(False))
            ),
            on="order_id",
            how="inner"
        )
        .select(
            "order_id",
            "state",
            "payment_method",
            "order_total",
            "order_date"
        )
        .orderBy(
            F.col("state").asc(),
            F.col("payment_method").asc(),
            F.col("order_date").asc()
        )
    )

    df_report.show(20, truncate=False)
    df_report.printSchema()

    (
        df_report
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print(f"Report successfully written to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Pipeline execution failed: {e}", file=sys.stderr)
        raise