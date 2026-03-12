import sys

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    DecimalType,
)


ORDERS_PATH = "datasets/datasets-csv-pedidos/data/pedidos/"
PAYMENTS_PATH = "datasets/dataset-json-pagamentos/data/pagamentos/"
OUTPUT_PATH = "datasets/output/sales_report_2025/"


orders_schema = StructType([
    StructField("ID_PEDIDO", StringType(), True),
    StructField("PRODUTO", StringType(), True),
    StructField("VALOR_UNITARIO", DecimalType(10, 2), True),
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
            StructField("score", StringType(), True),
        ]),
        True,
    ),
    StructField("data_processamento", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("id_pedido", StringType(), True),
    StructField("status", StringType(), True),
    StructField("valor_pagamento", DecimalType(10, 2), True),
])


class SalesReportBuilder:
    REPORT_YEAR = 2025
    INPUT_TIMESTAMP_PATTERN = "yyyy-MM-dd'T'HH:mm:ss"

    def __init__(self, payments_df: DataFrame, orders_df: DataFrame) -> None:
        self.payments_df = payments_df
        self.orders_df = orders_df

    def build_report(self) -> DataFrame:
        orders_typed_df = self._build_orders_typed()
        payments_typed_df = self._build_payments_typed()

        orders_silver_df = self._build_orders_silver(orders_typed_df)
        payments_silver_df = self._build_payments_silver(payments_typed_df)

        return (
            orders_silver_df
            .filter(F.year(F.col("order_date")) == self.REPORT_YEAR)
            .join(
                payments_silver_df.filter(
                    (F.col("payment_status") == F.lit(False)) &
                    (F.col("fraud_flag") == F.lit(False))
                ),
                on="order_id",
                how="inner",
            )
            .select(
                F.col("order_id"),
                F.col("state"),
                F.col("payment_method"),
                F.col("order_total"),
                F.col("order_date"),
            )
            .orderBy(
                F.col("state").asc(),
                F.col("payment_method").asc(),
                F.col("order_date").asc(),
            )
        )

    def _build_orders_typed(self) -> DataFrame:
        return (
            self.orders_df
            .select(
                F.col("ID_PEDIDO"),
                F.col("PRODUTO"),
                F.col("VALOR_UNITARIO").cast(DecimalType(12, 2)).alias("VALOR_UNITARIO"),
                F.col("QUANTIDADE").cast("int").alias("QUANTIDADE"),
                F.to_timestamp(
                    F.col("DATA_CRIACAO"),
                    self.INPUT_TIMESTAMP_PATTERN,
                ).alias("DATA_CRIACAO"),
                F.col("UF"),
                F.col("ID_CLIENTE"),
            )
        )

    def _build_payments_typed(self) -> DataFrame:
        return (
            self.payments_df
            .select(
                F.col("avaliacao_fraude"),
                F.to_timestamp(
                    F.col("data_processamento"),
                    self.INPUT_TIMESTAMP_PATTERN,
                ).alias("data_processamento"),
                F.col("forma_pagamento"),
                F.col("id_pedido"),
                self._parse_status_to_boolean(F.col("status")).alias("status"),
                F.col("valor_pagamento").cast(DecimalType(12, 2)).alias("valor_pagamento"),
            )
        )

    def _build_orders_silver(self, orders_typed_df: DataFrame) -> DataFrame:
        return (
            orders_typed_df
            .select(
                F.col("ID_PEDIDO").alias("order_id"),
                F.col("UF").alias("state"),
                F.col("DATA_CRIACAO").alias("order_date"),
                (
                    F.col("VALOR_UNITARIO") *
                    F.col("QUANTIDADE")
                ).cast(DecimalType(12, 2)).alias("order_total"),
            )
        )

    def _build_payments_silver(self, payments_typed_df: DataFrame) -> DataFrame:
        return (
            payments_typed_df
            .select(
                F.col("id_pedido").alias("order_id"),
                F.col("forma_pagamento").alias("payment_method"),
                F.col("status").alias("payment_status"),
                F.col("avaliacao_fraude.fraude").alias("fraud_flag"),
                F.col("data_processamento").alias("processing_date"),
            )
        )

    @staticmethod
    def _parse_status_to_boolean(column: F.Column) -> F.Column:
        normalized_column = F.lower(F.trim(column))

        return (
            F.when(normalized_column == F.lit("true"), F.lit(True))
            .when(normalized_column == F.lit("false"), F.lit(False))
            .otherwise(F.lit(None).cast(BooleanType()))
        )


def main() -> None:
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

    report_builder = SalesReportBuilder(
        payments_df=df_payments,
        orders_df=df_orders,
    )

    df_report = report_builder.build_report()

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
    except Exception as exc:
        print(f"Pipeline execution failed: {exc}", file=sys.stderr)
        raise