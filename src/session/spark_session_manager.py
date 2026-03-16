#--Adding management classes 


import sys
from typing import Optional

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


class SparkSessionManager:
    """Gerencia a criação e o ciclo de vida da SparkSession"""
    
    def __init__(self, app_name: str = "dataeng-spark-sql-mvp", log_level: str = "WARN"):
        self.app_name = app_name
        self.log_level = log_level
        self._spark: Optional[SparkSession] = None
    
    def create_session(self) -> SparkSession:
        """Cria e configura a SparkSession"""
        if self._spark is None:
            self._spark = (
                SparkSession.builder
                .appName(self.app_name)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel(self.log_level)
        return self._spark
    
    def get_session(self) -> SparkSession:
        """Retorna a SparkSession existente ou cria uma nova"""
        if self._spark is None:
            return self.create_session()
        return self._spark
    
    def stop_session(self) -> None:
        """Para a SparkSession e libera recursos"""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
    
    def __enter__(self) -> SparkSession:
        """Context manager para usar com 'with'"""
        return self.create_session()
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Garante que a sessão seja parada ao sair do contexto"""
        self.stop_session()


class SparkDataLoader:
    """Gerencia o carregamento de dados usando uma SparkSession"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def load_orders(self, path: str, schema: StructType) -> DataFrame:
        """Carrega os dados de pedidos do CSV"""
        return (
            self.spark.read
            .format("csv")
            .option("compression", "gzip")
            .option("sep", ";")
            .option("header", True)
            .schema(schema)
            .load(path)
        )
    
    def load_payments(self, path: str, schema: StructType) -> DataFrame:
        """Carrega os dados de pagamentos do JSON"""
        return (
            self.spark.read
            .format("json")
            .option("compression", "gzip")
            .schema(schema)
            .load(path)
        )


class SparkDataWriter:
    """Gerencia a escrita de dados usando uma SparkSession"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite") -> None:
        """Escreve um DataFrame no formato Parquet"""
        (
            df.write
            .mode(mode)
            .parquet(path)
        )


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


class PipelineExecutor:
    """Classe principal que orquestra toda a execução do pipeline"""
    
    def __init__(self, session_manager: SparkSessionManager):
        self.session_manager = session_manager
        self.data_loader: Optional[SparkDataLoader] = None
        self.data_writer: Optional[SparkDataWriter] = None
    
    def _initialize_components(self, spark: SparkSession) -> None:
        """Inicializa os componentes auxiliares"""
        self.data_loader = SparkDataLoader(spark)
        self.data_writer = SparkDataWriter(spark)
    
    def execute(self) -> None:
        """Executa o pipeline completo"""
        try:
            # Criar sessão Spark
            with self.session_manager as spark:
                self._initialize_components(spark)
                
                # Carregar dados
                print("Carregando dados de pedidos...")
                df_orders = self.data_loader.load_orders(ORDERS_PATH, orders_schema)
                
                print("Carregando dados de pagamentos...")
                df_payments = self.data_loader.load_payments(PAYMENTS_PATH, payments_schema)
                
                # Construir relatório
                print("Construindo relatório...")
                report_builder = SalesReportBuilder(
                    payments_df=df_payments,
                    orders_df=df_orders,
                )
                
                df_report = report_builder.build_report()
                
                # Mostrar resultados
                print("\nRelatório gerado:")
                df_report.show(20, truncate=False)
                df_report.printSchema()
                
                # Salvar resultados
                print(f"\nSalvando relatório em: {OUTPUT_PATH}")
                self.data_writer.write_parquet(df_report, OUTPUT_PATH)
                
                print(f"✅ Relatório salvo com sucesso em: {OUTPUT_PATH}")
                
        except Exception as e:
            print(f"❌ Erro na execução do pipeline: {e}", file=sys.stderr)
            raise


def main() -> None:
    """Função principal que inicia o pipeline"""
    
    # Criar gerenciador de sessão
    session_manager = SparkSessionManager(
        app_name="dataeng-spark-sql-mvp",
        log_level="WARN"
    )
    
    # Criar e executar pipeline
    pipeline = PipelineExecutor(session_manager)
    pipeline.execute()


if __name__ == "__main__":
    main()
