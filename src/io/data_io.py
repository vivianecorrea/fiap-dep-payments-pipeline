from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

class DataIO:
    """Classe responsável pela leitura e escrita de dados (Critério 6)."""
    
    def __init__(self, spark: SparkSession):
        # Injeção de dependência da sessão Spark (Critério 3)
        self.spark = spark

    def read_csv(self, path: str, schema: StructType) -> DataFrame:
        """Leitura de CSV com schema explícito."""
        return self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(path)

    def read_json(self, path: str, schema: StructType) -> DataFrame:
        """Leitura de JSON com schema explícito."""
        return self.spark.read \
            .format("json") \
            .schema(schema) \
            .load(path)

    def write_parquet(self, df: DataFrame, path: str):
        """Escrita do relatório final em formato Parquet."""
        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(path)