from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

class DataIO:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_csv(self, path: str, schema: StructType) -> DataFrame:
        return self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(path)

    def read_json(self, path: str, schema: StructType) -> DataFrame:
        return self.spark.read \
            .format("json") \
            .schema(schema) \
            .load(path)

    def write_parquet(self, df: DataFrame, path: str):
        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(path)