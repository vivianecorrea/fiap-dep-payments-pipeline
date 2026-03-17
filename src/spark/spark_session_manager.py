from typing import Optional

from pyspark.sql import SparkSession


class SparkSessionManager:

    def __init__(
        self,
        app_name: str = "dataeng-spark-sql-mvp",
        log_level: str = "WARN",
        master: Optional[str] = None,
    ):
        self.app_name = app_name
        self.log_level = log_level
        self.master = master
        self._spark: Optional[SparkSession] = None

    def create_session(self) -> SparkSession:
        if self._spark is None:
            builder = (
                SparkSession.builder
                .appName(self.app_name)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.parquet.compression.codec", "snappy")
            )
            if self.master:
                builder = builder.master(self.master)
            self._spark = builder.getOrCreate()
            self._spark.sparkContext.setLogLevel(self.log_level)
        return self._spark

    def get_session(self) -> SparkSession:
        if self._spark is None:
            return self.create_session()
        return self._spark

    def stop_session(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    def __enter__(self) -> SparkSession:
        return self.create_session()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop_session()
