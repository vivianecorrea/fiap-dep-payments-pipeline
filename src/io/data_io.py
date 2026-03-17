import logging
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class DataIO:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(
        self,
        path: str,
        fmt: str,
        schema: StructType,
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        logger.info("Reading %s from %s", fmt, path)
        reader = self.spark.read.format(fmt).schema(schema)
        for key, value in (options or {}).items():
            reader = reader.option(key, value)
        return reader.load(path)

    def write(
        self,
        df: DataFrame,
        path: str,
        fmt: str = "parquet",
        mode: str = "overwrite",
    ) -> None:
        logger.info("Writing %s to %s (mode=%s)", fmt, path, mode)
        df.write.format(fmt).mode(mode).save(path)
