import subprocess
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from src.io.data_io import DataIO


class ApiGitToBronze:

    def __init__(
        self,
        input_path: str,
        bronze_output_path: str,
        schema: StructType,
        file_format: str ,
        spark: SparkSession
    ):
        self.script_path = "/fiap-dep-payments-pipeline/src/layers/apigit_to_bronze/setup_datasets.sh"
        self.input_path = input_path
        self.bronze_output_path = bronze_output_path
        self.schema = schema
        self.file_format = file_format
        self.spark = spark

    def run_script(self) -> None:
        result = subprocess.run(
            ["bash", self.script_path],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Script execution failed:\n{result.stderr}"
            )

    def read_dataset(self) -> DataFrame:
        data_io = DataIO(self.spark)
        if self.file_format == "csv":
            return data_io.read_csv(self.input_path, self.schema)

        if self.file_format == "json":
            return data_io.read_json(self.input_path, self.schema)

        raise ValueError(f"Unsupported format: {self.file_format}")

    def run(self) -> DataFrame:
        self.run_script()

        df = self.read_dataset()
        data_io = DataIO(self.spark)
        data_io.write_parquet(df, self.bronze_output_path)

        return df