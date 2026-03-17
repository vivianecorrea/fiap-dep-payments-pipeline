import logging

from config.app_config import AppConfig
from src.business.sales_report import SalesReportBuilder
from src.io.data_io import DataIO
from src.pipeline.pipeline import PaymentsReportPipeline
from src.spark.spark_session_manager import SparkSessionManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CONFIG_PATH = "config/settings.yaml"


def main() -> None:
    logger.info("Loading configuration from %s", CONFIG_PATH)
    config = AppConfig.from_yaml(CONFIG_PATH)

    spark_manager = SparkSessionManager(
        app_name=config.spark.app_name,
        log_level=config.spark.log_level,
    )

    try:
        spark = spark_manager.create_session()

        data_io = DataIO(spark)

        report_builder = SalesReportBuilder(
            report_year=config.pipeline.report_year,
        )

        pipeline = PaymentsReportPipeline(
            config=config,
            spark_manager=spark_manager,
            data_io=data_io,
            report_builder=report_builder,
        )

        pipeline.run()

    except Exception as exc:
        logger.error("Pipeline execution failed: %s", exc)
        raise
    finally:
        spark_manager.stop_session()


if __name__ == "__main__":
    main()
