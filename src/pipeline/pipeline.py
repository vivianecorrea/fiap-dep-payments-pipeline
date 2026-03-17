import logging

import pyspark.sql.functions as F

from config.app_config import AppConfig
from src.business.columns import OutputColumns, ReportColumns
from src.business.sales_report import SalesReportBuilder
from src.business.schemas import Schemas
from src.io.data_io import DataIO
from src.spark.spark_session_manager import SparkSessionManager

logger = logging.getLogger(__name__)


class PaymentsReportPipeline:
    def __init__(
        self,
        config: AppConfig,
        spark_manager: SparkSessionManager,
        data_io: DataIO,
        report_builder: SalesReportBuilder,
    ) -> None:
        self.config = config
        self.spark_manager = spark_manager
        self.data_io = data_io
        self.report_builder = report_builder

    def run(self) -> None:
        logger.info("Pipeline started")

        orders_cfg = self.config.sources["orders"]
        payments_cfg = self.config.sources["payments"]
        sink_cfg = self.config.sink

        orders_df = self.data_io.read(
            path=orders_cfg.path,
            fmt=orders_cfg.format,
            schema=Schemas.ORDERS,
            options=orders_cfg.options,
        )

        payments_df = self.data_io.read(
            path=payments_cfg.path,
            fmt=payments_cfg.format,
            schema=Schemas.PAYMENTS,
            options=payments_cfg.options,
        )

        report_df = self.report_builder.build_report(
            orders_df=orders_df,
            payments_df=payments_df,
        )

        output_df = report_df.select(
            F.col(ReportColumns.ORDER_ID).alias(OutputColumns.ORDER_ID),
            F.col(ReportColumns.STATE).alias(OutputColumns.STATE),
            F.col(ReportColumns.PAYMENT_METHOD).alias(OutputColumns.PAYMENT_METHOD),
            F.col(ReportColumns.ORDER_TOTAL).alias(OutputColumns.ORDER_TOTAL),
            F.col(ReportColumns.ORDER_DATE).alias(OutputColumns.ORDER_DATE),
        )

        self.data_io.write(
            df=output_df,
            path=sink_cfg.path,
            fmt=sink_cfg.format,
            mode=sink_cfg.mode,
        )

        logger.info("Pipeline completed. Report written to %s", sink_cfg.path)
