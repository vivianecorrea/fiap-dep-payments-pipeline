import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType, DecimalType

from src.business.columns import OrderInputColumns, PaymentInputColumns, ReportColumns

logger = logging.getLogger(__name__)


class SalesReportBuilder:
    INPUT_TIMESTAMP_PATTERN = "yyyy-MM-dd'T'HH:mm:ss"

    def __init__(self, report_year: int = 2025) -> None:
        self.report_year = report_year

    def build_report(self, orders_df: DataFrame, payments_df: DataFrame) -> DataFrame:
        try:
            logger.info("Starting report build for year %s", self.report_year)

            orders_typed_df = self._build_orders_typed(orders_df)
            logger.info("Orders typed stage complete")

            payments_typed_df = self._build_payments_typed(payments_df)
            logger.info("Payments typed stage complete")

            orders_silver_df = self._build_orders_silver(orders_typed_df)
            logger.info("Orders silver stage complete")

            payments_silver_df = self._build_payments_silver(payments_typed_df)
            logger.info("Payments silver stage complete")

            report_df = self._filter_and_join(orders_silver_df, payments_silver_df)
            logger.info("Report join and filter complete")

            return report_df

        except Exception as exc:
            logger.error("Report build failed: %s", exc)
            raise

    def _filter_orders(self, orders_silver_df: DataFrame) -> DataFrame:
        return orders_silver_df.filter(
            F.year(F.col(ReportColumns.ORDER_DATE)) == self.report_year
        )

    def _filter_payments(self, payments_silver_df: DataFrame) -> DataFrame:
        return payments_silver_df.filter(
            (F.col(ReportColumns.PAYMENT_STATUS) == F.lit(False)) &
            (F.col(ReportColumns.FRAUD_FLAG) == F.lit(False))
        )

    def _filter_and_join(
        self,
        orders_silver_df: DataFrame,
        payments_silver_df: DataFrame,
    ) -> DataFrame:
        filtered_orders = self._filter_orders(orders_silver_df)
        filtered_payments = self._filter_payments(payments_silver_df)

        return (
            filtered_orders
            .join(filtered_payments, on=ReportColumns.ORDER_ID, how="inner")
            .select(
                F.col(ReportColumns.ORDER_ID),
                F.col(ReportColumns.STATE),
                F.col(ReportColumns.PAYMENT_METHOD),
                F.col(ReportColumns.ORDER_TOTAL),
                F.col(ReportColumns.ORDER_DATE),
            )
            .orderBy(
                F.col(ReportColumns.STATE).asc(),
                F.col(ReportColumns.PAYMENT_METHOD).asc(),
                F.col(ReportColumns.ORDER_DATE).asc(),
            )
        )

    @staticmethod
    def _calculate_order_total() -> F.Column:
        return (
            F.col(OrderInputColumns.UNIT_PRICE).cast(DecimalType(12, 2)) *
            F.col(OrderInputColumns.QUANTITY).cast("int")
        ).cast(DecimalType(12, 2))

    def _build_orders_typed(self, orders_df: DataFrame) -> DataFrame:
        return orders_df.select(
            F.col(OrderInputColumns.ID),
            F.col(OrderInputColumns.PRODUCT),
            F.col(OrderInputColumns.UNIT_PRICE).cast(DecimalType(12, 2)).alias(OrderInputColumns.UNIT_PRICE),
            F.col(OrderInputColumns.QUANTITY).cast("int").alias(OrderInputColumns.QUANTITY),
            F.to_timestamp(
                F.col(OrderInputColumns.CREATED_AT),
                self.INPUT_TIMESTAMP_PATTERN,
            ).alias(OrderInputColumns.CREATED_AT),
            F.col(OrderInputColumns.STATE),
            F.col(OrderInputColumns.CUSTOMER_ID),
        )

    def _build_payments_typed(self, payments_df: DataFrame) -> DataFrame:
        return payments_df.select(
            F.col(PaymentInputColumns.FRAUD_EVAL),
            F.to_timestamp(
                F.col(PaymentInputColumns.PROCESSING_DATE),
                self.INPUT_TIMESTAMP_PATTERN,
            ).alias(PaymentInputColumns.PROCESSING_DATE),
            F.col(PaymentInputColumns.PAYMENT_METHOD),
            F.col(PaymentInputColumns.ORDER_ID),
            self._parse_status_to_boolean(
                F.col(PaymentInputColumns.STATUS)
            ).alias(PaymentInputColumns.STATUS),
            F.col(PaymentInputColumns.PAYMENT_VALUE),
        )

    def _build_orders_silver(self, orders_typed_df: DataFrame) -> DataFrame:
        return orders_typed_df.select(
            F.col(OrderInputColumns.ID).alias(ReportColumns.ORDER_ID),
            F.col(OrderInputColumns.STATE).alias(ReportColumns.STATE),
            F.col(OrderInputColumns.CREATED_AT).alias(ReportColumns.ORDER_DATE),
            self._calculate_order_total().alias(ReportColumns.ORDER_TOTAL),
        )

    def _build_payments_silver(self, payments_typed_df: DataFrame) -> DataFrame:
        return payments_typed_df.select(
            F.col(PaymentInputColumns.ORDER_ID).alias(ReportColumns.ORDER_ID),
            F.col(PaymentInputColumns.PAYMENT_METHOD).alias(ReportColumns.PAYMENT_METHOD),
            F.col(PaymentInputColumns.STATUS).alias(ReportColumns.PAYMENT_STATUS),
            F.col(PaymentInputColumns.FRAUD_FLAG).alias(ReportColumns.FRAUD_FLAG),
            F.col(PaymentInputColumns.PROCESSING_DATE).alias(ReportColumns.PROCESSING_DATE),
        )

    @staticmethod
    def _parse_status_to_boolean(column: F.Column) -> F.Column:
        normalized = F.lower(F.trim(column))
        return (
            F.when(normalized == F.lit("true"), F.lit(True))
            .when(normalized == F.lit("false"), F.lit(False))
            .otherwise(F.lit(None).cast(BooleanType()))
        )
