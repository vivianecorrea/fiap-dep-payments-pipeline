from decimal import Decimal

from src.sales import SalesReportBuilder


def test_should_calculate_order_total_correctly(
    spark_session,
    payments_schema,
    orders_schema,
):
    payments_data = [
        ({"fraude": False, "score": "10"}, "2025-06-10T12:00:00", "pix", "1", "false", "30.00"),
    ]

    orders_data = [
        ("1", "produto_a", "10.00", "3", "2025-06-10T09:00:00", "SP", "101"),
    ]

    df_payments = spark_session.createDataFrame(payments_data, payments_schema)
    df_orders = spark_session.createDataFrame(orders_data, orders_schema)

    result = SalesReportBuilder(
        payments_df=df_payments,
        orders_df=df_orders,
    ).build_report()

    row = result.collect()[0]

    assert row.order_total == Decimal("30.00")


def test_should_round_order_total_to_two_decimal_places(
    spark_session,
    payments_schema,
    orders_schema,
):
    payments_data = [
        ({"fraude": "false", "score": "10"}, "2025-06-10T12:00:00", "credit_card", "1", "false", "10.01"),
    ]

    orders_data = [
        ("1", "produto_a", "3.33", "3", "2025-06-10T09:00:00", "SP", "101"),
    ]

    df_payments = spark_session.createDataFrame(payments_data, payments_schema)
    df_orders = spark_session.createDataFrame(orders_data, orders_schema)

    result = SalesReportBuilder(
        payments_df=df_payments,
        orders_df=df_orders,
    ).build_report()

    row = result.collect()[0]

    assert row.order_total == Decimal("9.99")