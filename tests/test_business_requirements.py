from src.business.sales_report import SalesReportBuilder


def test_should_keep_only_orders_with_refused_payment_and_legitimate_fraud_analysis(
    spark_session,
    payments_schema,
    orders_schema,
):
    payments_data = [
        ({"fraude": "false", "score": "10"}, "2025-01-10T12:00:00", "pix", "1", "false", "100.00"),
        ({"fraude": "true", "score": "90"}, "2025-01-10T12:00:00", "credit_card", "2", "false", "200.00"),
        ({"fraude": "false", "score": "15"}, "2025-01-10T12:00:00", "boleto", "3", "true", "300.00"),
    ]

    orders_data = [
        ("1", "produto_a", "50.00", "2", "2025-01-09T10:00:00", "SP", "101"),
        ("2", "produto_b", "100.00", "2", "2025-01-09T10:00:00", "RJ", "102"),
        ("3", "produto_c", "150.00", "2", "2025-01-09T10:00:00", "MG", "103"),
    ]

    df_payments = spark_session.createDataFrame(payments_data, payments_schema)
    df_orders = spark_session.createDataFrame(orders_data, orders_schema)

    result = SalesReportBuilder().build_report(
        orders_df=df_orders,
        payments_df=df_payments,
    )

    result_order_ids = [row.order_id for row in result.collect()]

    assert result_order_ids == ["1"]


def test_should_keep_only_orders_created_in_2025(
    spark_session,
    payments_schema,
    orders_schema,
):
    payments_data = [
        ({"fraude": "false", "score": "10"}, "2024-12-31T12:00:00", "pix", "1", "false", "100.00"),
        ({"fraude": "false", "score": "20"}, "2025-06-10T12:00:00", "credit_card", "2", "false", "200.00"),
        ({"fraude": "false", "score": "30"}, "2026-01-01T12:00:00", "boleto", "3", "false", "300.00"),
    ]

    orders_data = [
        ("1", "produto_a", "50.00", "2", "2024-12-31T23:59:59", "SP", "101"),
        ("2", "produto_b", "100.00", "2", "2025-06-10T09:00:00", "RJ", "102"),
        ("3", "produto_c", "150.00", "2", "2026-01-01T00:00:00", "MG", "103"),
    ]

    df_payments = spark_session.createDataFrame(payments_data, payments_schema)
    df_orders = spark_session.createDataFrame(orders_data, orders_schema)

    result = SalesReportBuilder().build_report(
        orders_df=df_orders,
        payments_df=df_payments,
    )

    result_order_ids = [row.order_id for row in result.collect()]

    assert result_order_ids == ["2"]


def test_should_include_orders_on_2025_boundary_dates(
    spark_session,
    payments_schema,
    orders_schema,
):
    payments_data = [
        ({"fraude": "false", "score": "10"}, "2025-01-01T12:00:00", "pix", "1", "false", "100.00"),
        ({"fraude": "false", "score": "20"}, "2025-12-31T12:00:00", "credit_card", "2", "false", "200.00"),
        ({"fraude": "false", "score": "30"}, "2024-12-31T12:00:00", "boleto", "3", "false", "300.00"),
        ({"fraude": "false", "score": "40"}, "2026-01-01T12:00:00", "debit_card", "4", "false", "400.00"),
    ]

    orders_data = [
        ("1", "produto_a", "50.00", "2", "2025-01-01T00:00:00", "SP", "101"),
        ("2", "produto_b", "100.00", "2", "2025-12-31T23:59:59", "RJ", "102"),
        ("3", "produto_c", "150.00", "2", "2024-12-31T23:59:59", "MG", "103"),
        ("4", "produto_d", "200.00", "2", "2026-01-01T00:00:00", "BA", "104"),
    ]

    df_payments = spark_session.createDataFrame(payments_data, payments_schema)
    df_orders = spark_session.createDataFrame(orders_data, orders_schema)

    result = SalesReportBuilder().build_report(
        orders_df=df_orders,
        payments_df=df_payments,
    )

    result_order_ids = [row.order_id for row in result.collect()]

    assert result_order_ids == ["2", "1"]
