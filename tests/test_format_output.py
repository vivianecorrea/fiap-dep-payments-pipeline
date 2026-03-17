from src.business.sales_report import SalesReportBuilder


def test_should_return_only_required_report_columns(
    spark_session,
    payments_schema,
    orders_schema,
):
    payments_data = [
        (
            {"fraude": "false", "score": "10"},
            "2025-01-10T12:00:00",
            "credit_card",
            "1",
            "false",
            "100.00",
        )
    ]

    orders_data = [
        (
            "1",
            "notebook",
            "50.00",
            "2",
            "2025-01-09T10:00:00",
            "SP",
            "123",
        )
    ]

    df_payments = spark_session.createDataFrame(
        data=payments_data,
        schema=payments_schema,
    )

    df_orders = spark_session.createDataFrame(
        data=orders_data,
        schema=orders_schema,
    )

    report_df = SalesReportBuilder().build_report(
        orders_df=df_orders,
        payments_df=df_payments,
    )

    expected_columns = [
        "order_id",
        "state",
        "payment_method",
        "order_total",
        "order_date",
    ]

    assert report_df.columns == expected_columns


def test_should_sort_report_by_uf_payment_method_and_order_date(
    spark_session,
    payments_schema,
    orders_schema,
):
    payments_data = [
        ({"fraude": "false", "score": "1"}, "2025-01-03T10:00:00", "pix", "1", "false", "100.00"),
        ({"fraude": "false", "score": "1"}, "2025-01-01T10:00:00", "credit_card", "2", "false", "100.00"),
        ({"fraude": "false", "score": "1"}, "2025-01-02T10:00:00", "credit_card", "3", "false", "100.00"),
    ]

    orders_data = [
        ("1", "produto", "50.00", "2", "2025-01-03T09:00:00", "SP", "10"),
        ("2", "produto", "50.00", "2", "2025-01-01T09:00:00", "RJ", "11"),
        ("3", "produto", "50.00", "2", "2025-01-02T09:00:00", "SP", "12"),
    ]

    df_payments = spark_session.createDataFrame(
        data=payments_data,
        schema=payments_schema,
    )

    df_orders = spark_session.createDataFrame(
        data=orders_data,
        schema=orders_schema,
    )

    result = SalesReportBuilder().build_report(
        orders_df=df_orders,
        payments_df=df_payments,
    )

    result_order = [
        (row.state, row.payment_method, row.order_date)
        for row in result.collect()
    ]

    expected_order = sorted(result_order)

    assert result_order == expected_order
