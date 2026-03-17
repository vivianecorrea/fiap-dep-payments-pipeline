import pytest

from src.business.schemas import Schemas
from src.spark.spark_session_manager import SparkSessionManager


@pytest.fixture(scope="session", autouse=True)
def spark_session():
    manager = SparkSessionManager(
        app_name="pytest-pyspark-testing",
        log_level="ERROR",
        master="local[*]",
    )
    spark = manager.create_session()
    yield spark
    manager.stop_session()


@pytest.fixture
def payments_schema():
    return Schemas.PAYMENTS


@pytest.fixture
def orders_schema():
    return Schemas.ORDERS
