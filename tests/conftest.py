import pytest
from pyspark_learning.spark_session import create_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session("pytest-pyspark-learning", shuffle_partitions=2)
    yield session
    session.stop()
