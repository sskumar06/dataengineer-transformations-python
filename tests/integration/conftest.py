# import findspark
# findspark.init('/Users/saravans/Documents/de/spark-3.0.2-bin-hadoop2.7')
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def SPARK():
    return SparkSession.builder.appName("IntegrationTests").getOrCreate()
