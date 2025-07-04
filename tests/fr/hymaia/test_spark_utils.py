# tests/test_spark_utils.py
from pyspark.sql import SparkSession

def get_spark_session(app_name="test"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
