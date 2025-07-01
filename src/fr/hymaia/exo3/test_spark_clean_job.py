from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_clean_job import filter_age
from pyspark.sql import Row
from pyspark.testing import assertDataFrameEqual


class TestSparkCleanJob(unittest.TestCase):
    def test_filter_age(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(dumb="ignore", age=17),
                Row(dumb="ignore", age=18),
                Row(dumb="ignore", age=19),
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(dumb="ignore", age=18),
                Row(dumb="ignore", age=19),
            ]
        )
        
        # WHEN
        actual = filter_age(input)
        
        # THEN
        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.columns, expected.columns)