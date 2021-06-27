import unittest

from pyspark.sql import SparkSession

from pysparkextra import spark


class SparkTest(unittest.TestCase):

    spark_session: SparkSession

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_session = spark()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark_session.stop()
