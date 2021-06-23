import unittest

from pyspark.sql import SparkSession


class SparkTest(unittest.TestCase):

    spark_session: SparkSession

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_session = SparkSession.builder.getOrCreate()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark_session.stop()
