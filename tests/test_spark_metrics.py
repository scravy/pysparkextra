import unittest

from pysparkextra.funcs import *
from pysparkextra.metrics import SparkMetrics
from pysparkextra.unittestcase import SparkTest


class MetricsTestCase(SparkTest):
    def test_metrics(self):
        df: DataFrame = self.spark_session.createDataFrame(
            [
                [1, 2],
                [-3, 4],
            ],
            schema=("foo", "bar")
        )

        with SparkMetrics(self.spark_session) as metrics:
            df.write.parquet("/tmp/file.parquet", mode='overwrite')
        self.assertEqual(2, metrics['numOutputRows'])

        with SparkMetrics(self.spark_session) as metrics:
            df.union(df).write.parquet("/tmp/file.parquet", mode='overwrite')
        self.assertEqual(4, metrics['numOutputRows'])


if __name__ == '__main__':
    unittest.main()
