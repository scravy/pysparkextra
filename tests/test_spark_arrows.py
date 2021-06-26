import unittest

from pysparkextra.arrows import *
from pysparkextra.funcs import *
from pysparkextra.unittestcase import SparkTest


class ArrowsTestCase(SparkTest):
    def test_flow(self):
        df: DataFrame = apply(
            self.spark_session.createDataFrame(
                [
                    [1, 2],
                    [-3, 4],
                ],
                schema=("foo", "bar")
            ),
            pipe(
                split(lt('foo', 0)),
                parallel(
                    drop_col('bar'),
                    with_cols(foo=col('foo') + 1),
                ),
                union,
            )
        )
        self.assertEqual(
            [
                [-3, None],
                [2, 2],
            ],
            [[c for c in r] for r in df.collect()],
        )


if __name__ == '__main__':
    unittest.main()
