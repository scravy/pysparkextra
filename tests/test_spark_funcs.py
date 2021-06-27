import unittest

from pysparkextra.arrows import *
from pysparkextra.funcs import *
from pysparkextra.unittestcase import SparkTest


class SparkFuncsTestCase(SparkTest):
    def make_dfs(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        df1: DataFrame = self.spark_session.createDataFrame(
            [
                [1, 2],
                [3, 4],
            ], schema=("foo", "bar"))
        df2: DataFrame = self.spark_session.createDataFrame(
            [
                [10, 20, 30],
                [40, 50, 60],
            ], schema=("bar", "qux", "foo")
        )
        df3: DataFrame = self.spark_session.createDataFrame(
            [
                [100, 200],
                [300, 400],
            ], schema=("foo", "bar")
        )
        return df1, df2, df3

    def test_union(self):
        df: DataFrame = apply(
            [
                [1, 2],
                [3, 4],
            ],
            pipe(
                lambda x: (x, x),
                both(lambda data: self.spark_session.createDataFrame(data, schema=("foo", "bar"))),
                second(rename_col("bar", "qux")),
                union,
            )
        )
        self.assertEqual(
            [
                [1, 2, None],
                [3, 4, None],
                [1, None, 2],
                [3, None, 4],
            ],
            [[c for c in r] for r in df.collect()],
        )

    def test_union_multiple(self):
        df1, df2, df3 = self.make_dfs()
        df = union(df1, df2, df3)
        self.assertEqual(
            [
                [1, 2, None],
                [3, 4, None],
                [30, 10, 20],
                [60, 40, 50],
                [100, 200, None],
                [300, 400, None],
            ],
            [[c for c in r] for r in df.collect()],
        )

    def test_union_lists(self):
        df1, df2, df3 = self.make_dfs()
        df = union(df1, [df2, df3])
        self.assertEqual(
            [
                [1, 2, None],
                [3, 4, None],
                [30, 10, 20],
                [60, 40, 50],
                [100, 200, None],
                [300, 400, None],
            ],
            [[c for c in r] for r in df.collect()],
        )

    def test_and(self):
        df1, *_ = self.make_dfs()

        row, = df1.filter(AND(
            neq("foo", 0),
            neq("foo", 1),
            neq("foo", 2),
        )).collect()
        self.assertEqual(3, row[0])

    def test_or(self):
        df1, *_ = self.make_dfs()

        row, = df1.filter(OR(
            eq("foo", 0),
            eq("foo", 1),
            eq("foo", 2),
        )).collect()
        self.assertEqual(1, row[0])

    def test_count_partitions(self):
        result = apply(
            self.spark_session.createDataFrame(
                [
                    *([[1, 2]] * 200),
                    [3, 4],
                    [5, 6],
                    [7, 8],
                    [9, 10],
                    [11, 12],
                ], schema=("foo", "bar")
            ),
            pipe(
                repartition(2),
                count_partitions,
                print_df,
                collect,
            )
        )
        self.assertEqual(2, len(result))

    def test_estimate_size(self):
        df, size = apply(
            self.spark_session.createDataFrame(
                [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10], [11, 12]] * 100,
                schema=("foo", "bar")
            ),
            estimate_size,
        )
        print(size)


if __name__ == '__main__':
    unittest.main()
