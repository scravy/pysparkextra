# pyspark extra utilities

## SparkMetrics

Track metrics (like number of rows/number of files written) when writing a DataFrame.

```python
import pyspark.sql
from pysparkextra.metrics import SparkMetrics

spark_session: pyspark.sql.SparkSession
df: pyspark.sql.DataFrame = spark_session.createDataFrame(
    [
        [1, 2],
        [-3, 4],
    ],
    schema=("foo", "bar")
)

with SparkMetrics(spark_session) as metrics:
    df.write.parquet("/tmp/target", mode='overwrite')
print(metrics['numOutputRows'])  # 2

with SparkMetrics(spark_session) as metrics:
    df.union(df).write.parquet("/tmp/target", mode='overwrite')
print(metrics['numOutputRows'])  # 4

print(metrics)  # {'numFiles': 5, 'numOutputBytes': 3175, 'numOutputRows': 4, 'numParts': 0}
```

## union arbitrary number of dataframes with arbitrary number of columns

```python
from pyspark.sql import DataFrame, SparkSession
from pysparkextra.funcs import union

spark_session: SparkSession
df1: DataFrame = spark_session.createDataFrame(
    [
        [1, 2],
        [3, 4],
    ], schema=("foo", "bar"))
df2: DataFrame = spark_session.createDataFrame(
    [
        [10, 20, 30],
        [40, 50, 60],
    ], schema=("bar", "qux", "foo")
)
df3: DataFrame = spark_session.createDataFrame(
    [
        [100, 200],
        [300, 400],
    ], schema=("foo", "bar")
)

df: DataFrame = union(df1, df2, df3)

df.show()

# +---+---+----+
# |foo|bar| qux|
# +---+---+----+
# |  1|  2|null|
# |  3|  4|null|
# | 30| 10|  20|
# | 60| 40|  50|
# |100|200|null|
# |300|400|null|
# +---+---+----+
```

## and more

Check out the tests, which also act as examples.
