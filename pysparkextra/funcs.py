import os
import typing
from functools import reduce
from numbers import Number
from typing import Union, List, Iterable, Optional, Callable, Tuple

import pyspark.sql.functions as fn
from pyspark import SparkContext
from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.types import StringType

from .arrows import apply
from .util import curried, read_varargs


def col(c: Union[str, Column]) -> Column:
    if isinstance(c, Column):
        return c
    # noinspection PyUnresolvedReferences
    return fn.col(c)  # pylint: disable=E1101


def lit(v) -> Column:
    # noinspection PyUnresolvedReferences
    return fn.lit(v)  # pylint: disable=E1101


def lit_or_col(c: Union[str, Column, Number]) -> Column:
    if isinstance(c, Number):
        return lit(c)
    return col(c)


# noinspection PyPep8Naming
def NOT(c: Column) -> Column:
    # noinspection PyUnresolvedReferences
    return ~c


def select(*cs: Union[Union[str, Column], Iterable[Union[str, Column]]]) -> Callable[[DataFrame], DataFrame]:
    cs = [col(c) for c in read_varargs(cs, (str, Column))]
    return lambda df: df.select(*cs)


def group_agg(*dimensions: Union[Union[str, Column], Iterable[Union[str, Column]]], **metrics: Column) -> \
        Callable[[DataFrame], DataFrame]:
    return lambda df: df \
        .groupBy(*(col(c) for c in read_varargs(dimensions, (str, Column)))) \
        .agg(*(v.alias(k) for k, v in metrics.items()))


@curried
def with_col(name: str, expr: Column, df: DataFrame) -> DataFrame:
    return df.withColumn(name, expr)


def with_cols(**kwargs: Column) -> Callable[[DataFrame], DataFrame]:
    return lambda df: df.select(*(c for c in df.columns if c not in kwargs), *(v.alias(k) for k, v in kwargs.items()))


def drop_col(*names: str) -> Callable[[DataFrame], DataFrame]:
    names = read_varargs(names, str)
    return lambda df: df.drop(*names)


def rename_col(old_name: str, new_name: str) -> Callable[[DataFrame], DataFrame]:
    return lambda df: df.withColumnRenamed(old_name, new_name)


@curried
def union2(df1: DataFrame, df2: DataFrame) -> DataFrame:
    df1_columns: List[str] = df1.columns
    df2_columns: List[str] = df2.columns
    df1_selection: List[Column] = list()
    df2_selection: List[Column] = list()
    for c in df1_columns:
        df1_selection.append(col(c))
        if c in df2_columns:
            df2_selection.append(col(c).alias(c))
        else:
            df2_selection.append(lit(None).alias(c))
    for c in df2_columns:
        if c not in df1_columns:
            df1_selection.append(lit(None).alias(c))
            df2_selection.append(col(c).alias(c))
    return df1.select(*df1_selection).union(df2.select(*df2_selection))


def union(*dfs: Union[DataFrame, Iterable[DataFrame]]) -> Optional[DataFrame]:
    """
    Unions all the given dataframes. Like unionByName as it does not care about column order.
    If any dataframe lacks any column that some other dataframe has it is filled with NULL
    values for that dataframe. This way you can union even dataframes which have different
    number of columns.
    """

    all_dfs: List[DataFrame] = read_varargs(dfs, DataFrame)
    if not all_dfs:
        return None
    return reduce(union2, all_dfs)


@curried
def split(predicate: Column, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Creates two DataFrames from one DataFrame: One where every row fulfills the given predicate,
    one which contains all the remaining rows.
    """

    c = "_condition"
    df = df.withColumn(c, fn.coalesce(predicate, lit(False))).cache()
    return df.filter(col(c)).drop(c), df.filter(NOT(col(c))).drop(c)


# noinspection PyUnresolvedReferences
def count(c: Union[None, str, Column] = None) -> Column:
    if c is None:
        return fn.count(lit(1))  # pylint: disable=E1101
    return fn.count(col(c))  # pylint: disable=E1101


def spark_session(df: DataFrame) -> SparkSession:
    return df.sql_ctx.sparkSession


def spark_context(df: DataFrame) -> SparkContext:
    # noinspection PyProtectedMember
    return df.sql_ctx._sc


def num_executors(ss: SparkSession) -> int:
    """
    Discovers the number of executors. If there are no executors it is assumed that we're
    running a local spark master which may utilize all local cores.
    """
    # noinspection PyProtectedMember
    sc = ss._jsc.sc()
    num = len([executor for executor in sc.statusTracker().getExecutorInfos()]) - 1  # driver is also returned
    if num == 0:
        num = os.cpu_count()
    return num


def repartition(num_partition: int, *cols: Union[str, Column]) -> Callable[[DataFrame], DataFrame]:
    return lambda df: df.repartition(num_partition, *cols)


def count_partitions(df: DataFrame):
    return apply(df, group_agg(fn.spark_partition_id(), count=count()))


def collect(df: DataFrame):
    return df.collect()


def estimate_size(df: DataFrame) -> Tuple[DataFrame, int]:
    """
    https://stackoverflow.com/questions/49492463/compute-size-of-spark-dataframe-sizeestimator-gives-unexpected-results
    """
    cf = df.cache()
    cf.foreach(lambda x: x)
    # noinspection PyProtectedMember
    catalyst_plan = df._jdf.queryExecution().logical()
    # noinspection PyProtectedMember
    s = spark_session(cf)._jsparkSession.sessionState().executePlan(catalyst_plan).optimizedPlan().stats().sizeInBytes()
    return cf, s


# noinspection PyTypeChecker
@typing.overload
def print_df(df: None = None, n=20, truncate=True, vertical=False) -> Callable[[DataFrame], DataFrame]:
    pass


def print_df(df: DataFrame = None, n=20, truncate=True, vertical=False) -> DataFrame:
    if df is None:
        # noinspection PyTypeChecker
        return lambda f: print_df(f, n=n, truncate=truncate, vertical=vertical)
    df.show(n=n, truncate=truncate, vertical=vertical)
    return df


def print_schema(df: DataFrame) -> DataFrame:
    df.printSchema()
    return df


# noinspection PyPep8Naming
def AND(*args: Union[Column, Iterable[Column]]) -> Column:
    columns: List[Column] = read_varargs(args, Column)
    if not columns:
        return lit(False)
    return reduce(Column.__and__, columns)


# noinspection PyPep8Naming
def OR(*args: Union[Iterable[Column], Column]) -> Column:
    columns: List[Column] = list()
    for arg in args:
        if isinstance(arg, Column):
            columns.append(arg)
        else:
            columns.extend(arg)
    if not columns:
        return lit(True)
    return reduce(Column.__or__, columns)


@curried
def eq(c1: Union[str, Column, Number, None], c2: Union[str, Column, Number, None]) -> Column:
    if c1 is None and c2 is None:
        return lit(None)
    if c1 is None:
        return col(c2).isNull()
    if c2 is None:
        return col(c1).isNull()
    # noinspection PyTypeChecker
    return lit_or_col(c1) == lit_or_col(c2)


@curried
def neq(c1: Union[str, Column, Number, None], c2: Union[str, Column, Number, None]) -> Column:
    if c1 is None and c2 is None:
        return lit(None)
    if c1 is None:
        return col(c2).isNotNull()
    if c2 is None:
        return col(c1).isNotNull()
    # noinspection PyTypeChecker
    return lit_or_col(c1) != lit_or_col(c2)


@curried
def lt(c1: Union[str, Column, Number], c2: Union[str, Column, Number]) -> Column:
    # noinspection PyTypeChecker
    return lit_or_col(c1) < lit_or_col(c2)


@curried
def gt(c1: Union[str, Column], c2: Union[str, Column]) -> Column:
    # noinspection PyTypeChecker
    return lit_or_col(c1) > lit_or_col(c2)


@curried
def lte(c1: Union[str, Column], c2: Union[str, Column]) -> Column:
    # noinspection PyTypeChecker
    return lit_or_col(c1) <= lit_or_col(c2)


@curried
def gte(c1: Union[str, Column], c2: Union[str, Column]) -> Column:
    # noinspection PyTypeChecker
    return lit_or_col(c1) >= lit_or_col(c2)


def is_null(c: Union[str, Column]) -> Column:
    return col(c).isNull()


def is_nonzero(c: Union[str, Column]) -> Column:
    return col(c).isNotNull() & (col(c) != lit(0))


def is_empty(c: Union[str, Column]) -> Column:
    return col(c).isNull() | (col(c).cast(StringType()) == lit(''))


def is_nonempty(c: Union[str, Column]) -> Column:
    return NOT(c)


def is_zero(c: Union[str, Column]) -> Column:
    c = col(c)
    return c.isNull() | (c == 0)
