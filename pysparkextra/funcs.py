from functools import reduce
from numbers import Number
from typing import Union, List, Iterable, Optional, Callable, Tuple

import pyspark.sql.functions as fn
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType

from pysparkextra.util import curried


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


def select(*cs: Column) -> Callable[[DataFrame], DataFrame]:
    return lambda df: df.select(*cs)


@curried
def with_col(name: str, expr: Column, df: DataFrame) -> DataFrame:
    return df.withColumn(name, expr)


def with_cols(**kwargs: Column) -> Callable[[DataFrame], DataFrame]:
    return lambda df: df.select(*(c for c in df.columns if c not in kwargs), *(v.alias(k) for k, v in kwargs.items()))


def drop_col(*names: str) -> Callable[[DataFrame], DataFrame]:
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


def union(*dfs: Union[Iterable[DataFrame], DataFrame]) -> Optional[DataFrame]:
    """
    Unions all the given dataframes. Like unionByName as it does not care about column order.
    If any dataframe lacks any column that some other dataframe has it is filled with NULL
    values for that dataframe. This way you can union even dataframes which have different
    number of columns.
    """

    all_dfs: List[DataFrame] = list()
    for df in dfs:
        if isinstance(df, DataFrame):
            all_dfs.append(df)
        else:
            all_dfs.extend(df)
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


# noinspection PyPep8Naming
def AND(*args: Union[Iterable[Column], Column]) -> Column:
    columns: List[Column] = list()
    for arg in args:
        if isinstance(arg, Column):
            columns.append(arg)
        else:
            columns.extend(arg)
    if not columns:
        return lit(False)
    return reduce(lambda c1, c2: c1 & c2, columns)


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
    return reduce(lambda c1, c2: c1 | c2, columns)


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
