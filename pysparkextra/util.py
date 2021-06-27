import typing
from functools import partial, wraps
from typing import TypeVar, Union, List, Iterable, Type


def curry(f, argcount):
    return wraps(f)(lambda *args: f(*args) if len(args) >= argcount else curry(partial(f, *args), argcount - len(args)))


def curried(f):
    return curry(f, f.__code__.co_argcount)


@curried
def add(left, right):
    return left + right


plus = add


@curried
def multiply(left, right):
    return left * right


times = multiply


def subtract(amount):
    return lambda v: v - amount


minus = subtract


def divide_by(amount):
    return lambda v: v / amount


T = TypeVar('T')
U = TypeVar('U')


@typing.overload
def read_varargs(args: Iterable[Union[Union[T, U], Iterable[Union[T, U]]]], type: typing.Tuple[Type[T], Type[U]]) -> \
        List[Union[T, U]]:
    pass


# noinspection PyShadowingBuiltins
def read_varargs(args: Iterable[Union[T, Iterable[T]]], type: Type[T]) -> List[T]:
    res: List[T] = []
    for arg in args:
        if isinstance(arg, type):
            res.append(arg)
        else:
            res.extend(arg)
    return res
