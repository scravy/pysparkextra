from functools import reduce
from typing import TypeVar, Callable, Tuple

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')


def left(f: Callable[[A], B]) -> Callable[[Tuple[A, C]], Tuple[B, C]]:
    return lambda t: (f(t[0]), t[1])


def right(f: Callable[[A], B]) -> Callable[[Tuple[C, A]], Tuple[C, B]]:
    return lambda t: (t[0], f(t[1]))


def both(f: Callable[[A], B], g: Callable[[C], D]) -> Callable[[Tuple[A, C]], Tuple[B, D]]:
    return lambda t: (f(t[0]), g(t[1]))


def fanout(f: Callable[[A], B], g: Callable[[A], C]) -> Callable[[A], Tuple[B, C]]:
    return lambda v: (f(v), g(v))


def compose(f: Callable[[B], C], g: Callable[[A], B]) -> Callable[[A], C]:
    return lambda v: f(g(v))


def pipe(*fs):
    return reduce(compose, fs[::-1])


def apply(v: A, f: Callable[[A], B]) -> B:
    return f(v)


def splat(f):
    return lambda t: f(*t)
