from functools import partial, wraps


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
