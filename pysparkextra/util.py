from functools import partial, wraps


def curry(f, argcount):
    return wraps(f)(lambda *args: f(*args) if len(args) >= argcount else curry(partial(f, *args), argcount - len(args)))


def curried(f):
    return curry(f, f.__code__.co_argcount)
