from functools import partial


def curry(f, argcount):
    cf = lambda *args: f(*args) if len(args) >= argcount else curry(partial(f, *args), argcount - len(args))
    cf.__curried__ = argcount
    return cf


def curried(f):
    argcount = f.__code__.co_argcount
    return curry(f, argcount)
