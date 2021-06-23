import unittest

from pysparkextra.arrows import *
from pysparkextra.util import plus, minus


class ArrowsTestCase(unittest.TestCase):

    def test_pipe(self):
        f = pipe(lambda v: v + 3, lambda v: v * 4, lambda v: v - 1)

        self.assertEqual((((5 + 3) * 4) - 1), f(5))

    def test_first(self):
        f = first(plus(1))
        v = (3, 3)
        self.assertEqual((4, 3), f(v))

    def test_second(self):
        f = second(plus(1))
        v = (3, 3)
        self.assertEqual((3, 4), f(v))

    def test_both(self):
        f = both(plus(1))
        v = (3, 3)
        self.assertEqual((4, 4), f(v))

    def test_parallel(self):
        f = parallel(plus(1), minus(1))
        v = (3, 3)
        self.assertEqual((4, 2), f(v))

    def test_fanout(self):
        f = plus(1)
        g = minus(1)
        h = fanout(f, g)
        self.assertEqual((4, 2), h(3))

    def test_splat(self):
        def f(*k):
            return sum(k)

        g = splat(f)
        self.assertEqual(10, g([1, 2, 3, 4]))


if __name__ == '__main__':
    unittest.main()
