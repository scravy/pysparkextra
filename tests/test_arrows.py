import unittest

from pysparkextra.arrows import *


class ArrowsTestCase(unittest.TestCase):

    def test_pipe(self):
        f = pipe(lambda v: v + 3, lambda v: v * 4, lambda v: v - 1)

        self.assertEqual((((5 + 3) * 4) - 1), f(5))


if __name__ == '__main__':
    unittest.main()
