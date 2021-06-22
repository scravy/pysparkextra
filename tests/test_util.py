import unittest

from pysparkextra.util import curried


class UtilTestCase(unittest.TestCase):
    def test_currying(self):
        const = curried(lambda a, b: a)

        three = const(3)

        self.assertEqual(3, three(4))
        self.assertEqual(3, three(5))

        addthree = curried(lambda a, b, c: a + b + c)

        self.assertEqual(6, addthree(1, 2, 3))
        self.assertEqual(6, addthree(1)(2, 3))
        self.assertEqual(6, addthree(1, 2)(3))
        self.assertEqual(6, addthree(1)(2)(3))

        self.assertEqual(6, addthree()(1)()()(2)(3))


if __name__ == '__main__':
    unittest.main()
