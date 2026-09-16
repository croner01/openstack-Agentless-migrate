"""环境变量读取：非法值必须回退默认值，不能让后台线程/导入期直接崩。"""
import unittest

from env_utils import env_float, env_int, env_str


class EnvUtilsTest(unittest.TestCase):
    def test_str_strips_and_falls_back(self):
        self.assertEqual(env_str("X_MISSING", "fallback", environ={}), "fallback")
        self.assertEqual(env_str("X", environ={"X": "  v  "}), "v")

    def test_int_falls_back_on_garbage(self):
        self.assertEqual(env_int("X", 20, environ={}), 20)
        self.assertEqual(env_int("X", 20, environ={"X": "40"}), 40)
        self.assertEqual(env_int("X", 20, environ={"X": "20m"}), 20)
        self.assertEqual(env_int("X", 20, environ={"X": "  "}), 20)

    def test_int_applies_minimum(self):
        self.assertEqual(env_int("X", 5, minimum=1, environ={"X": "0"}), 1)
        self.assertEqual(env_int("X", 5, minimum=1, environ={"X": "-3"}), 1)

    def test_float_falls_back_on_garbage(self):
        self.assertEqual(env_float("X", 1.5, environ={}), 1.5)
        self.assertEqual(env_float("X", 1.5, environ={"X": "2.25"}), 2.25)
        self.assertEqual(env_float("X", 1.5, environ={"X": "abc"}), 1.5)


if __name__ == "__main__":
    unittest.main()
