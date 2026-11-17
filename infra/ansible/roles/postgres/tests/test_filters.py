"""Unit tests for the postgres role filter.

Run directly with python3; no Ansible or collection is needed:

    python3 roles/postgres/tests/test_filters.py
"""

from __future__ import annotations

import importlib.util
import pathlib
import unittest

MODULE_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "filter_plugins"
    / "postgres_filters.py"
)
SPEC = importlib.util.spec_from_file_location("postgres_filters", MODULE_PATH)
FILTERS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FILTERS)


class PositiveIntTest(unittest.TestCase):
    def test_accepts_ints_and_ascii_digit_strings(self):
        self.assertEqual(FILTERS.postgres_positive_int(5432), 5432)
        self.assertEqual(FILTERS.postgres_positive_int("55432"), 55432)

    def test_rejects_floats_and_non_decimal_strings(self):
        for value in [5432.5, "5432.5", "5432x", "1e3", " 5432", "0", 0, -1, True]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FILTERS.postgres_positive_int(value)

    def test_rejects_unicode_digits(self):
        with self.assertRaises(ValueError):
            FILTERS.postgres_positive_int("\u0665\u0664\u0663\u0662")


if __name__ == "__main__":
    unittest.main()
