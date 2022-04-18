import _csv
import unittest

from test.test_constants import test_data_path
from joiner.utils import load_headers


class UtilsTest(unittest.TestCase):
    def test_load_headers_empty_file(self):
        with self.assertRaises(_csv.Error) as context:
            headers, dialect = load_headers(test_data_path + 'test_empty.csv')

    def test_load_headers_dummy_file(self):
        headers, dialect = load_headers(test_data_path + 'test0.csv')
        dummy_headers = ['id', 'firstname', 'lastname']
        self.assertEqual(headers, dummy_headers)

    def test_load_headers_file_not_exits(self):
        with self.assertRaises(FileNotFoundError) as context:
            headers, dialect = load_headers(test_data_path + 'this_file_doesnt_exist.csv')
