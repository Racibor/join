import os
import unittest

from joiner.csv_file_divider import HashDivider
from test.test_constants import test_data_path
from joiner.utils import load_headers


class CsvHashDividerTest(unittest.TestCase):
    def setUp(self):
        self.base_file_name = test_data_path + 'test0.csv'
        self.joined_file_name = test_data_path + 'test1.csv'

        headers, dialect = load_headers(self.base_file_name)
        self.column_name = headers[0]

    def test_files_exist(self):
        divider = HashDivider(self.base_file_name, self.joined_file_name)
        files_count = 5
        divider.initialize(self.column_name, files_count)

        for base_file, joined_file in divider.open():
            self.assertTrue(os.path.isfile(base_file))
            self.assertTrue(os.path.isfile(joined_file))

        divider.initialize(self.column_name, files_count*2)
        for base_file, joined_file in divider.open():
            self.assertTrue(os.path.isfile(base_file))
            self.assertTrue(os.path.isfile(joined_file))

        divider.close()

    def test_batch_size_1(self):
        divider = HashDivider(self.base_file_name, self.joined_file_name)
        divider.initialize(self.column_name, 1)
        files = divider.open()

        self.assertTrue(len(files) == 1)

        file = files[0]

        self.assertTrue(file[0] == self.base_file_name)
        self.assertTrue(file[1] == self.joined_file_name)

        divider.close()

    def test_batch_size_less_than_0(self):
        divider = HashDivider(self.base_file_name, self.joined_file_name)
        with self.assertRaises(AssertionError) as context:
            divider.initialize(self.column_name, -1)
        divider.close()

    def test_files_count(self):
        divider = HashDivider(self.base_file_name, self.joined_file_name)
        files_count = 5
        divider.initialize(self.column_name, files_count)

        files = divider.open()
        self.assertEqual(files_count, len(files))

        divider.close()

    def test_close(self):
        divider = HashDivider(self.base_file_name, self.joined_file_name)
        files_count = 5
        divider.initialize(self.column_name, files_count)

        files = divider.open()

        divider.close()

        for base_file, joined_file in files:
            self.assertFalse(os.path.exists(base_file))
            self.assertFalse(os.path.exists(joined_file))
