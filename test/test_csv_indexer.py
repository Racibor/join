import csv
import unittest

from joiner.csv_indexer import CsvIndexer
from test.test_constants import test_data_path

from joiner.utils import load_headers


class CsvIndexerTest(unittest.TestCase):
    def test_generator_batch_size(self):
        test_file_1 = 'test0.csv'
        headers, dialect = load_headers(test_data_path + test_file_1)
        indexer = CsvIndexer(test_data_path + test_file_1, headers[0])
        batch_size = 2
        for batch in indexer.batch_generator(batch_size):
            self.assertTrue(len(batch) <= batch_size)

        for batch in indexer.batch_generator(batch_size*2):
            self.assertTrue(len(batch) <= batch_size*2)

    def test_generator_negative_batch_size(self):
        test_file_1 = 'test0.csv'
        headers, dialect = load_headers(test_data_path + test_file_1)
        indexer = CsvIndexer(test_data_path + test_file_1, headers)
        with self.assertRaises(AssertionError) as context:
            for i in indexer.batch_generator(-1):
                tmp = i

    def test_empty_file(self):
        test_file_1 = 'test_empty.csv'
        with self.assertRaises(csv.Error) as context:
            CsvIndexer(test_data_path + test_file_1, 'id')
