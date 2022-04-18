import csv
import unittest
from io import StringIO

from joiner.csv_joiner import create_joiner, CsvInnerJoiner, CsvLeftJoiner, CsvRightJoiner, CsvJoiner
from test.test_constants import test_data_path
from joiner.utils import load_headers, get_join_header_id


class CsvJoinerTest(unittest.TestCase):
    def test_create_joiner(self):
        headers, dialect = load_headers(test_data_path + 'test0.csv')
        dummy_args = [test_data_path + 'test0.csv', test_data_path + 'test1.csv', headers[0]]

        joiner = create_joiner('inner', dummy_args[0], dummy_args[1], dummy_args[2])
        joiner.close()
        self.assertTrue(isinstance(joiner, CsvInnerJoiner))

        joiner = create_joiner('left', dummy_args[0], dummy_args[1], dummy_args[2])
        joiner.close()
        self.assertTrue(isinstance(joiner, CsvLeftJoiner))

        joiner = create_joiner('right', dummy_args[0], dummy_args[1], dummy_args[2])
        joiner.close()
        self.assertTrue(isinstance(joiner, CsvRightJoiner))

    def test_write_headers_on_start(self):
        base_headers, dialect = load_headers(test_data_path + 'test0.csv')
        joined_headers, dialect = load_headers(test_data_path + 'test1.csv')

        column_name = "firstname"
        joined_join_header_id = get_join_header_id(joined_headers, column_name)
        joined_headers.pop(joined_join_header_id)

        merged_headers = base_headers + joined_headers

        temp_file = StringIO()
        joiner = CsvJoiner(test_data_path + 'test0.csv', test_data_path + 'test1.csv', column_name,
                               output=temp_file)
        joiner.start()

        temp_file.seek(0)
        written_header = next(csv.reader(temp_file))

        self.assertEqual(written_header, merged_headers)


    def test_inner_joiner(self):
        temp_file = StringIO()
        joiner = create_joiner('inner', test_data_path + 'test0.csv', test_data_path + 'test1.csv', "firstname",
                               output=temp_file)
        joiner.start()

        temp_file.seek(0)
        result = list(csv.reader(temp_file))

        with open(test_data_path + 'test0_1_inner_join_result.csv') as desired_file:
            desired = list(csv.reader(desired_file))
            self.assertEqual(len(result), len(desired))
            for row in result:
                self.assertTrue(row in desired)
        temp_file.close()

    def test_left_joiner(self):
        temp_file = StringIO()
        joiner = create_joiner('left', test_data_path + 'test0.csv', test_data_path + 'test1.csv', "firstname",
                               output=temp_file)
        joiner.start()
        temp_file.seek(0)
        result = list(csv.reader(temp_file))

        with open(test_data_path + 'test0_1_left_join_result.csv') as desired_file:
            desired = list(csv.reader(desired_file))
            self.assertEqual(len(result), len(desired))
            for row in result:
                self.assertTrue(row in desired)
        temp_file.close()

    def test_right_joiner(self):
        temp_file = StringIO()
        joiner = create_joiner('right', test_data_path + 'test0.csv', test_data_path + 'test1.csv', "firstname",
                               output=temp_file)
        joiner.start()
        temp_file.seek(0)
        result = list(csv.reader(temp_file))

        with open(test_data_path + 'test0_1_right_join_result.csv') as desired_file:
            desired = list(csv.reader(desired_file))
            self.assertEqual(len(result), len(desired))
            for row in result:
                self.assertTrue(row in desired)
        temp_file.close()