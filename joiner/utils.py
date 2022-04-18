import _csv
import csv
import os.path


def load_headers(filename):
    """

    :param filename: csv file name
    :return: (csv headers, csv dialect)
    """
    with open(filename, 'r') as csv_file:
        assert os.path.isfile(filename), f"no file found: {filename}"
        dialect = csv.Sniffer().sniff(csv_file.read(1024))
        csv_file.seek(0)
        reader = csv.reader(csv_file, dialect)
        headers = next(reader)
        assert len(headers) > 0, f"no columns found in file {filename}"
        return headers, dialect


def get_join_header_id(headers, column_name):
    """

    :param headers: list of headers
    :param column_name: column name
    :return: index of column in given headers
    """
    join_header_id = -1
    for i, header in enumerate(headers):
        if header == column_name:
            join_header_id = i
            break
    assert join_header_id > -1, f"no desired column among headers found"
    return join_header_id
