import csv

from joiner.utils import get_join_header_id, load_headers


class CsvIndexer:
    """
            Partitions a file into smaller batches, creating maps: column -> list of csv rows

            constructor params description:

            - file_name: file to partition
            - column_name: column to partition by

            other internal params:

            - _divider: is used to create a list of pairs of csv files (base_file, joined_file) in a way that doesn't lose any possible relation
            - _files: holds

            """
    def __init__(self, file_name, column_name):
        self.file_name = file_name
        self.column_name = column_name
        self.headers, self.dialect = load_headers(self.file_name)

    def batch_generator(self, batch_size=100):
        """
        returns a generator of batches on a given file

        :param batch_size: size of batch
        """
        assert batch_size > 0, f"batch_size must be greater than 0"
        read_index = 0
        batch = dict()
        join_header_id = get_join_header_id(self.headers, self.column_name)

        with open(self.file_name, 'r') as csv_file:
            reader = csv.reader(csv_file, self.dialect)
            next(reader)
            for row in reader:
                read_index += 1
                join_header_value = row[join_header_id]
                if join_header_value in batch:
                    batch[join_header_value].append(row)
                else:
                    batch[join_header_value] = [row]

                if read_index >= batch_size:
                    read_index = 0
                    yield batch
                    batch.clear()
        yield batch

