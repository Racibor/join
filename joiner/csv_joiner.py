import csv
import sys

from joiner.csv_file_divider import HashDivider
from joiner.csv_indexer import CsvIndexer
from joiner.utils import load_headers, get_join_header_id

supported_joins = ['inner', 'left', 'right']


def create_joiner(join_type, base_file_name, joined_file_name, column_name, output=sys.stdout, hash_size=10, batch_size=100):
    """
    returns desired joiner

    :param join_type: join type to be performed
    :param base_file_name: name of the base file
    :param joined_file_name: name of the file that will be joined
    :param column_name: name of the joined column (must appear in both files)
    :param output: all rows will be saved to specified output
    :param hash_size: number of external (files) temporary files to create
    :param batch_size: size of internal rows table
    :return: joiner of desired type
    """
    if join_type == 'inner':
        return CsvInnerJoiner(base_file_name, joined_file_name, column_name, output, hash_size=hash_size, batch_size=batch_size)
    elif join_type == 'left':
        return CsvLeftJoiner(base_file_name, joined_file_name, column_name, output, hash_size=hash_size, batch_size=batch_size)
    elif join_type == 'right':
        return CsvRightJoiner(base_file_name, joined_file_name, column_name, output, hash_size=hash_size, batch_size=batch_size)


class CsvJoiner:
    """
        base class of the joiner.

        base constructor collects and registers data for a subclass implementation to perform on

        constructor params description:

        - join_type: join type to be performed \n
        - base_file_name: name of the base file \n
        - joined_file_name: name of the file that will be joined \n
        - column_name: name of the joined column (must appear in both files) \n
        - output: all rows will be saved to specified output \n
        - hash_size: number of external (files) temporary files to create \n
        - batch_size: size of internal rows table \n

        """
    def __init__(self, base_file_name, joined_file_name, column_name, output=sys.stdout, hash_size=1, batch_size=100):
        self._column_name = column_name
        self._batch_size = batch_size
        self._hash_size = hash_size

        self._divider = HashDivider(base_file_name, joined_file_name)
        self._output = output

        self._joined_headers, self._joined_dialect = load_headers(joined_file_name)
        base_headers, base_dialect = load_headers(base_file_name)

        self._joined_join_header_id = get_join_header_id(self._joined_headers, self._column_name)
        assert self._joined_join_header_id > -1, f"column not found in the joined file"

        self._base_join_header_id = get_join_header_id(base_headers, self._column_name)
        assert self._base_join_header_id > -1, f"column not found in the base file"

        self._writer = csv.writer(output, lineterminator='\n', dialect=base_dialect, escapechar='\\')

        self._joined_headers.pop(self._joined_join_header_id)
        self.merged_headers = base_headers + self._joined_headers

    def start(self):
        """
            starts the process of joining.
        """
        try:
            self._divider.initialize(self._column_name, self._hash_size)
            files = self._divider.open()

            self._writer.writerow(self.merged_headers)
            for tmp_base_file, tmp_joined_file in files:
                self._join(tmp_base_file, tmp_joined_file)
        finally:
            self.close()

    def close(self):
        """
            removes current temporary files
        """
        self._divider.close()

    def _join(self, base_file, joined_file):
        """
        method to override. Subclasses should implement their join algorithm here

        :param base_file: base file name
        :param joined_file: joined file name
        """
        pass


class CsvInnerJoiner(CsvJoiner):
    """
            sub class of CsvJoiner

            It's _join method is overridden to extract inner join type relations

            CsvIndexer is used to create maps (in code called batch): column -> list of csv_rows

            Then for each batch a joined file is being traversed.
            If a header is spotted in a batch then a relation is formed and written to output

            """
    def __init__(self, base_file_name, joined_file_name, column_name, output=sys.stdout, hash_size=1, batch_size=10):
        CsvJoiner.__init__(self, base_file_name, joined_file_name, column_name, output, hash_size, batch_size)

    def _join(self, base_file, joined_file):
        indexer = CsvIndexer(base_file, self._column_name)
        with open(joined_file, 'r') as csv_file:
            for batch in indexer.batch_generator(self._batch_size):
                csv_file.seek(0)
                reader = csv.reader(csv_file, self._joined_dialect)
                next(reader)
                for row in reader:
                    self.__perform_join(batch, row)

    def __perform_join(self, batch, row):
        column_val = row[self._joined_join_header_id]
        if column_val in batch:
            batched_rows = batch[column_val]
            for batched_row in batched_rows:
                row = [item for index, item in enumerate(row) if index != self._joined_join_header_id]
                new_row = batched_row + row
                self._writer.writerow(new_row)


class CsvLeftJoiner(CsvJoiner):
    """
                sub class of CsvJoiner

                It's _join method is overridden to extract left join type relations

                CsvIndexer is used to create maps (in code called batch): column -> list of csv_rows

                Then for each batch a joined file is being traversed.
                If a header is spotted in a batch then a relation is formed and written to output.
                Additionally, a header in a batch is marked by appending a list with 0.
                All headers not marked with 0 are outputted with empty prerendered row


                """
    def __init__(self, base_file_name, joined_file_name, column_name, output=sys.stdout, hash_size=1, batch_size=10):
        CsvJoiner.__init__(self, base_file_name, joined_file_name, column_name, output, hash_size, batch_size)

    def _join(self, base_file, joined_file):
        prerendered_row = ["" for x in range(len(self._joined_headers))]
        indexer = CsvIndexer(base_file, self._column_name)
        with open(joined_file, 'r') as csv_file:
            for batch in indexer.batch_generator(self._batch_size):
                csv_file.seek(0)
                reader = csv.reader(csv_file, self._joined_dialect)
                next(reader)
                for row in reader:
                    self.__perform_join(batch, row)
                for key in batch:
                    if 0 not in batch[key]:
                        for batched_row in batch[key]:
                            new_row = batched_row + prerendered_row
                            self._writer.writerow(new_row)

    def __perform_join(self, batch, row):
        column_val = row[self._joined_join_header_id]
        if column_val in batch:
            batched_rows = batch[column_val]
            for batched_row in batched_rows:
                if batched_row != 0:
                    row = [item for index, item in enumerate(row) if index != self._joined_join_header_id]
                    new_row = batched_row + row
                    self._writer.writerow(new_row)
            batched_rows.append(0)


class CsvRightJoiner(CsvJoiner):
    """
                    sub class of CsvJoiner

                    It's _join method is overridden to extract right join type relations

                    CsvIndexer is used to create maps (in code called batch): column -> list of csv_rows

                    Then for each batch a joined file is being traversed.
                    If a header is spotted in a batch then a relation is formed and written to output.
                    Additionally, a header in a batch is marked by appending a list with 0.
                    All headers not marked with 0 are outputted with empty prerendered row


                    """
    def __init__(self, base_file_name, joined_file_name, column_name, output=sys.stdout, hash_size=1, batch_size=10):
        CsvJoiner.__init__(self, joined_file_name, base_file_name, column_name, output, hash_size, batch_size)

    def _join(self, base_file, joined_file):
        prerendered_row = ["" for x in range(len(self._joined_headers))]
        indexer = CsvIndexer(base_file, self._column_name)
        with open(joined_file, 'r') as csv_file:
            for batch in indexer.batch_generator(self._batch_size):
                csv_file.seek(0)
                reader = csv.reader(csv_file, self._joined_dialect)
                next(reader)
                for row in reader:
                    self.__perform_join(batch, row)
                for key in batch:
                    if 0 not in batch[key]:
                        for batched_row in batch[key]:
                            new_row = prerendered_row + batched_row
                            self._writer.writerow(new_row)

    def __perform_join(self, batch, row):
        column_val = row[self._joined_join_header_id]
        if column_val in batch:
            batched_rows = batch[column_val]
            for batched_row in batched_rows:
                if batched_row != 0:
                    row = [item for index, item in enumerate(row) if index != self._joined_join_header_id]
                    new_row = batched_row + row
                    self._writer.writerow(new_row)
            batched_rows.append(0)