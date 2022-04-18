import csv
import hashlib
import os
import time

from joiner.utils import get_join_header_id, load_headers

path = './tmp/'
tmp_file_name = 'tmp'


class HashDivider:
    """
            Class used to partition file into smaller ones. csv rows are written to temporary files based on their certain header value.
            Header values are hashed with md5 and based on it written to temporary files. Datasets gets smaller and no possible relation will be lost.


            constructor params description:

            - base_file_name: name of the base file
            - joined_file_name: name of the joined file

            """
    def __init__(self, base_file_name, joined_file_name):
        self._base_file_name = base_file_name
        self._joined_file_name = joined_file_name
        self._folder_name = str(hashlib.md5(str(time.time_ns()).encode('utf8')).hexdigest())
        self._file_handles = []
        self._files = []
        self._initialized = False

    def initialize(self, column_name, size=10):
        """

        if param size=1 then no partition is being made and when we call open() normal file names are returned

        :param column_name: column by which we partition files
        :param size: desired number of files
        """
        self._initialized = True
        assert size > 0, f"bucket must be of positive size"
        if size == 1:
            self._files = [(self._base_file_name, self._joined_file_name)]
            return
        if len(self._file_handles) > 0:
            self.close()
        self._file_handles = []

        base_headers, base_dialect = load_headers(self._base_file_name)
        joined_headers, joined_dialect = load_headers(self._joined_file_name)

        file_names = self.__generate_file_names(size)
        self._files, self._file_handles = self.__create_files(file_names, base_headers, joined_headers, base_dialect)
        self.__fill_files(column_name, len(self._files), base_dialect, joined_dialect)

        for file_handle in self._file_handles:
            for handle in file_handle:
                handle.seek(0)


    def close(self):
        """
            closes file handles and clears temporary dirs
        """
        for file_handle in self._file_handles:
            for handle in file_handle:
                handle.close()
                os.remove(handle.name)
        for i in range(len(self._file_handles)):
            os.rmdir(f"{path}{self._folder_name}/tmp{i}")
        if os.path.exists(f"{path}{self._folder_name}/"):
            os.rmdir(f"{path}{self._folder_name}/")

    def __fill_files(self, column_name, files_count, base_dialect, joined_dialect):
        """

        :param column_name: column by which we join files
        :param files_count: number of available temporary files
        :param base_dialect: base file csv dialect
        :param joined_dialect: joined files csv dialect
        """
        with open(self._base_file_name, "r") as base_csv_file:
            reader = csv.reader(base_csv_file, base_dialect)
            headers = next(reader)
            join_header_id = get_join_header_id(headers, column_name)
            for row in reader:
                index = int(hashlib.md5(row[join_header_id].encode('utf8')).hexdigest(), 16) % files_count
                writer = csv.writer(self._file_handles[index][0], lineterminator='\n', dialect=base_dialect, escapechar='\\')
                writer.writerow(row)

        with open(self._joined_file_name, "r") as joined_csv_file:
            reader = csv.reader(joined_csv_file, joined_dialect)
            headers = next(reader)
            join_header_id = get_join_header_id(headers, column_name)
            for row in reader:
                index = int(hashlib.md5(row[join_header_id].encode('utf8')).hexdigest(), 16) % files_count
                writer = csv.writer(self._file_handles[index][1], lineterminator='\n', dialect=base_dialect, escapechar='\\')
                writer.writerow(row)

    def __create_files(self, file_names, base_headers, joined_headers, base_dialect):
        """
        creates files and initializes them with headers

        :param file_names: file names
        :param base_headers: base file csv headers
        :param joined_headers: joined file csv headers
        :param base_dialect: base file csv dialect
        :return: list of file name tuples (base file, corresponding joined file) and  list of file handles
        """
        files = []
        file_handles = []
        for base_file, joined_file in file_names:
            os.makedirs(os.path.dirname(base_file), exist_ok=True)
            os.makedirs(os.path.dirname(joined_file), exist_ok=True)

            base_file_handle = open(base_file, 'w+')
            writer = csv.writer(base_file_handle, lineterminator='\n', dialect=base_dialect)
            writer.writerow(base_headers)

            joined_file_handle = open(joined_file, 'w+')
            writer = csv.writer(joined_file_handle, lineterminator='\n', dialect=base_dialect)
            writer.writerow(joined_headers)

            files.append((base_file, joined_file))
            file_handles.append((base_file_handle, joined_file_handle))
        return files, file_handles

    def __generate_file_names(self, size):
        """

        :param size: number of files
        :return: zipped lists of temporary base and join files
        """
        base_files = []
        joined_files = []


        for i in range(size):
            base_files.append(f"{path}{self._folder_name}/{tmp_file_name}{i}/base")
            joined_files.append(f"{path}{self._folder_name}/{tmp_file_name}{i}/joined")

        return zip(base_files, joined_files)

    def open(self):
        """

        :return: list of files created by the divider if initialized
        """
        if not self._initialized:
            print("divider not initialized")
            return
        return self._files
