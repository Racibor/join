import argparse
import os.path

from joiner.csv_joiner import *

supported_operations = ['join']


def parse_arguments():
    parser = argparse.ArgumentParser(description="program performs joins on csv files")
    parser.add_argument("operation")
    parser.add_argument("base_file_path")
    parser.add_argument("joined_file_path")
    parser.add_argument("column_name")
    parser.add_argument("join_type", nargs="?", default='inner')
    parser.add_argument("-hash", help="specify number of file partitions")
    parser.add_argument("-batch", help="specify internal indexing batch size")

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_arguments()

    hash_size = 1
    batch_size = 1000

    if args.hash is not None:
        assert args.hash.isdigit(), "value of -hash must be a positive integer"
        assert int(args.hash) > 0, "value of -hash must be a positive integer"
        hash_size = int(args.hash)
    if args.batch is not None:
        assert args.batch.isdigit(), "value of -batch must be a positive integer"
        assert int(args.batch) > 0, "value of -batch must be a positive integer"
        batch_size = int(args.batch)

    assert args.operation in supported_operations, f"unsupported operation - list of supported operations: {', '.join(supported_operations)}"

    assert args.join_type in supported_joins, f"unsupported join type - list of supported joins: {', '.join(supported_joins)}"

    assert os.path.isfile(args.base_file_path), f"base file not found {args.base_file_path}"
    assert os.path.isfile(args.joined_file_path), f"joined file not found {args.joined_file_path}"

    joiner = create_joiner(args.join_type, args.base_file_path, args.joined_file_path, args.column_name,hash_size=hash_size, batch_size=batch_size)
    joiner.start()



