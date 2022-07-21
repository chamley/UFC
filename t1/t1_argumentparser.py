import argparse
import boto3
import csv

from numpy import require


def my_argument_parser():
    parser = argparse.ArgumentParser(
        description="Perform a transformation step from stage layer 1 to stage layer 2"
    )
    arg_group = parser.add_mutually_exclusive_group(required=True)
    arg_group.add_argument(
        "-dates",
        nargs=2,
        help="date range to cover. Takes 2 arguments (start and end) formatted like so: YYYY-MM-DD",
    )
    arg_group.add_argument("-b", help="backfill all missing dates found in s3")
    arg_group.add_argument(
        "-csv",
        help="provide an s3 url to the location of a csv with fight prefixes to apply t1 to like so: s3://bucket/full-file-name",
    )
    arg_group.add_argument(
        "-dev",
        help="activates dev flag you can put around the program to debug",
        action="store_true",
    )
    return parser
