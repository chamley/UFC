import argparse


def my_argument_parser():
    parser = argparse.ArgumentParser(description="Extract Data from ufcstats.com")
    arg_group = parser.add_mutually_exclusive_group(required=True)
    arg_group.add_argument(
        "-dates",
        nargs=2,
        help="This is the main way of using this program. Lets you specify a date range to cover. Takes 2 arguments (start and end) formatted like so: YYYY-MM-DD. ",
    )

    arg_group.add_argument(
        "-csv",
        help="provide the name of the csv file with fight prefixes to apply t1. csv should be located in folder: s3://ufc-meta/e1-dates",
    )
    arg_group.add_argument(
        "-dev",
        help="activates DEV_MODE flag you can put around the program to debug",
        action="store_true",
    )
    return parser
