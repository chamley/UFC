import argparse

x = {"dates": {"start": "2020-01-01", "end": "2021-01-01"}}


def my_argument_parser():
    parser = argparse.ArgumentParser(
        description="Perform a transformation step from stage layer 1 to stage layer 2"
    )
    arg_group = parser.add_mutually_exclusive_group()
    arg_group.add_argument(
        "-dates",
        nargs=2,
        help="This is the main way of using this program. Lets you specify a date range to cover. Takes 2 arguments (start and end) formatted like so: YYYY-MM-DD. ",
    )

    arg_group.add_argument(
        "-csv",
        help="provide the name of the csv file with fight dates to apply t1. Folder is s3://ufc-meta/t1-dates",
    )
    arg_group.add_argument(
        "-dev",
        help="activates DEV_MODE flag you can put around the program to debug",
        action="store_true",
    )
    return parser
